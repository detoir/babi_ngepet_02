"""
================================================================================
MODULE: ai_scanner.py
ROLE  : Discovery Engine (Pending Capping, Whale Fast Trades & Speed Watchlist)
================================================================================
"""

import os
import sys
import io
import json
import time
import sqlite3
import datetime
import html
import asyncio
import re
import aiohttp
import xml.etree.ElementTree as ET
from openai import OpenAI
from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from config_db import (
    BOT_NAME,
    SCAN_INTERVAL_SECONDS,
    MIN_RRR,
    CONFIDENCE_THRESHOLD,
    MAX_24H_LOSS_THRESHOLD,
    MAX_CONCURRENT_POSITIONS,
    MAX_DAILY_TRADES,
    MAX_CORRELATED_POSITIONS_PER_SIDE,
    MAX_CORRELATED_SCALPS_PER_SIDE,
    DEFAULT_LEVERAGE,
    SCALP_LEVERAGE,
    POSITION_SIZE_MULTIPLIER,
    SCALP_ENTRY_TYPE,
    TRADE_ENTRY_TYPE,
    TRADING_MODE,
    DEEPSEEK_API_KEY,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    ACCOUNT_BALANCE_SIMULATED,
    WHALE_MIN_PROBABILITY,
    SCALP_MIN_PROBABILITY,
    ENABLE_WHALE_SIGNALS,
    is_weekend_regime,
    generate_monthly_performance_report,
    export_monthly_report_csv,
    get_exchange_instance,
    get_public_exchange_instance,
    get_outbound_ip_sync,
    get_db_connection,
    log_event,
    is_scanning_enabled,
    check_rolling_24h_drawdown,
    count_active_and_daily_trades,
    count_active_positions_by_direction,
    fetch_current_account_capital,
    bridge_symbol_formats,
    get_contextual_trade_lessons,
    get_cooldown_symbols,
    upsert_watched_signal,
    remove_watched_signal_by_symbol,
    clear_all_watched_signals,
    get_active_watched_signals,
    get_active_strategy_rules,
    log_ai_interaction,
    get_recent_system_logs,
    prune_old_logs,
    place_binance_market_entry,
    place_binance_entry_limit,
    place_binance_stop_loss,
    place_binance_take_profit,
    place_binance_tp_sl_brackets,
    split_tp_quantities,
    fetch_live_binance_position,
    cancel_binance_order,
    emergency_binance_market_close,
    get_symbol_min_qty,
    format_order_quantity,
    parse_database_timestamp,
    utc_now,
)

exchange = get_exchange_instance()
public_exchange = get_public_exchange_instance()
ROUND_TRIP_FEE_PCT = 0.10
ai_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

def extract_clean_json(raw_text: str) -> dict:
    if not raw_text:
        return {}
    cleaned = re.sub(r"^```json\s*", "", raw_text.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"```$", "", cleaned.strip(), flags=re.MULTILINE)
    
    match = re.search(r"(\{.*\})", cleaned, re.DOTALL)
    if match:
        cleaned = match.group(1)
    
    try:
        return json.loads(cleaned)
    except Exception:
        sanitized = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", cleaned)
        return json.loads(sanitized)

def fetch_real_perp_market_price(symbol: str) -> float:
    try:
        bridge = bridge_symbol_formats(symbol)
        ticker = public_exchange.fetch_ticker(bridge["ccxt"])
        last_price = ticker.get("last") or ticker.get("close")
        if last_price:
            return float(last_price)
        mark_price = ticker.get("info", {}).get("markPrice")
        return float(mark_price) if mark_price else 0.0
    except Exception as e:
        log_event("WARN", "SCANNER_PRICE", f"Price ticker fetch error on {symbol}: {e}")
        return 0.0

def fetch_candles(symbol: str, timeframe="1h", limit=35):
    try:
        bridge = bridge_symbol_formats(symbol)
        ohlcv = public_exchange.fetch_ohlcv(bridge["ccxt"], timeframe=timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 20:
            return []
        return [{"open": c[1], "high": c[2], "low": c[3], "close": c[4], "vol": c[5]} for c in ohlcv[:-1]]
    except Exception:
        return []

def calculate_rsi(prices: list, period=14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 1)

def calculate_atr(candles: list, period=14) -> float:
    if len(candles) < period + 1:
        return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        cp = candles[i-1]["close"]
        tr_list.append(max(h - l, abs(h - cp), abs(l - cp)))
    return sum(tr_list[-period:]) / float(period)

def calculate_ema(prices: list, period: int) -> float:
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price * k) + (ema * (1 - k))
    return ema

def calculate_adx_regime(candles: list, period=14) -> dict:
    if len(candles) < period * 2:
        return {"regime": "RANGING", "adx": 18.0}
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(candles)):
        h_diff = candles[i]["high"] - candles[i-1]["high"]
        l_diff = candles[i-1]["low"] - candles[i]["low"]
        plus_dm.append(h_diff if (h_diff > l_diff and h_diff > 0) else 0.0)
        minus_dm.append(l_diff if (l_diff > h_diff and l_diff > 0) else 0.0)
        tr = max(candles[i]["high"] - candles[i]["low"], abs(candles[i]["high"] - candles[i-1]["close"]), abs(candles[i]["low"] - candles[i-1]["close"]))
        tr_list.append(tr)
    tr14 = sum(tr_list[:period])
    if tr14 == 0:
        return {"regime": "RANGING", "adx": 15.0}
    plus_di = (sum(plus_dm[:period]) / tr14) * 100.0
    minus_di = (sum(minus_dm[:period]) / tr14) * 100.0
    denom = plus_di + minus_di
    dx = (abs(plus_di - minus_di) / denom * 100.0) if denom > 0 else 0.0
    return {"regime": "TRENDING" if dx >= 25.0 else "RANGING", "adx": round(dx, 1)}

def calculate_deterministic_trade_levels(price: float, atr_1h: float, direction: str, ema21_1h: float, strat_mode: str = "TREND_MOMENTUM", change_24h: float = 0.0, is_scalp: bool = False):
    is_weekend = is_weekend_regime()
    is_sweep = (strat_mode == "SWEEP_REVERSION" or is_weekend)

    if is_scalp or strat_mode == "SCALP":
        # Professional Scalp Sniper:
        # Balanced ATR swing risk (0.85% min to avoid 5m spread/wick noise, 1.20% max to cap single loss)
        raw_risk = atr_1h * 0.75
        risk_dist = max(raw_risk, price * 0.0085)
        risk_dist = min(risk_dist, price * 0.0120)

        # High-Hit-Rate Scalp Targets: Bank 50% early at 1.0R so winning positions pay out before reversals!
        tp1_mult = 1.0
        tp2_mult = 1.8
        tp3_mult = 2.8

        # If MARKET mode is active (default), use mark price so entry + SL + TP execute immediately
        if SCALP_ENTRY_TYPE == "MARKET":
            entry = round(price, 6)
        elif direction == "LONG":
            pullback_target = max(price * 0.9970, min(price * 0.9985, ema21_1h)) if ema21_1h < price else (price * 0.9975)
            entry = round(pullback_target, 6)
        else:
            bounce_target = min(price * 1.0030, max(price * 1.0015, ema21_1h)) if ema21_1h > price else (price * 1.0025)
            entry = round(bounce_target, 6)

        if direction == "LONG":
            sl = round(entry - risk_dist, 6)
            t1 = round(entry + (tp1_mult * risk_dist), 6)
            t2 = round(entry + (tp2_mult * risk_dist), 6)
            t3 = round(entry + (tp3_mult * risk_dist), 6)
        else:
            sl = round(entry + risk_dist, 6)
            t1 = round(entry - (tp1_mult * risk_dist), 6)
            t2 = round(entry - (tp2_mult * risk_dist), 6)
            t3 = round(entry - (tp3_mult * risk_dist), 6)

        return {
            "entry": entry,
            "sl": sl,
            "t1": t1,
            "t2": t2,
            "t3": t3,
            "tp1_mult": tp1_mult,
            "tp2_mult": tp2_mult,
            "tp3_mult": tp3_mult,
            "risk_dist": risk_dist,
            "risk_pct": round((risk_dist / entry) * 100.0, 2) if entry > 0 else 1.0
        }

    # Dynamic risk distance for Fast/Swing trades: Cap strictly at 1.45%
    raw_risk = atr_1h * 1.10
    risk_dist = max(raw_risk, price * 0.010)
    risk_dist = min(risk_dist, price * 0.0145)

    # Adaptive reward multipliers: Closer TP1 (1.1R-1.2R) to bank 50% early on first impulse
    if is_sweep:
        tp1_mult = 1.1
        tp2_mult = 1.9
        tp3_mult = 2.8
    else:
        tp1_mult = 1.2
        tp2_mult = 2.0
        tp3_mult = 3.0

    if TRADE_ENTRY_TYPE == "MARKET":
        entry = round(price, 6)
    elif not is_sweep:
        if direction == "SHORT":
            if change_24h <= -4.0:
                bounce_target = min(price * 1.008, max(price * 1.004, ema21_1h)) if ema21_1h > price else (price * 1.005)
            else:
                bounce_target = min(price * 1.0035, max(price * 1.0012, ema21_1h)) if ema21_1h > price else (price * 1.0018)
            entry = round(bounce_target, 6)
        else:
            if change_24h >= 3.5:
                pullback_target = max(price * 0.992, min(price * 0.996, ema21_1h)) if ema21_1h < price else (price * 0.994)
            else:
                pullback_target = max(price * 0.9965, min(price * 0.9988, ema21_1h)) if ema21_1h < price else (price * 0.9982)
            entry = round(pullback_target, 6)
    else:  # SWEEP_REVERSION / WEEKEND
        if direction == "LONG":
            entry = round(price * 0.994, 6)
        else:
            entry = round(price * 1.006, 6)

    if direction == "LONG":
        sl = round(entry - risk_dist, 6)
        t1 = round(entry + (tp1_mult * risk_dist), 6)
        t2 = round(entry + (tp2_mult * risk_dist), 6)
        t3 = round(entry + (tp3_mult * risk_dist), 6)
    else:
        sl = round(entry + risk_dist, 6)
        t1 = round(entry - (tp1_mult * risk_dist), 6)
        t2 = round(entry - (tp2_mult * risk_dist), 6)
        t3 = round(entry - (tp3_mult * risk_dist), 6)
        
    return {
        "entry": entry,
        "sl": sl,
        "t1": t1,
        "t2": t2,
        "t3": t3,
        "tp1_mult": tp1_mult,
        "tp2_mult": tp2_mult,
        "tp3_mult": tp3_mult,
        "risk_dist": risk_dist,
        "risk_pct": round((risk_dist / entry) * 100.0, 2) if entry > 0 else 1.4
    }

def calculate_objective_quant_score(direction: str, rsi_1h: float, rsi_4h: float, btc_regime: str, vol_m: float, funding_rate: float, change_24h: float, btc_24h_pct: float, book_ratio: float = 1.0) -> float:
    # Hard Disqualification Gates: Never buy overbought tops / falling knives or short oversold capitulation bottoms
    if direction == "LONG":
        if btc_24h_pct < -1.5:
            return 0.10
        if rsi_1h > 66.0 or rsi_1h < 38.0:
            return 0.10
        if change_24h >= 9.0 or funding_rate >= 0.030 or book_ratio < 0.78:
            return 0.10
    else:  # SHORT
        if btc_24h_pct > 2.2:
            return 0.10
        if rsi_1h < 34.0 or rsi_1h > 60.0:
            return 0.10
        if change_24h <= -9.0 or funding_rate <= -0.035 or book_ratio > 1.28:
            return 0.10

    score = 0.62
    
    if direction == "LONG":
        if btc_24h_pct >= 0.2: score += 0.10
        if 0.2 <= (change_24h - btc_24h_pct) <= 6.0: score += 0.10
        if 44.0 <= rsi_1h <= 62.0: score += 0.10
        if 44.0 <= rsi_4h <= 64.0: score += 0.08
        if funding_rate <= 0.012: score += 0.06
        if book_ratio >= 1.10: score += 0.08
    else:  # SHORT
        if btc_24h_pct <= -0.3: score += 0.12
        if -6.5 <= (change_24h - btc_24h_pct) <= -0.3: score += 0.10
        if 36.0 <= rsi_1h <= 54.0: score += 0.10
        if 36.0 <= rsi_4h <= 56.0: score += 0.08
        if -0.025 <= funding_rate <= 0.015: score += 0.06
        if book_ratio <= 0.90: score += 0.08

    if vol_m >= 35.0: score += 0.06
    elif vol_m >= 10.0: score += 0.04

    # Clamp positive bonuses BEFORE applying any edge exhaustion penalties
    score = min(0.92, score)

    if direction == "LONG":
        if rsi_1h > 63.0: score -= 0.20
        if change_24h >= 6.5: score -= 0.20
    else:
        if rsi_1h < 36.5: score -= 0.20
        if change_24h <= -6.5: score -= 0.20

    return round(max(0.10, min(0.95, score)), 2)

def calculate_dynamic_position_size(quant_score: float, risk_pct: float, balance=None, is_scalp: bool = False) -> float:
    bal = float(balance) if balance is not None else float(ACCOUNT_BALANCE_SIMULATED)
    # Allocate ~40% of wallet balance as margin per slot (2x bigger margin per trade)
    lev = SCALP_LEVERAGE if is_scalp else DEFAULT_LEVERAGE
    slot_margin_fraction = 0.40  # Twice bigger margin per trade
    max_slot_notional = bal * slot_margin_fraction * lev
    risk_alloc = (0.0050 if quant_score < 0.78 else (0.0075 if quant_score < 0.85 else 0.0100)) * POSITION_SIZE_MULTIPLIER
    risk_dollars = bal * risk_alloc
    pos_size = (risk_dollars / (risk_pct / 100.0)) * 2.0 if risk_pct > 0 else max_slot_notional
    return round(min(pos_size, max_slot_notional), 0)

async def fetch_microstructure_data_async(session: aiohttp.ClientSession, symbol_clean: str) -> dict:
    oi_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol_clean}"
    depth_url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol_clean}&limit=20"
    ls_ratio_url = f"https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={symbol_clean}&period=5m&limit=1"
    
    oi_val = 0.0
    book_ratio = 1.0
    whale_ls_ratio = 1.0
    
    try:
        async with session.get(oi_url, timeout=aiohttp.ClientTimeout(total=3.0)) as resp:
            if resp.status == 200:
                data = await resp.json()
                oi_val = float(data.get("openInterest") or 0.0)
    except Exception:
        pass

    try:
        async with session.get(depth_url, timeout=aiohttp.ClientTimeout(total=3.0)) as resp:
            if resp.status == 200:
                data = await resp.json()
                bids = sum(float(b[1]) for b in data.get("bids", []))
                asks = sum(float(a[1]) for a in data.get("asks", []))
                if asks > 0:
                    book_ratio = round(bids / asks, 2)
    except Exception:
        pass

    try:
        async with session.get(ls_ratio_url, timeout=aiohttp.ClientTimeout(total=3.0)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and isinstance(data, list):
                    whale_ls_ratio = round(float(data[0].get("longShortRatio") or 1.0), 2)
    except Exception:
        pass

    return {"oi": oi_val, "book_ratio": book_ratio, "whale_ls_ratio": whale_ls_ratio}

async def fetch_live_world_and_crypto_news() -> str:
    headlines = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    high_impact_alerts = []
    
    async def fetch_url(session, url):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4.5)) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception:
            pass
        return None

    async with aiohttp.ClientSession() as session:
        t_yahoo, t_goog, t_ct, t_cd, t_fng = await asyncio.gather(
            fetch_url(session, "https://finance.yahoo.com/news/rssindex"),
            fetch_url(session, "https://news.google.com/rss/search?q=Federal+Reserve+OR+CPI+OR+Inflation+OR+FOMC+OR+Recession&hl=en-US&gl=US&ceid=US:en"),
            fetch_url(session, "https://cointelegraph.com/rss"),
            fetch_url(session, "https://www.coindesk.com/arc/outboundfeeds/rss/"),
            fetch_url(session, "https://api.alternative.me/fng/?limit=1")
        )

    # 1. Global Financial & Macro News (Yahoo Finance)
    if t_yahoo:
        try:
            root = ET.fromstring(t_yahoo)
            for item in root.findall("./channel/item")[:4]:
                t = item.find("title")
                if t is not None and t.text:
                    clean_txt = re.sub(r'<[^>]+>', '', t.text).strip()
                    headlines.append(f"[Global Markets/Finance] {clean_txt}")
                    if any(k in clean_txt.upper() for k in ["FOMC", "FED", "RATE HIKE", "RATE CUT", "POWELL", "CPI", "INFLATION", "PAYROLLS", "RECESSION", "TARIFF", "WAR"]):
                        high_impact_alerts.append(f"⚠️ {clean_txt}")
        except Exception:
            pass

    # 2. Central Bank & Macroeconomic Economic Indicators (Google News Macro)
    if t_goog:
        try:
            root = ET.fromstring(t_goog)
            for item in root.findall("./channel/item")[:3]:
                t = item.find("title")
                if t is not None and t.text:
                    clean_txt = re.sub(r'<[^>]+>', '', t.text).strip()
                    headlines.append(f"[Global Macro Policy] {clean_txt}")
                    if any(k in clean_txt.upper() for k in ["FOMC", "FED", "RATE", "POWELL", "CPI", "INFLATION", "NFP"]):
                        high_impact_alerts.append(f"⚠️ {clean_txt}")
        except Exception:
            pass

    # 3. Crypto Native Major News (CoinDesk & CoinTelegraph)
    if t_cd:
        try:
            root = ET.fromstring(t_cd)
            for item in root.findall("./channel/item")[:3]:
                t = item.find("title")
                if t is not None and t.text:
                    headlines.append(f"[Crypto Macro] {re.sub(r'<[^>]+>', '', t.text).strip()}")
        except Exception:
            pass

    if t_ct:
        try:
            root = ET.fromstring(t_ct)
            for item in root.findall("./channel/item")[:3]:
                t = item.find("title")
                if t is not None and t.text:
                    headlines.append(f"[Crypto Market] {re.sub(r'<[^>]+>', '', t.text).strip()}")
        except Exception:
            pass

    # 4. Sentiment Index
    if t_fng:
        try:
            data = json.loads(t_fng).get("data", [])[0]
            headlines.append(f"[Global Sentiment] Fear & Greed Index: {data.get('value', '50')} ({data.get('value_classification', 'Neutral')})")
        except Exception:
            pass

    final_report = []
    if high_impact_alerts:
        final_report.append("[🚨 HIGH-IMPACT MACRO / FINANCIAL CATALYST DETECTED]\n" + "\n".join(high_impact_alerts[:3]))
    if headlines:
        final_report.append("[LIVE FINANCIAL & CRYPTO FEEDS]\n" + "\n".join(headlines[:10]))

    return "\n\n".join(final_report) if final_report else "Normal global liquidity and financial conditions."

async def fetch_full_market_universe_tickers() -> list:
    try:
        tickers = await asyncio.to_thread(public_exchange.fetch_tickers)
    except Exception as e:
        log_event("ERROR", "SCANNER_TICKERS", f"Exchange fetch_tickers failure: {e}")
        return []

    funding_dict = {}
    try:
        premium_data = await asyncio.to_thread(public_exchange.fapiPublicGetPremiumIndex)
        for item in premium_data:
            s_raw = item.get("symbol")
            fr_raw = item.get("lastFundingRate")
            if s_raw and fr_raw is not None:
                funding_dict[s_raw] = float(fr_raw) * 100.0
    except Exception as e:
        log_event("WARN", "SCANNER_FUNDING", f"Premium index fetch exception: {e}")

    cooldown_list = get_cooldown_symbols()
    market_universe = []

    for symbol, data in tickers.items():
        if not data or not (":USDT" in symbol or ":USDC" in symbol):
            continue
        if symbol in cooldown_list:
            continue

        quote_vol = float(data.get("quoteVolume") or 0.0)
        pct_change = float(data.get("percentage") or 0.0)
        last_price = float(data.get("last") or data.get("close") or 0.0)
        high_24h = float(data.get("high") or last_price)
        low_24h = float(data.get("low") or last_price)

        if last_price <= 0 or quote_vol < 1_000_000:
            continue

        clean_pair = symbol.split(":")[0].replace("/", "")
        funding_rate = funding_dict.get(clean_pair, 0.0)

        market_universe.append({
            "s": symbol,
            "p": last_price,
            "c": round(pct_change, 2),
            "v_m": round(quote_vol / 1_000_000.0, 1),
            "fr": round(funding_rate, 4),
            "oi_m": 0.0,
            "book_ratio": 1.0,
            "whale_ls_ratio": 1.0,
            "hi": high_24h,
            "lo": low_24h
        })

    market_universe.sort(key=lambda x: x["v_m"], reverse=True)
    top_candidates = market_universe[:35]

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_microstructure_data_async(session, cand["s"].split(":")[0].replace("/", ""))
            for cand in top_candidates
        ]
        micro_results = await asyncio.gather(*tasks)

    for cand, res in zip(top_candidates, micro_results):
        oi_contracts = res.get("oi", 0.0)
        cand["book_ratio"] = res.get("book_ratio", 1.0)
        cand["whale_ls_ratio"] = res.get("whale_ls_ratio", 1.0)
        if oi_contracts > 0:
            cand["oi_m"] = round((oi_contracts * cand["p"]) / 1_000_000.0, 1)
        else:
            cand["oi_m"] = round(cand["v_m"] * 0.15, 1)

    return market_universe

def analyze_macro_universe_with_ai(market_universe: list, live_news: str, btc_regime: dict, btc_24h_pct: float, capital_info: dict = None, active_positions: list = None, pending_watchlist: list = None) -> dict:
    user_rules = get_active_strategy_rules()
    lessons = get_contextual_trade_lessons()
    top_candidates = market_universe[:35]

    adx_val = float(btc_regime.get("adx") or 18.0)
    regime_str = btc_regime.get("regime", "RANGING")

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0
    cap_mode = capital_info.get("mode", TRADING_MODE) if capital_info else TRADING_MODE

    weekend_clause = ""
    if is_weekend_regime():
        weekend_clause = """
        [WEEKEND REGIME ACTIVE: CME CLOSED & LOW INSTITUTIONAL VOLUME]
        - Strictly ban TREND_MOMENTUM breakouts.
        - Prioritize SWEEP_REVERSION only at range extremes.
        """

    macro_alert = ""
    if btc_24h_pct <= -0.8:
        macro_alert = f"""
        [CRITICAL BEARISH REGIME: BTC DOWN ({btc_24h_pct:+.2f}%)]
        - Macro trend is firmly DOWNSIDE.
        - STRICTLY DISQUALIFY ALL LONGS ON ALTCOINS.
        - Focus exclusively on SHORTING relative weakness tokens with weak order books.
        """

    sideways_clause = ""
    if regime_str == "RANGING" or adx_val < 22.0:
        sideways_clause = """
        [SIDEWAYS / CHOP MARKET REGIME ACTIVE]
        - Market is range-bound (ADX < 22).
        - Breakouts are prone to failure; prioritize SWEEP_REVERSION or quick FAST TRADE mean-reversions.
        - Set coin_trend to 'NEUTRAL CONSOLIDATION'.
        """

    active_pos_section = "No active positions currently."
    if active_positions:
        active_pos_section = json.dumps(active_positions)

    pending_watch_section = "No pending limit orders on watchlist."
    if pending_watchlist:
        pending_watch_section = json.dumps(pending_watchlist)

    prompt = f"""
    You are the Chief Quantitative Strategist & Portfolio Risk Sentinel for {BOT_NAME}.
    PRIMARY OBJECTIVES:
    1. Maximize Positive Expected Value (+EV) by synthesizing Global Financial News, Central Bank Catalysts, and Order Flow Microstructure.
    2. Adapt strategy dynamically: Unleash TREND_MOMENTUM on institutional weekdays, enforce SWEEP_REVERSION / fast scalp harvesting during Chop/Weekends.
    3. Defend active positions: Detect sudden macro shock reversals and order book flips.
    4. Audit pending watchlist limit orders: Verify whether unfilled limit orders are still technically/fundamentally applicable or should be cancelled to free capital.

    [CURRENT ACCOUNT CAPITAL & MARGIN CAPACITY]
    • Total Balance: ${cap_total:.2f} USD | Free Margin: ${cap_free:.2f} USD | Mode: {cap_mode}

    [MACRO & GLOBAL FINANCIAL CONTEXT]
    BTC Macro Regime: {regime_str} | ADX: {adx_val} | BTC 24h Change: {btc_24h_pct:+.2f}%
    {macro_alert}
    {sideways_clause}
    {weekend_clause}
    
    {live_news}

    [ACTIVE POSITIONS - REVERSAL AUDIT]
    Format: [symbol, action, entry, curr_price, pnl_pct, peak_gain_pct, pullback_from_peak]
    {active_pos_section}

    [PENDING WATCHLIST & LIMIT ORDERS AUDIT (Stage 0 - Review for Invalidation)]
    Format: [id, symbol, action, planned_entry, curr_price, dist_to_entry_pct, thesis, pending_mins, order_id]
    {pending_watch_section}

    [REAL-TIME MICROSTRUCTURE UNIVERSE (Top 35 Liquid Perps)]
    Format: [s: symbol, p: mark price, c: 24h %, v_m: volume M$, fr: funding %, oi_m: OI M$, book_ratio: bid/ask depth, hi: 24h high, lo: 24h low]
    {json.dumps(top_candidates)}

    [USER DIRECTIVES]
    {user_rules}

    [RECENT POST-MORTEM LESSONS]
    {lessons}

    ======================================================================
    EXECUTION & RISK DIRECTIVE:
    ======================================================================
    1. Macro & News Alignment:
       - If High-Impact Catalysts (FOMC/CPI/War) are detected or BTC < -0.8%, focus on SHORT relative weakness altcoins.
       - Disqualify long breakouts when broader financial markets or crypto are in risk-off mode.
    2. Adaptive Strategy Mode:
       - Trending weekdays (ADX >= 25): Select TREND_MOMENTUM with BEARISH DOWNTREND or BULLISH UPTREND.
       - Ranging / Chop (ADX < 22) or Weekends: Select SWEEP_REVERSION with NEUTRAL CONSOLIDATION.
    3. Traps to Disqualify:
       - No longs when BTC is red.
       - No shorts if the asset has already dumped > -10% in 24h (capitulation wick risk).
    4. Sudden Reversal Management:
       - For any active position showing momentum exhaustion or adverse flow, prescribe HARVEST_NOW or TIGHTEN_SL.
    5. Watchlist Applicability Audit:
       - For each pending Stage 0 limit setup: check if the original thesis holds. If market trend flipped, price ran away without fill, or order book structure deteriorated, prescribe CANCEL_INVALID to purge it. Otherwise, prescribe KEEP.
    6. Multi-Slot Portfolio Allocation:
       - Return 4 to 5 distinct high-conviction setups in `selected_setups` so all 4 portfolio slots can be filled simultaneously.

    Respond ONLY in strict JSON format:
    {{
        "action": "TRADE" | "HOLD",
        "selected_setups": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "action": "LONG" | "SHORT",
                "strategy_mode": "TREND_MOMENTUM" | "SWEEP_REVERSION",
                "trade_speed": "FAST TRADE" | "SLOW TRADE",
                "coin_trend": "BULLISH UPTREND" | "BEARISH DOWNTREND" | "NEUTRAL CONSOLIDATION",
                "strategic_thesis": "1-sentence explicit technical/news/funding edge"
            }}
        ],
        "active_position_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "HOLD" | "TIGHTEN_SL" | "HARVEST_NOW",
                "reason": "1-sentence explanation"
            }}
        ],
        "watchlist_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "KEEP" | "CANCEL_INVALID",
                "reason": "1-sentence explanation why setup remains valid or is invalidated"
            }}
        ],
        "macro_verdict": "1-sentence summary of market regime and strategy activated"
    }}
    """

    try:
        t0 = time.time()
        res = ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.15
        )
        duration_ms = (time.time() - t0) * 1000.0
        raw_response = res.choices[0].message.content or "{}"
        parsed = extract_clean_json(raw_response)
        log_ai_interaction("GLOBAL_UNIVERSE", "HOURLY_HYBRID_EVALUATION", prompt, raw_response, duration_ms)
        return parsed
    except Exception as e:
        log_event("ERROR", "AI_SCANNER", f"AI Parse Error: {e}")
        return {"action": "HOLD", "selected_setups": [], "active_position_directives": [], "watchlist_directives": [], "macro_verdict": f"AI Error: {e}"}

def analyze_whale_microstructure_with_ai(market_universe: list, btc_regime: dict, btc_24h_pct: float, capital_info: dict = None, active_positions: list = None, pending_watchlist: list = None) -> dict:
    top_candidates = market_universe[:35]

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0

    downside_bias = ""
    if btc_24h_pct <= -0.8:
        downside_bias = "[ALERT: BEARISH PRESSURE] Target LIQUIDATION FLUSH SHORTS where retail/whale longs are trapped."

    active_pos_section = "No active positions currently."
    if active_positions:
        active_pos_section = json.dumps(active_positions)

    pending_watch_section = "No pending limit orders on watchlist."
    if pending_watchlist:
        pending_watch_section = json.dumps(pending_watchlist)

    prompt = f"""
    You are the Senior Institutional Order Flow Specialist & Risk Defender for {BOT_NAME}.
    PRIMARY OBJECTIVES:
    1. Exploit whale positioning WITHOUT falling for spoofed bid walls or crowded liquidation traps.
    2. Protect active profits: Identify sudden institutional distribution or whale flips against open positions.
    3. Audit pending watchlist limit orders: Purge limit orders where order flow, depth ratio, or top trader ratios have flipped against the planned entry.

    [PORTFOLIO CAPITAL ANCHOR]
    • Total Balance: ${cap_total:.2f} USD | Free Margin: ${cap_free:.2f} USD

    [BTC MACRO ANCHOR]
    Regime: {btc_regime.get('regime')} | ADX: {btc_regime.get('adx')} | BTC 24h Change: {btc_24h_pct:+.2f}%
    {downside_bias}

    [ACTIVE POSITIONS - WHALE REVERSAL AUDIT]
    {active_pos_section}

    [PENDING WATCHLIST & LIMIT ORDERS AUDIT (Stage 0 - Review for Invalidation)]
    Format: [id, symbol, action, planned_entry, curr_price, dist_to_entry_pct, thesis, pending_mins, order_id]
    {pending_watch_section}

    [NATIVE BINANCE DERIVATIVES MICROSTRUCTURE]
    Format: [s: symbol, p: price, c: 24h %, v_m: volume M$, fr: funding %, oi_m: Open Interest M$, book_ratio: bid/ask depth, whale_ls_ratio: top trader long/short ratio, hi: 24h high, lo: 24h low]
    {json.dumps(top_candidates)}

    INSTITUTIONAL RULES & TRAP PREVENTION:
    1. CROWDED TRAP RULE:
       - If BTC is red (< -0.5%) and whale_ls_ratio > 1.6, DO NOT LONG. Top accounts are trapped long. Target these for LIQUIDATION FLUSH SHORTS.
    2. SPOOFING FILTER:
       - Book_ratio > 2.5 near 24h lows is often fake bid support. Require negative funding and relative strength vs BTC before considering any long.
    3. MOMENTUM SHORTS:
       - When BTC is dumping, identify high OI tokens with positive funding and dropping prices (whales distributing into retail exit liquidity).
    4. REVERSAL HARVEST:
       - If an active position has top traders flipping against it or massive order book imbalance against it, recommend HARVEST_NOW or TIGHTEN_SL.
    5. WATCHLIST APPLICABILITY AUDIT:
       - If a pending limit order has order flow or top trader ratios flipping adversely, recommend CANCEL_INVALID to free the slot. Otherwise, recommend KEEP.

    Respond ONLY in strict JSON:
    {{
        "action": "TRADE" | "HOLD",
        "whale_setups": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "action": "LONG" | "SHORT",
                "movement_probability": 85,
                "whale_pattern": "LIQUIDATION_FLUSH" | "DISTRIBUTION_TRAP" | "GENUINE_ACCUMULATION",
                "orderflow_thesis": "1-sentence technical edge citing OI, funding, and top trader positioning"
            }}
        ],
        "active_position_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "HOLD" | "TIGHTEN_SL" | "HARVEST_NOW",
                "reason": "1-sentence explanation"
            }}
        ],
        "watchlist_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "KEEP" | "CANCEL_INVALID",
                "reason": "1-sentence explanation"
            }}
        ],
        "whale_verdict": "1-sentence summary of institutional positioning"
    }}
    """

    try:
        t0 = time.time()
        res = ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.10
        )
        duration_ms = (time.time() - t0) * 1000.0
        raw_response = res.choices[0].message.content or "{}"
        parsed = extract_clean_json(raw_response)
        log_ai_interaction("WHALE_RADAR", "BINANCE_MICROSTRUCTURE_WHALE_SCAN", prompt, raw_response, duration_ms)
        return parsed
    except Exception as e:
        log_event("ERROR", "WHALE_SCANNER", f"AI Whale Scan Error: {e}")
        return {"action": "HOLD", "whale_setups": [], "active_position_directives": [], "watchlist_directives": [], "whale_verdict": f"AI Parsing Error: {e}"}

def analyze_scalp_opportunities_with_ai(market_universe: list, btc_regime: dict, btc_24h_pct: float, capital_info: dict = None, active_positions: list = None, pending_watchlist: list = None) -> dict:
    top_scalp_candidates = [m for m in market_universe if m.get("v_m", 0) >= 10.0][:25]
    if not top_scalp_candidates:
        top_scalp_candidates = market_universe[:20]

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0

    btc_market_bias = ""
    if btc_24h_pct < -0.4:
        btc_market_bias = f"""
    [CRITICAL MARKET REGIME DIRECTIVE: BEARISH / RED BTC ({btc_24h_pct:+.2f}%)]
    - Bitcoin is currently red/dumping.
    - NEVER propose LONG setups on falling altcoins. (Bid walls during red tapes are often exit liquidity / liquidation traps).
    - PRIORITIZE SHORT setups on high-OI, ask-heavy, or breakdown altcoins in alignment with market weakness.
    """
    elif btc_24h_pct > 0.8:
        btc_market_bias = f"""
    [CRITICAL MARKET REGIME DIRECTIVE: BULLISH / GREEN BTC ({btc_24h_pct:+.2f}%)]
    - Bitcoin is strong and rising.
    - PRIORITIZE LONG breakout setups on high relative-strength altcoins with strong bid support.
    """

    prompt = f"""
    You are the Chief High-Speed Scalp Sniper Specialist for {BOT_NAME}.
    PRIMARY OBJECTIVE:
    Identify ultra-fast, high-probability SCALP opportunities (5m-15m micro-swings) with MAXIMAL EXPECTED PROFIT (+EV).
    This evaluation runs strictly on the HOURLY discovery cycle to capture the highest-velocity, highest-profit scalp setups for the upcoming hour.

    [SCALP PROFIT PRIORITIZATION DIRECTIVE]
    - PRIORITIZE MAXIMUM DOLLAR PROFIT: Focus exclusively on tokens where high order flow momentum, depth imbalance, and volatility provide the largest DOLLAR GAIN potential.
    - DISQUALIFY DEAD CHOP: Reject low-volatility, low-profit ranging assets where slippage or fees erode edge.
    - HIGH CONVICTION ONLY: Require clear institutional order book backing (bid/ask depth skew >= 1.30 for LONG, <= 0.70 for SHORT).

    [SCALP TRADING SPECIFICATIONS]
    - Horizon: High-velocity turnover (< 45 minutes pending, rapid fills).
    - Profit Targets: TP1 (+1.0R, ~+0.8% to +1.3%), TP2 (+1.8R), TP3 (+2.6R).
    - Stop-Loss: Tight micro-stop (0.8% to 1.3% risk distance).
    - Capital Efficiency: 2.0x-2.5x larger position size with 50% early cash banking at +0.5R milestone.

    [PORTFOLIO CAPITAL ANCHOR]
    • Total Balance: ${cap_total:.2f} USD | Free Margin: ${cap_free:.2f} USD

    [BTC HOURLY MACRO ANCHOR]
    Regime: {btc_regime.get('regime')} | ADX: {btc_regime.get('adx')} | BTC 24h Change: {btc_24h_pct:+.2f}%
    {btc_market_bias}

    [TOP LIQUID PERPETUALS FOR SCALPING]
    Format: [s: symbol, p: price, c: 24h %, v_m: volume M$, fr: funding %, oi_m: Open Interest M$, book_ratio: bid/ask depth, whale_ls_ratio: top trader long/short ratio, hi: 24h high, lo: 24h low]
    {json.dumps(top_scalp_candidates)}

    SCALP SNIPER CRITERIA:
    1. ORDERBOOK IMBALANCE:
       - book_ratio >= 1.30 with flat/healthy funding -> High-conviction LONG scalp.
       - book_ratio <= 0.70 with negative/flat funding -> High-conviction SHORT scalp.
    2. LIQUIDITY SWEEP & REVERSAL:
       - Asset tested 24h high/low with top accounts overextended (whale_ls_ratio > 2.0 or < 0.6) -> Fast mean-reversion scalp.
    3. MOMENTUM THRUST & PROFIT EXPANSION:
       - High volume (v_m > $25M) breaking out in alignment with BTC micro-trend.
    4. NO EXTENDED CHASE:
       - Disqualify assets already up > +10% or down < -10% in 24h (capitulation/exhaustion danger).

    SCALPING PROBABILITY ASSESSMENT:
    For each setup, compute an explicit "scalp_probability" integer between 70 and 95 based on technical edge confluence, book depth skew, and market alignment.
    Select 4 to 5 distinct top setups with scalp_probability >= 72 so all 4 concurrent scalp slots can be populated.

    Respond ONLY in strict JSON:
    {{
        "action": "TRADE" | "HOLD",
        "scalp_setups": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "action": "LONG" | "SHORT",
                "scalp_probability": 85,
                "expected_profit_potential": "VERY_HIGH" | "HIGH",
                "scalp_type": "ORDERBOOK_IMBALANCE" | "LIQUIDITY_SWEEP" | "VOLATILITY_EXPANSION",
                "trade_speed": "SCALP TRADE",
                "coin_trend": "SCALP_SNIPER",
                "scalp_thesis": "1-sentence high-speed scalp justification citing book depth, momentum, and profit edge"
            }}
        ],
        "scalp_verdict": "1-sentence summary of hourly scalp market conditions"
    }}
    """

    try:
        t0 = time.time()
        res = ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.15
        )
        duration_ms = (time.time() - t0) * 1000.0
        raw_response = res.choices[0].message.content or "{}"
        parsed = extract_clean_json(raw_response)
        log_ai_interaction("SCALP_SNIPER", "HIGH_SPEED_SCALP_SCAN", prompt, raw_response, duration_ms)
        return parsed
    except Exception as e:
        log_event("ERROR", "SCALP_SCANNER", f"AI Scalp Scan Error: {e}")
        return {"action": "HOLD", "scalp_setups": [], "scalp_verdict": f"AI Parsing Error: {e}"}

# ==============================================================================
# PIPELINES
# ==============================================================================

async def execute_unified_hourly_discovery_pipeline(context: ContextTypes.DEFAULT_TYPE = None, is_manual_scan: bool = False, scan_type: str = "ALL"):
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not is_scanning_enabled():
        return

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT value FROM bot_state WHERE key = 'eval_running'")
        row = c.fetchone()
        if row and row["value"] == "1":
            log_event("INFO", "SCANNER", "Evaluation currently in progress. Skipping duplicate trigger.")
            return
        conn.execute("UPDATE bot_state SET value = '1' WHERE key = 'eval_running'")

    t_start = time.time()
    try:
        # 1. Reconcile & Auto-Sync Live Binance Positions BEFORE checking slot capacity
        if TRADING_MODE in ("LIVE", "TESTNET"):
            try:
                live_positions = await asyncio.to_thread(exchange.fetch_positions)
                open_live_symbols = set()
                for lp in live_positions:
                    contracts = abs(float(lp.get("contracts") or 0.0))
                    if contracts > 0:
                        raw_sym = lp.get("symbol", "")
                        open_live_symbols.add(raw_sym)
                        b_side = str(lp.get("side", "long")).upper()
                        entry_px = float(lp.get("entryPrice") or lp.get("markPrice") or 0.0)
                        pos_notional = float(lp.get("notional") or (contracts * entry_px))
                        
                        # Check if already present in database
                        with get_db_connection() as c_check:
                            c_cur = c_check.cursor()
                            c_cur.execute("SELECT id, status, stage FROM watched_signals WHERE symbol = ? AND status = 'WATCHING'", (raw_sym,))
                            existing_row = c_cur.fetchone()
                            if not existing_row:
                                # Auto-import manual or external Binance trade into bot tracking and attach protective SL/TP brackets
                                est_sl = round(entry_px * 0.990, 6) if b_side == "LONG" else round(entry_px * 1.010, 6)
                                est_t1 = round(entry_px * 1.011, 6) if b_side == "LONG" else round(entry_px * 0.989, 6)
                                est_t2 = round(entry_px * 1.020, 6) if b_side == "LONG" else round(entry_px * 0.980, 6)
                                est_t3 = round(entry_px * 1.030, 6) if b_side == "LONG" else round(entry_px * 0.970, 6)
                                sync_brackets = await asyncio.to_thread(
                                    place_binance_tp_sl_brackets, raw_sym, b_side, contracts, est_sl, est_t1, est_t2, est_t3
                                )
                                c_check.execute("""
                                    INSERT INTO watched_signals
                                    (symbol, action, entry_limit, initial_sl, current_sl, target_1, target_2, target_3,
                                     position_size_usd, stage, highest_favorable_price, status, remaining_ratio, partial_realized_pnl, confidence, trade_speed,
                                     coin_trend, strategic_thesis, movement_tag, binance_entry_order_id, binance_sl_order_id, binance_tp1_id, binance_tp2_id, binance_tp3_id,
                                     executed_qty, trading_mode, timestamp, last_updated)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 'WATCHING', 1.0, 0.0, 0.90, 'FAST TRADE', 'BINANCE_SYNC', 'Auto-synced from active Binance position', '🔄 BINANCE_SYNC', '', ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                    ON CONFLICT(symbol) DO UPDATE SET
                                        action = excluded.action,
                                        status = 'WATCHING', stage = 1, executed_qty = excluded.executed_qty,
                                        entry_limit = excluded.entry_limit, initial_sl = excluded.initial_sl, current_sl = excluded.current_sl,
                                        target_1 = excluded.target_1, target_2 = excluded.target_2, target_3 = excluded.target_3,
                                        highest_favorable_price = excluded.entry_limit,
                                        binance_entry_order_id = '',
                                        binance_sl_order_id = excluded.binance_sl_order_id,
                                        binance_tp1_id = excluded.binance_tp1_id,
                                        binance_tp2_id = excluded.binance_tp2_id,
                                        binance_tp3_id = excluded.binance_tp3_id,
                                        remaining_ratio = 1.0, partial_realized_pnl = 0.0,
                                        movement_tag = '🔄 BINANCE_SYNC', timestamp = CURRENT_TIMESTAMP, last_updated = CURRENT_TIMESTAMP
                                """, (
                                    raw_sym, b_side, entry_px, est_sl, est_sl, est_t1, est_t2, est_t3,
                                    pos_notional, entry_px,
                                    sync_brackets["sl_id"], sync_brackets["tp1_id"], sync_brackets["tp2_id"], sync_brackets["tp3_id"],
                                    contracts, TRADING_MODE
                                ))
                                log_event("INFO", "BINANCE_SYNC", f"Auto-synced open Binance {b_side} position on {raw_sym}: {contracts} contracts (${pos_notional:.1f}) with SL/TP brackets.")

                # Reconcile any WATCHING rows whose positions are now closed (0 contracts) on Binance, or stale Stage 0 rows in MARKET mode
                with get_db_connection() as c_reconcile:
                    c_cur = c_reconcile.cursor()
                    c_cur.execute("SELECT id, symbol, stage, action, entry_limit, current_sl, partial_realized_pnl, remaining_ratio FROM watched_signals WHERE status = 'WATCHING'")
                    for db_pos in c_cur.fetchall():
                        b_sym = bridge_symbol_formats(db_pos["symbol"])["ccxt"]
                        if db_pos["symbol"] not in open_live_symbols and b_sym not in open_live_symbols:
                            if int(db_pos["stage"] or 0) >= 1 or (TRADE_ENTRY_TYPE == "MARKET" and SCALP_ENTRY_TYPE == "MARKET"):
                                c_reconcile.execute("""
                                    UPDATE watched_signals
                                    SET status = 'COMPLETED', remaining_ratio = 0.0, exit_reason = 'EXCHANGE_CLOSED_SYNC',
                                        binance_sl_order_id = '', binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '',
                                        last_updated = CURRENT_TIMESTAMP
                                    WHERE id = ?
                                """, (db_pos["id"],))
                                log_event("INFO", "BINANCE_SYNC", f"Freed slot {db_pos['symbol']} (not open on Binance).")
            except Exception as e_pos:
                if "-2015" in str(e_pos):
                    ip_addr = await asyncio.to_thread(get_outbound_ip_sync)
                    log_event("ERROR", "BINANCE_AUTH", f"Binance -2015 Auth Error on fetch_positions: Whitelist Server IP [{ip_addr}] in Binance API Management & enable 'Enable Futures'.")
                else:
                    log_event("WARN", "BINANCE_SYNC", f"Live position sync warning: {e_pos}")

        watched_cnt, daily_cnt = count_active_and_daily_trades()
        
        if watched_cnt >= MAX_CONCURRENT_POSITIONS:
            log_event("INFO", "SCANNER", f"Watched slots full ({watched_cnt}/{MAX_CONCURRENT_POSITIONS}). Standing aside.")
            return

        if daily_cnt >= MAX_DAILY_TRADES:
            log_event("INFO", "SCANNER", f"Daily trade limit reached ({daily_cnt}/{MAX_DAILY_TRADES} in 24h). Standing aside.")
            return

        prune_old_logs()

        daily_loss = check_rolling_24h_drawdown()
        if daily_loss <= MAX_24H_LOSS_THRESHOLD:
            log_event("WARN", "SCANNER", f"24h drawdown floor reached ({daily_loss:.2f}%).")
            return

        # 2. Fetch Current Capital & Dynamic Balance
        capital_info = fetch_current_account_capital()
        account_balance = float(capital_info.get("total", 10000.0))
        log_event("INFO", "CAPITAL", f"Current Capital Checked: ${account_balance:.2f} USD ({capital_info.get('mode', TRADING_MODE)})")

        active_positions_payload = []
        pending_watchlist_payload = []
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM watched_signals WHERE status = 'WATCHING'")
            all_watched_rows = c.fetchall()
            for r in all_watched_rows:
                sym = r["symbol"]
                act = r["action"]
                entry = float(r["entry_limit"] or 0.0)
                sl = float(r["current_sl"] or 0.0)
                stage_val = int(r["stage"] or 0)
                curr_price = await asyncio.to_thread(fetch_real_perp_market_price, sym)
                created_at = parse_database_timestamp(r["timestamp"])
                mins_elapsed = round((utc_now() - created_at).total_seconds() / 60.0, 1)

                if stage_val >= 1:
                    try:
                        high_fav = float(r["highest_favorable_price"] or entry)
                    except (ValueError, TypeError):
                        high_fav = entry
                    if curr_price > 0 and entry > 0:
                        pnl_pct = ((curr_price - entry) / entry * 100.0) if act == "LONG" else ((entry - curr_price) / entry * 100.0)
                        peak_gain = ((high_fav - entry) / entry * 100.0) if act == "LONG" else ((entry - high_fav) / entry * 100.0)
                        pullback = ((high_fav - curr_price) / high_fav * 100.0) if act == "LONG" else ((curr_price - high_fav) / high_fav * 100.0)
                        active_positions_payload.append({
                            "id": r["id"],
                            "symbol": sym,
                            "action": act,
                            "entry": entry,
                            "current_sl": sl,
                            "curr_price": curr_price,
                            "pnl_pct": round(pnl_pct, 2),
                            "peak_gain_pct": round(peak_gain, 2),
                            "pullback_from_peak": round(pullback, 2),
                            "sl_order_id": str(r["binance_sl_order_id"] or ""),
                            "tp1_order_id": str(dict(r).get("binance_tp1_id", "") or ""),
                            "executed_qty": float(r["executed_qty"] or 0.0),
                            "remaining_ratio": float(dict(r).get("remaining_ratio", 1.0) or 1.0),
                            "partial_realized_pnl": float(dict(r).get("partial_realized_pnl", 0.0) or 0.0)
                        })
                else:
                    dist_pct = round(abs(curr_price - entry) / entry * 100.0, 2) if entry > 0 else 0.0
                    pending_watchlist_payload.append({
                        "id": r["id"],
                        "symbol": sym,
                        "action": act,
                        "planned_entry": entry,
                        "curr_price": curr_price,
                        "dist_to_entry_pct": dist_pct,
                        "thesis": r["strategic_thesis"] or "",
                        "pending_mins": mins_elapsed,
                        "order_id": str(r["binance_entry_order_id"] or "")
                    })

        # 3. Market Data Ingestion
        def _safe_fetch_btc_ticker():
            try:
                return public_exchange.fetch_ticker("BTC/USDT:USDT")
            except Exception as e_t:
                log_event("WARN", "BTC_TICKER", f"BTC ticker fetch warning: {e_t}")
                return {"percentage": 0.0}

        btc_candles_task = asyncio.to_thread(fetch_candles, "BTC/USDT:USDT", "1h", 35)
        btc_ticker_task = asyncio.to_thread(_safe_fetch_btc_ticker)
        news_task = fetch_live_world_and_crypto_news()
        tickers_task = fetch_full_market_universe_tickers()

        btc_candles, btc_ticker, live_news, market_universe = await asyncio.gather(
            btc_candles_task, btc_ticker_task, news_task, tickers_task
        )

        btc_regime = calculate_adx_regime(btc_candles) if btc_candles else {"regime": "RANGING", "adx": 18.0}
        btc_24h_pct = float(btc_ticker.get("percentage") or 0.0)

        # 4. Concurrently Run AI Macro, AI Whale, and AI Scalp Discovery Prompts
        macro_task = asyncio.to_thread(
            analyze_macro_universe_with_ai,
            market_universe, live_news, btc_regime, btc_24h_pct, capital_info, active_positions_payload, pending_watchlist_payload
        )
        whale_task = asyncio.to_thread(
            analyze_whale_microstructure_with_ai,
            market_universe, btc_regime, btc_24h_pct, capital_info, active_positions_payload, pending_watchlist_payload
        )
        scalp_task = asyncio.to_thread(
            analyze_scalp_opportunities_with_ai,
            market_universe, btc_regime, btc_24h_pct, capital_info, active_positions_payload, pending_watchlist_payload
        )

        macro_decision, whale_decision, scalp_decision = await asyncio.gather(macro_task, whale_task, scalp_task)

        # 5. Apply AI Watchlist Applicability Directives (Purge Invalid Pending Setups)
        all_watchlist_directives = (
            macro_decision.get("watchlist_directives", []) +
            whale_decision.get("watchlist_directives", [])
        )

        for w_dir in all_watchlist_directives:
            w_sym = w_dir.get("symbol")
            w_verdict = str(w_dir.get("verdict") or "KEEP").upper()
            w_reason = w_dir.get("reason", "")
            if not w_sym or w_verdict != "CANCEL_INVALID":
                continue

            match_pending = next((p for p in pending_watchlist_payload if p["symbol"] == w_sym), None)
            if not match_pending:
                continue

            # Cancel pending limit order on Binance
            if match_pending["order_id"]:
                await asyncio.to_thread(cancel_binance_order, match_pending["symbol"], match_pending["order_id"])

            with get_db_connection() as conn:
                conn.execute("""
                    UPDATE watched_signals 
                    SET status = 'INVALIDATED', realized_pnl = 0.0, movement_tag = '🚫 AI_PURGED_INVALID', last_updated = CURRENT_TIMESTAMP 
                    WHERE id = ?
                """, (match_pending["id"],))

            log_event("INFO", "AI_WATCHLIST", f"Pending setup {w_sym} cancelled by AI audit: {w_reason}")
            if chat_id and context:
                msg = (
                    f"🚫 <b>WATCHLIST AUDIT: SETUP CANCELLED</b>\n\n"
                    f"• <b>Contract:</b> <code>{match_pending['symbol']}</code> ({match_pending['action']})\n"
                    f"• <b>Planned Entry:</b> ${match_pending['planned_entry']:.6f} (Mark: ${match_pending['curr_price']:.6f})\n"
                    f"• <b>AI Verdict:</b> <i>{html.escape(w_reason)}</i>\n"
                    f"🤖 <b>Cancelled pending order on Binance. Slot freed for higher EV setups.</b>"
                )
                try:
                    await context.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML")
                except Exception:
                    pass

        # 6. Apply AI Active Position Sudden Reversal & Profit Management Directives
        all_reversal_directives = (
            macro_decision.get("active_position_directives", []) +
            whale_decision.get("active_position_directives", [])
        )

        for directive in all_reversal_directives:
            d_sym = directive.get("symbol")
            d_verdict = directive.get("verdict", "HOLD").upper()
            d_reason = directive.get("reason", "")
            if not d_sym or d_verdict == "HOLD":
                continue

            match_pos = next((p for p in active_positions_payload if p["symbol"] == d_sym), None)
            if not match_pos:
                continue

            if d_verdict == "HARVEST_NOW" and match_pos["pnl_pct"] >= 0.65:
                # Emergency Harvest (only when net profit comfortably exceeds exchange fees)
                if match_pos["sl_order_id"]:
                    await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                if match_pos.get("tp1_order_id"):
                    await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["tp1_order_id"])
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, match_pos["symbol"], match_pos["action"], match_pos["executed_qty"])

                net_leg_pnl = match_pos["pnl_pct"] - ROUND_TRIP_FEE_PCT
                final_pnl = round(match_pos["partial_realized_pnl"] + (match_pos["remaining_ratio"] * net_leg_pnl), 2)
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, exit_reason = 'AI_REVERSAL_HARVEST', movement_tag = '🎯 AI_REVERSAL_HARVEST', last_updated = CURRENT_TIMESTAMP WHERE id = ?", (final_pnl, match_pos["id"]))

                log_event("INFO", "AI_REVERSAL", f"Emergency profit harvested on {d_sym}: {final_pnl:+.2f}% net. Reason: {d_reason}")
                if chat_id and context:
                    msg = (
                        f"💰 <b>AI PROFIT HARVEST: REVERSAL DEFENSE</b>\n\n"
                        f"• <b>Contract:</b> <code>{match_pos['symbol']}</code> ({match_pos['action']})\n"
                        f"• <b>Exit Mark:</b> ${match_pos['curr_price']:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                        f"• <b>Total Realized Trade PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                        f"• <b>AI Diagnostic:</b> <i>{html.escape(d_reason)}</i>\n"
                        f"🤖 <b>Closed immediately on Binance to protect profit.</b>"
                    )
                    try:
                        await context.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML")
                    except Exception:
                        pass

            elif d_verdict == "TIGHTEN_SL" and match_pos["pnl_pct"] >= 0.60:
                # Tighten SL to Break-Even+Profit (+0.20%) once trade is at least +0.60% in profit
                tighter_sl = round(match_pos["entry"] * 1.0020, 6) if match_pos["action"] == "LONG" else round(match_pos["entry"] * 0.9980, 6)
                is_better = (tighter_sl > match_pos["current_sl"]) if match_pos["action"] == "LONG" else (tighter_sl < match_pos["current_sl"])
                is_safe_side = (match_pos["curr_price"] >= tighter_sl * 1.0020) if match_pos["action"] == "LONG" else (match_pos["curr_price"] <= tighter_sl * 0.9980)
                if is_better and is_safe_side:
                    if match_pos["sl_order_id"]:
                        await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                    new_sl_id = ""
                    if TRADING_MODE in ("LIVE", "TESTNET"):
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, match_pos["symbol"], match_pos["action"], match_pos["executed_qty"], tighter_sl)

                    with get_db_connection() as conn:
                        conn.execute("UPDATE watched_signals SET current_sl = ?, binance_sl_order_id = ?, movement_tag = '🛡️ AI_TIGHTENED_SL', last_updated = CURRENT_TIMESTAMP WHERE id = ? AND status = 'WATCHING'", (tighter_sl, new_sl_id, match_pos["id"]))

                    log_event("INFO", "AI_REVERSAL", f"Stop-Loss tightened on {d_sym} to ${tighter_sl:.6f}. Reason: {d_reason}")

        # 7. Profit Prioritization & Candidate Ranking Engine
        raw_candidates = []

        # Ingest AI Macro Setups
        if macro_decision.get("action") == "TRADE":
            for setup in macro_decision.get("selected_setups", []):
                raw_candidates.append({
                    "source": "AI_MACRO",
                    "symbol": setup.get("symbol"),
                    "action": setup.get("action"),
                    "strategy_mode": setup.get("strategy_mode", "TREND_MOMENTUM"),
                    "trade_speed": setup.get("trade_speed", "FAST TRADE"),
                    "coin_trend": setup.get("coin_trend", "NEUTRAL"),
                    "thesis": setup.get("strategic_thesis", ""),
                    "confidence": 0.80
                })

        # Ingest Whale Order Flow Setups (FROZEN by default to eliminate counter-trend drag)
        if ENABLE_WHALE_SIGNALS and whale_decision.get("action") == "TRADE":
            for setup in whale_decision.get("whale_setups", []):
                prob = float(setup.get("movement_probability") or 75.0)
                if prob >= WHALE_MIN_PROBABILITY:
                    raw_candidates.append({
                        "source": "WHALE_FLOW",
                        "symbol": setup.get("symbol"),
                        "action": setup.get("action", "SHORT"),
                        "strategy_mode": "TREND_MOMENTUM",
                        "trade_speed": "FAST TRADE",
                        "coin_trend": "WHALE_FLOW",
                        "thesis": setup.get("orderflow_thesis", ""),
                        "confidence": prob / 100.0,
                        "whale_pattern": setup.get("whale_pattern", "DISTRIBUTION_TRAP")
                    })
        elif not ENABLE_WHALE_SIGNALS and whale_decision.get("action") == "TRADE":
            log_event("INFO", "PROFIT_ENGINE", "Whale setups frozen: Focusing exclusively on high-win-rate Scalp & Swing momentum.")

        # Ingest AI Scalp Sniper Setups
        if scalp_decision.get("action") == "TRADE":
            for setup in scalp_decision.get("scalp_setups", []):
                prob = float(setup.get("scalp_probability") or setup.get("movement_probability") or 80.0)
                if prob >= SCALP_MIN_PROBABILITY:  # Configurable scalp probability gate (default 70.0%)
                    raw_candidates.append({
                        "source": "AI_SCALP",
                        "symbol": setup.get("symbol"),
                        "action": setup.get("action"),
                        "strategy_mode": "SCALP",
                        "trade_speed": "SCALP TRADE",
                        "coin_trend": "SCALP_SNIPER",
                        "thesis": setup.get("scalp_thesis", ""),
                        "confidence": prob / 100.0,
                        "scalp_probability": prob,
                        "scalp_type": setup.get("scalp_type", "ORDERBOOK_IMBALANCE")
                    })

        # Filter candidates by scan_type when called specifically (e.g. /scalp or /whale)
        if scan_type == "SCALP":
            raw_candidates = [c for c in raw_candidates if c["source"] == "AI_SCALP"]
        elif scan_type == "WHALE":
            raw_candidates = [c for c in raw_candidates if c["source"] == "WHALE_FLOW"]
        elif scan_type == "MACRO":
            raw_candidates = [c for c in raw_candidates if c["source"] == "AI_MACRO"]

        # Calculate Expected Profit Index for every setup
        processed_candidates = []
        for cand in raw_candidates:
            sym = cand.get("symbol")
            act = cand.get("action")
            if not sym or act not in ["LONG", "SHORT"]:
                continue

            bridge = bridge_symbol_formats(sym)
            cand_info = next((m for m in market_universe if m["s"] == bridge["ccxt"] or m["s"] == sym), None)
            curr_price = float(cand_info["p"]) if cand_info else await asyncio.to_thread(fetch_real_perp_market_price, bridge["ccxt"])
            if curr_price <= 0:
                continue

            c1h = await asyncio.to_thread(fetch_candles, bridge["ccxt"], "1h", 30)
            c4h = await asyncio.to_thread(fetch_candles, bridge["ccxt"], "4h", 30)

            rsi_1h = calculate_rsi([c["close"] for c in c1h]) if c1h else 50.0
            rsi_4h = calculate_rsi([c["close"] for c in c4h]) if c4h else 50.0
            atr_1h = calculate_atr(c1h, 14) if c1h else (curr_price * 0.018)
            ema21_1h = calculate_ema([c["close"] for c in c1h], 21) if c1h else curr_price

            vol_m = cand_info["v_m"] if cand_info else 10.0
            funding = cand_info["fr"] if cand_info else 0.0
            chg = cand_info["c"] if cand_info else 0.0
            book_ratio = cand_info.get("book_ratio", 1.0) if cand_info else 1.0
            whale_ls = cand_info.get("whale_ls_ratio", 1.0) if cand_info else 1.0
            hi_24h = float(cand_info.get("hi") or curr_price) if cand_info else curr_price
            lo_24h = float(cand_info.get("lo") or curr_price) if cand_info else curr_price

            quant_score = calculate_objective_quant_score(act, rsi_1h, rsi_4h, btc_regime["regime"], vol_m, funding, chg, btc_24h_pct, book_ratio)
            # Hard Quantitative Safety Veto: Never allow AI confidence to override a failed quant safety check (< 0.50)
            if quant_score < 0.50:
                log_event("INFO", "QUANT_VETO", f"Rejected {act} on {sym}: Objective quant score {quant_score:.2f} failed safety gate (RSI 1H: {rsi_1h:.1f}, BTC 24h: {btc_24h_pct:+.2f}%, Book: {book_ratio}x).")
                continue

            if cand["source"] in ("WHALE_FLOW", "AI_SCALP"):
                quant_score = round((quant_score * 0.65) + (float(cand["confidence"]) * 0.35), 2)

            if quant_score < CONFIDENCE_THRESHOLD:
                continue

            is_scalp = (cand.get("trade_speed") == "SCALP TRADE" or cand.get("strategy_mode") == "SCALP")

            # Strict Trend, Orderbook & 24h Range Room Alignment Gate (Enforced on ALL trades):
            # Eliminates extreme overbought/oversold chasing and buying 24h highs / shorting 24h lows while allowing 3-4 clean setups
            if act == "LONG":
                near_24h_top = (chg >= 3.5 and hi_24h > 0 and curr_price >= hi_24h * 0.993)
                if curr_price < (ema21_1h * 0.992) or rsi_1h < 40.0 or rsi_1h > 65.0 or book_ratio < 0.82 or btc_24h_pct < -1.20 or near_24h_top:
                    log_event("INFO", "ALIGNMENT_GATE", f"Filtered LONG on {sym}: Failed alignment (Price ${curr_price:.4f} vs EMA21 ${ema21_1h:.4f}, 24h High ${hi_24h:.4f}, RSI {rsi_1h:.1f}, Book {book_ratio}x, BTC {btc_24h_pct:+.2f}%).")
                    continue
            elif act == "SHORT":
                near_24h_bottom = (chg <= -3.5 and lo_24h > 0 and curr_price <= lo_24h * 1.007)
                if curr_price > (ema21_1h * 1.008) or rsi_1h > 58.0 or rsi_1h < 35.0 or book_ratio > 1.22 or btc_24h_pct > 1.80 or near_24h_bottom:
                    log_event("INFO", "ALIGNMENT_GATE", f"Filtered SHORT on {sym}: Failed alignment (Price ${curr_price:.4f} vs EMA21 ${ema21_1h:.4f}, 24h Low ${lo_24h:.4f}, RSI {rsi_1h:.1f}, Book {book_ratio}x, BTC {btc_24h_pct:+.2f}%).")
                    continue

            levels = calculate_deterministic_trade_levels(curr_price, atr_1h, act, ema21_1h, cand["strategy_mode"], change_24h=chg, is_scalp=is_scalp)
            
            # Dynamic Position Sizing used to compute EXPECTED CASH PROFIT
            pos_size_calc = calculate_dynamic_position_size(quant_score, levels["risk_pct"], balance=account_balance, is_scalp=is_scalp)

            # Expected Profit Index formula: EV% = (Prob * Gain_TP1) - ((1 - Prob) * Risk)
            gain_tp1_pct = abs(levels["t1"] - levels["entry"]) / levels["entry"] * 100.0 if levels["entry"] > 0 else 0.0
            risk_pct = levels["risk_pct"]
            ev_pct = (quant_score * gain_tp1_pct) - ((1.0 - quant_score) * risk_pct)

            # SCALPING PRIORITIZE PROFIT:
            # Expected Cash Profit ($) = Pos_Size * (EV% / 100)
            expected_cash_profit = pos_size_calc * (ev_pct / 100.0)

            # Scalp Profit Velocity Prioritization:
            # Scalps execute and turn over in <45m, compounding capital faster than swing trades
            if is_scalp:
                profit_index = (expected_cash_profit * 1.50) + 10.0
            else:
                profit_index = expected_cash_profit

            # Confluence Bonuses: Confluence between Whale, Macro, & Scalp AI
            if any(other["symbol"] == sym and other["source"] != cand["source"] for other in raw_candidates):
                profit_index += 10.0

            # Order book ratio and whale alignment bonus
            if (act == "SHORT" and book_ratio < 0.70) or (act == "LONG" and book_ratio > 1.30):
                profit_index += 5.0
            if (act == "SHORT" and whale_ls > 1.8 and btc_24h_pct < -0.5):
                profit_index += 5.0

            processed_candidates.append({
                "cand": cand,
                "bridge": bridge,
                "levels": levels,
                "quant_score": quant_score,
                "profit_index": round(profit_index, 2),
                "expected_cash_profit": round(expected_cash_profit, 2),
                "pos_size_usd": pos_size_calc,
                "curr_price": curr_price,
                "ema21_1h": ema21_1h,
                "chg": chg,
                "rsi_1h": rsi_1h,
                "funding": funding,
                "book_ratio": book_ratio,
                "whale_ls": whale_ls,
                "atr_1h": atr_1h,
                "is_scalp": is_scalp
            })

        # 7. PRIORITIZE PROFIT: Sort all setups by Expected Profit Index descending
        processed_candidates.sort(key=lambda x: x["profit_index"], reverse=True)

        watched_cnt, _ = count_active_and_daily_trades()
        emitted_count = 0
        emitted_symbols = set()
        for item in processed_candidates:
            if watched_cnt + emitted_count >= MAX_CONCURRENT_POSITIONS:
                break

            cand = item["cand"]
            bridge = item["bridge"]
            act = cand["action"]
            sym = bridge["ccxt"]
            is_scalp = item.get("is_scalp", False)

            # Prevent Duplicate Orders: Never open a new setup on a symbol that is already active/watching
            active_symbols = [p["symbol"] for p in active_positions_payload] + [p["symbol"] for p in pending_watchlist_payload] + list(emitted_symbols)
            if sym in active_symbols:
                log_event("INFO", "PROFIT_ENGINE", f"Symbol {sym} already active on watchlist/position. Skipping duplicate emission.")
                continue

            # Enforce correlation / cluster limit (Scalps have separate higher concurrency)
            dir_count = count_active_positions_by_direction(act)
            max_allowed_dir = MAX_CORRELATED_SCALPS_PER_SIDE if is_scalp else MAX_CORRELATED_POSITIONS_PER_SIDE
            if dir_count >= max_allowed_dir:
                log_event("INFO", "PROFIT_ENGINE", f"Correlated {act} limit ({dir_count}/{max_allowed_dir}) reached. Skipping {sym}.")
                continue

            # Re-fetch fresh live price after AI evaluation so 20s LLM latency never causes stale fills
            fresh_price = await asyncio.to_thread(fetch_real_perp_market_price, sym)
            curr_price = fresh_price if fresh_price > 0 else item["curr_price"]
            levels = calculate_deterministic_trade_levels(
                curr_price, item["atr_1h"], act, item.get("ema21_1h", curr_price),
                cand["strategy_mode"], change_24h=item.get("chg", 0.0), is_scalp=is_scalp
            )
            quant_score = item["quant_score"]
            pos_size_usd = item.get("pos_size_usd") or calculate_dynamic_position_size(quant_score, levels["risk_pct"], balance=account_balance, is_scalp=is_scalp)

            # Execution Logic: Default MARKET entry so trade + Stop-Loss + TP1/TP2/TP3 brackets are placed together immediately
            is_market_entry = (SCALP_ENTRY_TYPE == "MARKET" if is_scalp else TRADE_ENTRY_TYPE == "MARKET")
            is_immediate_fill = is_market_entry or ((curr_price <= levels["entry"]) if act == "LONG" else (curr_price >= levels["entry"]))
            init_stage = 1 if is_immediate_fill else 0

            binance_order_id = ""
            binance_sl_id = ""
            binance_tp1_id = ""
            binance_tp2_id = ""
            binance_tp3_id = ""
            executed_qty = 0.0
            actual_fill_price = levels["entry"]
            target_leverage = SCALP_LEVERAGE if is_scalp else DEFAULT_LEVERAGE

            if is_immediate_fill:
                actual_fill_price = curr_price
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    # Fire immediate Market Entry on Binance
                    market_res = await asyncio.to_thread(place_binance_market_entry, sym, act, pos_size_usd, target_leverage)
                    binance_order_id = market_res.get("id", "")
                    executed_qty = float(market_res.get("filled", 0.0))
                    if executed_qty <= 0:
                        log_event("WARN", "PROFIT_ENGINE", f"Market entry failed on {sym} (0 qty filled). Skipping watchlist insertion.")
                        continue
                    if market_res.get("price", 0.0) > 0:
                        actual_fill_price = float(market_res["price"])
                else:
                    executed_qty = format_order_quantity(sym, pos_size_usd, actual_fill_price)

                # Recalculate SL/TP brackets relative to actual_fill_price so slippage never distorts risk/reward
                fill_risk_dist = actual_fill_price * (levels["risk_pct"] / 100.0)
                tp1_m = levels.get("tp1_mult", 1.0 if is_scalp else 1.2)
                tp2_m = levels.get("tp2_mult", 1.8 if is_scalp else 2.0)
                tp3_m = levels.get("tp3_mult", 2.8 if is_scalp else 3.0)
                if act == "LONG":
                    levels["sl"] = round(actual_fill_price - fill_risk_dist, 6)
                    levels["t1"] = round(actual_fill_price + (tp1_m * fill_risk_dist), 6)
                    levels["t2"] = round(actual_fill_price + (tp2_m * fill_risk_dist), 6)
                    levels["t3"] = round(actual_fill_price + (tp3_m * fill_risk_dist), 6)
                else:
                    levels["sl"] = round(actual_fill_price + fill_risk_dist, 6)
                    levels["t1"] = round(actual_fill_price - (tp1_m * fill_risk_dist), 6)
                    levels["t2"] = round(actual_fill_price - (tp2_m * fill_risk_dist), 6)
                    levels["t3"] = round(actual_fill_price - (tp3_m * fill_risk_dist), 6)

                # Immediately put up Hard Stop-Loss and TP1/TP2/TP3 orders on Binance while sending trade
                brackets = await asyncio.to_thread(
                    place_binance_tp_sl_brackets,
                    sym, act, executed_qty, levels["sl"], levels["t1"], levels["t2"], levels["t3"]
                )
                binance_sl_id = brackets["sl_id"]
                binance_tp1_id = brackets["tp1_id"]
                binance_tp2_id = brackets["tp2_id"]
                binance_tp3_id = brackets["tp3_id"]
            else:
                # Place Resting Limit Order on Binance AND immediately put up conditional SL & TP brackets
                executed_qty = format_order_quantity(sym, pos_size_usd, levels["entry"])
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    binance_order_id = await asyncio.to_thread(place_binance_entry_limit, sym, act, pos_size_usd, levels["entry"], target_leverage)
                    if not binance_order_id:
                        log_event("WARN", "PROFIT_ENGINE", f"Limit entry failed on {sym}. Skipping watchlist insertion.")
                        continue
                    # Check if limit order filled immediately on matching engine
                    pos_check = await asyncio.to_thread(fetch_live_binance_position, sym)
                    if pos_check.get("ok") and float(pos_check.get("contracts") or 0.0) > 0:
                        init_stage = 1
                        executed_qty = float(pos_check["contracts"])
                        if float(pos_check.get("entryPrice") or 0.0) > 0:
                            actual_fill_price = float(pos_check["entryPrice"])
                    brackets = await asyncio.to_thread(
                        place_binance_tp_sl_brackets,
                        sym, act, executed_qty, levels["sl"], levels["t1"], levels["t2"], levels["t3"]
                    )
                    binance_sl_id = brackets["sl_id"]
                    binance_tp1_id = brackets["tp1_id"]
                    binance_tp2_id = brackets["tp2_id"]
                    binance_tp3_id = brackets["tp3_id"]

            upsert_watched_signal(
                sym, act, actual_fill_price, levels["sl"],
                levels["t1"], levels["t2"], levels["t3"],
                item["atr_1h"], pos_size_usd, cand["trade_speed"], cand["coin_trend"], cand["thesis"],
                confidence=quant_score, stage_init=init_stage,
                binance_entry_order_id=binance_order_id,
                executed_qty=executed_qty,
                leverage=target_leverage,
                rsi_1h=float(item.get("rsi_1h") or 50.0),
                funding_rate=float(item.get("funding") or 0.0),
                book_ratio=float(item.get("book_ratio") or 1.0),
                whale_ls_ratio=float(item.get("whale_ls") or 1.0),
                btc_24h_pct=float(btc_24h_pct),
                profit_index=float(item.get("profit_index") or 0.0),
                binance_sl_order_id=binance_sl_id,
                binance_tp1_id=binance_tp1_id,
                binance_tp2_id=binance_tp2_id,
                binance_tp3_id=binance_tp3_id
            )
            emitted_count += 1
            emitted_symbols.add(sym)

            side_badge = "🟢 BUY / LONG" if act == "LONG" else "🔴 SELL / SHORT"
            if cand["source"] == "WHALE_FLOW":
                source_badge = "🐋 WHALE FLOW"
            elif cand["source"] == "AI_SCALP":
                source_badge = "⚡ SCALP SNIPER"
            else:
                source_badge = "🌐 MACRO FLOW"
            exec_badge = f"🤖 [AUTO-{TRADING_MODE}]" if TRADING_MODE in ("LIVE", "TESTNET") else "📋 [SIMULATION]"

            if is_scalp:
                tp_desc = (
                    f"• TP1 (50% Qty): <code>${levels['t1']:.6f}</code> (+1.0R) {'✅' if binance_tp1_id else ''}\n"
                    f"• TP2 (25% Qty): <code>${levels['t2']:.6f}</code> (+1.8R) {'✅' if binance_tp2_id else ''}\n"
                    f"• TP3 (25% Qty): <code>${levels['t3']:.6f}</code> (+2.8R) {'✅' if binance_tp3_id else ''}"
                )
            else:
                tp_desc = (
                    f"• TP1 (50% Qty): <code>${levels['t1']:.6f}</code> (+1.2R) {'✅' if binance_tp1_id else ''}\n"
                    f"• TP2 (25% Qty): <code>${levels['t2']:.6f}</code> (+2.0R) {'✅' if binance_tp2_id else ''}\n"
                    f"• TP3 (25% Qty): <code>${levels['t3']:.6f}</code> (+3.0R) {'✅' if binance_tp3_id else ''}"
                )

            prob_line = f" | Scalp Prob: <b>{cand.get('scalp_probability', 80):.0f}%</b>" if is_scalp else ""
            cash_ev_line = f" (${item.get('expected_cash_profit', 0):+.2f} Cash EV)"

            if is_immediate_fill:
                msg = (
                    f"⚡ <b>IMMEDIATE AGGRESSIVE ENTRY EXECUTED ({source_badge}) {exec_badge}</b>\n"
                    f"📊 <b>Profit Priority Index:</b> <b>+{item['profit_index']:.2f} EV</b>{cash_ev_line} | Score: <b>{quant_score*100:.0f}%</b>{prob_line}\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({side_badge})\n"
                    f"• <b>Filled Entry Mark:</b> <code>${actual_fill_price:.6f}</code> ({target_leverage}x Leverage)\n"
                    f"• <b>Position Size:</b> <b>${pos_size_usd:.0f} USD</b>\n"
                    f"• <b>Hard SL on Binance:</b> <code>${levels['sl']:.6f}</code> (<code>-{levels['risk_pct']}%</code>)\n"
                    f"• <b>Edge Thesis:</b> <i>{html.escape(cand['thesis'])}</i>\n\n"
                    f"🎯 <b>PROFIT TARGETS:</b>\n"
                    f"{tp_desc}\n\n"
                    f"🛡️ <i>Sentinel 8s radar is actively managing early cash milestones and micro-floors.</i>"
                )
            else:
                msg = (
                    f"🏆 <b>HIGH +EV SETUP DISCOVERED ({source_badge}) {exec_badge}</b>\n"
                    f"📊 <b>Profit Priority Index:</b> <b>+{item['profit_index']:.2f} EV</b>{cash_ev_line} | Score: <b>{quant_score*100:.0f}%</b>{prob_line}\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({side_badge})\n"
                    f"• <b>Allocated Capital:</b> <b>${pos_size_usd:.0f} USD</b> ({target_leverage}x Leverage)\n"
                    f"• <b>Metrics:</b> RSI 1H: <code>{item['rsi_1h']}</code> | FR: <code>{item['funding']:+.4f}%</code> | Depth: <code>{item['book_ratio']}x</code>\n"
                    f"• <b>Edge Thesis:</b> <i>{html.escape(cand['thesis'])}</i>\n\n"
                    f"📍 <b>BINANCE LIMIT BRACKETS:</b>\n"
                    f"1. <b>Entry Price:</b> <code>${levels['entry']:.6f}</code>\n"
                    f"2. <b>Stop-Loss:</b> <code>${levels['sl']:.6f}</code> (<code>-{levels['risk_pct']}%</code>)\n\n"
                    f"🎯 <b>PROFIT TARGETS:</b>\n"
                    f"{tp_desc}"
                )

            if chat_id and context:
                try:
                    await context.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML")
                except Exception:
                    pass
            await asyncio.sleep(0.5)

        if is_manual_scan and emitted_count == 0 and chat_id and context:
            stand_msg = (
                f"📊 <b>UNIFIED EVALUATION REPORT</b>\n\n"
                f"• <b>Account Balance:</b> <code>${account_balance:.2f} USD</code> ({capital_info.get('mode', TRADING_MODE)})\n"
                f"• <b>BTC Context:</b> <code>{btc_regime['regime']}</code> (ADX: {btc_regime['adx']}, 24h: {btc_24h_pct:+.2f}%)\n"
                f"• <b>Macro Verdict:</b> <i>{html.escape(macro_decision.get('macro_verdict', 'Stand aside'))}</i>\n"
                f"• <b>Whale Verdict:</b> <i>{html.escape(whale_decision.get('whale_verdict', 'No anomalies'))}</i>\n"
                f"• <b>Scalp Verdict:</b> <i>{html.escape(scalp_decision.get('scalp_verdict', 'No scalp triggers'))}</i>\n"
                f"• <b>Action:</b> ⚪ No setups passed the profit expectancy gate."
            )
            try:
                await context.bot.send_message(chat_id=int(chat_id), text=stand_msg, parse_mode="HTML")
            except Exception:
                pass

        total_duration = round(time.time() - t_start, 2)
        log_event("INFO", "PROFIT_ENGINE", f"Unified Hourly Scan completed in {total_duration}s. Emitted: {emitted_count}")

    except Exception as e_pipe:
        log_event("ERROR", "PROFIT_ENGINE", f"Discovery pipeline error: {e_pipe}")
    finally:
        with get_db_connection() as conn:
            conn.execute("UPDATE bot_state SET value = '0' WHERE key = 'eval_running'")

async def execute_hourly_discovery_pipeline(context: ContextTypes.DEFAULT_TYPE = None, is_manual_scan: bool = False):
    await execute_unified_hourly_discovery_pipeline(context, is_manual_scan=is_manual_scan, scan_type="ALL")

async def execute_whale_discovery_pipeline(context: ContextTypes.DEFAULT_TYPE = None):
    await execute_unified_hourly_discovery_pipeline(context, is_manual_scan=True, scan_type="WHALE")

async def execute_scalp_discovery_pipeline(context: ContextTypes.DEFAULT_TYPE = None):
    await execute_unified_hourly_discovery_pipeline(context, is_manual_scan=True, scan_type="SCALP")

def auth_guard(update: Update) -> bool:
    if not TELEGRAM_CHAT_ID:
        return True
    clean_target_id = str(TELEGRAM_CHAT_ID).strip()
    user_id = str(update.effective_user.id).strip() if update.effective_user else ""
    chat_id = str(update.effective_chat.id).strip() if update.effective_chat else ""

    if user_id == clean_target_id or chat_id == clean_target_id:
        return True

    print(f"[AUTH_GUARD] ⛔ Blocked command from unauthorized User ID: '{user_id}' (Chat ID: '{chat_id}'). Expected TELEGRAM_CHAT_ID: '{clean_target_id}'")
    return False

async def set_bot_menu_commands(application):
    commands = [
        BotCommand("scan", "Run regular CEX technical scan"),
        BotCommand("whale", "Scan Whale & Institutional Order Flow on Binance"),
        BotCommand("scalp", "Scan High-Speed Scalp Sniper Setups"),
        BotCommand("report", "View & download monthly performance CSV audit"),
        BotCommand("watchlist", "View active orders & positions with speed tag"),
        BotCommand("addwatch", "Add manual pair: /addwatch BTC LONG [entry] [sl] [tp1]"),
        BotCommand("unwatch", "Remove pair: /unwatch <coin>"),
        BotCommand("clearwatch", "Clear all items from watchlist"),
        BotCommand("enter", "Promote pending coin to active position"),
        BotCommand("start", "System status & resume"),
        BotCommand("stop", "Pause market scans"),
        BotCommand("ip", "Show current server Outbound IP for Binance whitelist"),
        BotCommand("logs", "View recent logs"),
    ]
    try:
        await application.bot.set_my_commands(commands)
    except Exception:
        pass

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update):
        user_id = update.effective_user.id if update.effective_user else "unknown"
        chat_id = update.effective_chat.id if update.effective_chat else "unknown"
        await update.message.reply_text(
            f"🔒 <b>Unauthorized Access</b>\n\n"
            f"• <b>Your Telegram User ID:</b> <code>{user_id}</code>\n"
            f"• <b>Your Chat ID:</b> <code>{chat_id}</code>\n\n"
            f"👉 Set <code>TELEGRAM_CHAT_ID={user_id}</code> in your Helipod Environment Variables to authorize your account.",
            parse_mode="HTML"
        )
        return

    with get_db_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('system_shutdown', '0')")
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('scanning_active', '1')")
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('eval_running', '0')")
    ip_addr = await asyncio.to_thread(get_outbound_ip_sync)
    cap = await asyncio.to_thread(fetch_current_account_capital)
    await update.message.reply_text(
        f"🟢 <b>System Active.</b> Hourly AI discovery and silent 8s Sentinel running.\n"
        f"• <b>Mode / Balance:</b> <code>{cap.get('mode')}</code> (<code>${cap.get('total', 0.0):.2f} USD</code>)\n"
        f"• <b>Server Outbound IP:</b> <code>{ip_addr}</code>",
        parse_mode="HTML"
    )

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    with get_db_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('scanning_active', '0')")
    await update.message.reply_text("🟡 <b>Scanning Paused.</b> Sentinel remains active to manage open positions.", parse_mode="HTML")

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    status_msg = await update.message.reply_text("🔍 <b>Evaluating Real-Time Microstructure...</b>", parse_mode="HTML")
    await execute_hourly_discovery_pipeline(context, is_manual_scan=True)
    try:
        await status_msg.delete()
    except Exception:
        pass

async def whale_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    status_msg = await update.message.reply_text("🐋 <b>Scanning Binance Derivatives Whale Footprints...</b>", parse_mode="HTML")
    await execute_whale_discovery_pipeline(context)
    try:
        await status_msg.delete()
    except Exception:
        pass

async def scalp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    status_msg = await update.message.reply_text("⚡ <b>Scanning High-Speed Scalp Sniper Opportunities...</b>", parse_mode="HTML")
    await execute_scalp_discovery_pipeline(context)
    try:
        await status_msg.delete()
    except Exception:
        pass

async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    
    rep = generate_monthly_performance_report()
    pnl_color = "🟢" if rep["net_pnl"] >= 0 else "🔴"
    
    summary_text = (
        f"📊 <b>MONTHLY PERFORMANCE AUDIT ({rep['month'].upper()})</b>\n"
        f"<i>Cycle Baseline: {rep['start_date']} (Resets monthly)</i>\n\n"
        f"💰 <b>ACCOUNT CAPITAL & TELEMETRY:</b>\n"
        f"• <b>Total Balance:</b> <code>${rep['capital_usd']:.2f} USD</code> ({rep['mode']})\n"
        f"• <b>Free Margin:</b> <code>${rep['free_margin_usd']:.2f} USD</code>\n"
        f"• <b>Market Regime:</b> <code>{rep['regime']}</code>\n\n"
        f"🔍 <b>DISCOVERY METRICS:</b>\n"
        f"• <b>Total AI Scans:</b> <code>{rep['total_scans']}</code> cycles\n"
        f"• <b>Signals Produced:</b> <code>{rep['total_signals']}</code> setups\n"
        f"• <b>Active Positions:</b> <code>{rep['active_positions']}</code> live | <b>Pending:</b> <code>{rep['pending_orders']}</code>\n\n"
        f"📈 <b>EXECUTION OUTCOMES:</b>\n"
        f"• <b>Closed Trades:</b> <code>{rep['total_closed']}</code> (Wins: <code>{rep['wins']}</code> | Losses: <code>{rep['losses']}</code>)\n"
        f"• <b>Win Rate:</b> <b>{rep['win_rate']}%</b>\n"
        f"• <b>Avg Win:</b> <code>+{rep['avg_win']:.2f}%</code> | <b>Avg Loss:</b> <code>{rep['avg_loss']:.2f}%</code>\n"
        f"• <b>Net Realized PnL:</b> {pnl_color} <b>{rep['net_pnl']:+.2f}%</b>\n\n"
        f"📥 <i>Attached CSV contains full debug variables (RSI, Funding, Depth Ratio, Whale L/S, Leverage, EV).</i>"
    )
    await update.message.reply_text(summary_text, parse_mode="HTML")

    try:
        csv_bytes, filename = export_monthly_report_csv()
        await update.message.reply_document(
            document=io.BytesIO(csv_bytes),
            filename=filename,
            caption=f"📄 Complete Trade Audit Ledger for {rep['month']}."
        )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Could not generate CSV file: {e}")

async def watchlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    watched = get_active_watched_signals()
    if not watched:
        await update.message.reply_text("📋 <b>Watchlist Empty.</b> No active pending orders or live positions.", parse_mode="HTML")
        return

    cards = []
    for item in watched:
        sym = item["symbol"]
        act = item["action"]
        entry = float(item["entry_limit"] or 0.0)
        current_sl = float(item["current_sl"] or 0.0)
        t1 = float(item["target_1"] or 0.0)
        t2 = float(item["target_2"] or 0.0)
        t3 = float(item["target_3"] or 0.0)
        stage = int(item["stage"] or 0)
        speed = item["trade_speed"] or "FAST TRADE"
        trend = item["coin_trend"] or "NEUTRAL"
        tag = item["movement_tag"] or ""

        if speed == "SCALP TRADE":
            speed_badge = "⚡ <b>[SCALP TRADE]</b>"
        elif speed == "FAST TRADE":
            speed_badge = "🚀 <b>[FAST TRADE]</b>"
        else:
            speed_badge = "⏳ <b>[SLOW TRADE]</b>"

        stage_desc = "⏳ Pending Limit Order" if stage == 0 else f"🟢 Active Position (Stage {stage})"

        if trend == "WHALE_FLOW":
            mode_badge = "🐋 WHALE"
        elif "SCALP" in trend:
            mode_badge = "🎯 SCALP SNIPER"
        else:
            mode_badge = "📈 REGULAR"

        card = (
            f"<b>{sym}</b> (<code>{act}</code>) — {speed_badge} | {mode_badge}\n"
            f"• <b>Status:</b> {stage_desc} {tag}\n"
            f"• <b>Entry:</b> <code>${entry:.6f}</code> | <b>SL:</b> <code>${current_sl:.6f}</code>\n"
            f"• <b>Targets:</b> TP1: <code>${t1:.6f}</code> | TP2: <code>${t2:.6f}</code> | TP3: <code>${t3:.6f}</code>"
        )
        cards.append(card)

    await update.message.reply_text("\n\n".join(cards), parse_mode="HTML")

async def addwatch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update) or not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "ℹ️ <b>Usage:</b> <code>/addwatch &lt;COIN&gt; &lt;LONG|SHORT&gt; [ENTRY] [SL] [TP1] [FAST|SLOW]</code>\n"
            "• Example: <code>/addwatch SOL LONG</code> (Auto-computes brackets as FAST TRADE)\n"
            "• Example: <code>/addwatch ETH SHORT 2500 2560 2420 SLOW</code>",
            parse_mode="HTML"
        )
        return

    raw_coin = context.args[0]
    direction = context.args[1].upper()
    if direction not in ["LONG", "SHORT"]:
        await update.message.reply_text("❌ Direction must be LONG or SHORT.", parse_mode="HTML")
        return

    bridge = bridge_symbol_formats(raw_coin)
    sym = bridge["ccxt"]

    curr_price = await asyncio.to_thread(fetch_real_perp_market_price, sym)
    if curr_price <= 0:
        await update.message.reply_text(f"❌ Could not fetch market price for <code>{sym}</code>.", parse_mode="HTML")
        return

    c1h = await asyncio.to_thread(fetch_candles, sym, "1h", 25)
    atr_1h = calculate_atr(c1h, 14) if c1h else (curr_price * 0.02)

    speed_choice = "FAST TRADE"
    if len(context.args) >= 6:
        if context.args[5].upper() == "SLOW":
            speed_choice = "SLOW TRADE"

    if len(context.args) >= 5:
        try:
            entry = float(context.args[2])
            sl = float(context.args[3])
            t1 = float(context.args[4])
            risk_dist = abs(entry - sl)
            t2 = round(entry + (1.8 * risk_dist), 6) if direction == "LONG" else round(entry - (1.8 * risk_dist), 6)
            t3 = round(entry + (2.8 * risk_dist), 6) if direction == "LONG" else round(entry - (2.8 * risk_dist), 6)
        except ValueError:
            await update.message.reply_text("❌ Invalid numeric prices provided.", parse_mode="HTML")
            return
    else:
        levels = calculate_deterministic_trade_levels(curr_price, atr_1h, direction, curr_price, "SWEEP_REVERSION")
        entry = levels["entry"]
        sl = levels["sl"]
        t1 = levels["t1"]
        t2 = levels["t2"]
        t3 = levels["t3"]

    cap_info = fetch_current_account_capital()
    pos_size_usd = calculate_dynamic_position_size(0.85, abs(entry - sl) / entry * 100.0 if entry > 0 else 1.2, balance=cap_info.get("total", 10000.0))
    exec_qty = format_order_quantity(sym, pos_size_usd, entry)
    entry_oid = ""
    sl_id = ""
    tp1_id = ""
    tp2_id = ""
    tp3_id = ""
    init_stage = 0

    if TRADING_MODE in ("LIVE", "TESTNET"):
        is_imm = ((curr_price <= entry) if direction == "LONG" else (curr_price >= entry)) or TRADE_ENTRY_TYPE == "MARKET"
        if is_imm:
            init_stage = 1
            m_res = await asyncio.to_thread(place_binance_market_entry, sym, direction, pos_size_usd, DEFAULT_LEVERAGE)
            entry_oid = m_res.get("id", "")
            if float(m_res.get("filled", 0.0)) > 0:
                exec_qty = float(m_res["filled"])
            if float(m_res.get("price", 0.0)) > 0:
                entry = float(m_res["price"])
        else:
            entry_oid = await asyncio.to_thread(place_binance_entry_limit, sym, direction, pos_size_usd, entry, DEFAULT_LEVERAGE)
        brackets = await asyncio.to_thread(place_binance_tp_sl_brackets, sym, direction, exec_qty, sl, t1, t2, t3)
        sl_id = brackets["sl_id"]
        tp1_id = brackets["tp1_id"]
        tp2_id = brackets["tp2_id"]
        tp3_id = brackets["tp3_id"]

    upsert_watched_signal(
        sym, direction, entry, sl, t1, t2, t3, atr_1h, pos_size_usd,
        speed_choice, "MANUAL", "Manual Telegram Watchlist Insertion", confidence=0.85,
        stage_init=init_stage, binance_entry_order_id=entry_oid, executed_qty=exec_qty,
        leverage=DEFAULT_LEVERAGE, binance_sl_order_id=sl_id,
        binance_tp1_id=tp1_id, binance_tp2_id=tp2_id, binance_tp3_id=tp3_id
    )

    speed_icon = "⚡" if speed_choice == "FAST TRADE" else "⏳"
    await update.message.reply_text(
        f"✅ <b>Added to Watchlist:</b> <code>{sym}</code> ({direction}) | {speed_icon} <b>[{speed_choice}]</b>\n\n"
        f"• <b>Entry:</b> ${entry:.6f} {'(Filled)' if init_stage == 1 else '(Limit)'}\n"
        f"• <b>Stop-Loss:</b> ${sl:.6f} {'✅' if sl_id else ''}\n"
        f"• <b>TP1:</b> ${t1:.6f} {'✅' if tp1_id else ''} | <b>TP2:</b> ${t2:.6f} {'✅' if tp2_id else ''} | <b>TP3:</b> ${t3:.6f} {'✅' if tp3_id else ''}\n"
        f"👉 Sentinel will defend this trade on every 8s tick.",
        parse_mode="HTML"
    )

async def enter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update) or not context.args: return
    raw_coin = context.args[0]
    bridge = bridge_symbol_formats(raw_coin)
    target_sym = bridge["ccxt"]

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM watched_signals WHERE (symbol = ? OR symbol LIKE ?) AND status = 'WATCHING'", (target_sym, f"%{bridge['base']}%"))
        row = c.fetchone()
        if not row:
            await update.message.reply_text(f"❌ <code>{raw_coin.upper()}</code> not on watchlist.", parse_mode="HTML")
            return

        curr_price = await asyncio.to_thread(fetch_real_perp_market_price, row["symbol"])
        c1h = await asyncio.to_thread(fetch_candles, row["symbol"], "1h", 25)
        atr_1h = calculate_atr(c1h, 14) if c1h else (curr_price * 0.018)
        levels = calculate_deterministic_trade_levels(curr_price, atr_1h, row["action"], curr_price, "TREND_MOMENTUM")

        sl_id = ""
        tp1_id = ""
        tp2_id = ""
        tp3_id = ""
        exec_qty = float(row["executed_qty"] or 0.0)
        pos_size = float(row["position_size_usd"] or 500.0)
        if TRADING_MODE in ("LIVE", "TESTNET"):
            old_entry_id = str(row["binance_entry_order_id"] or "")
            if old_entry_id:
                await asyncio.to_thread(cancel_binance_order, row["symbol"], old_entry_id)
            lev = int(row["leverage"] or DEFAULT_LEVERAGE)
            m_res = await asyncio.to_thread(place_binance_market_entry, row["symbol"], row["action"], pos_size, lev)
            if float(m_res.get("filled", 0.0)) > 0:
                exec_qty = float(m_res["filled"])
                curr_price = float(m_res.get("price") or curr_price)
            else:
                exec_qty = format_order_quantity(row["symbol"], pos_size, curr_price)
            levels = calculate_deterministic_trade_levels(curr_price, atr_1h, row["action"], curr_price, "TREND_MOMENTUM")
            brackets = await asyncio.to_thread(
                place_binance_tp_sl_brackets,
                row["symbol"], row["action"], exec_qty, levels["sl"], levels["t1"], levels["t2"], levels["t3"]
            )
            sl_id = brackets["sl_id"]
            tp1_id = brackets["tp1_id"]
            tp2_id = brackets["tp2_id"]
            tp3_id = brackets["tp3_id"]

        conn.execute("""
            UPDATE watched_signals 
            SET stage = 1, entry_limit = ?, initial_sl = ?, current_sl = ?, target_1 = ?, target_2 = ?, target_3 = ?, highest_favorable_price = ?, executed_qty = ?, binance_sl_order_id = ?, binance_tp1_id = ?, binance_tp2_id = ?, binance_tp3_id = ?, last_updated = CURRENT_TIMESTAMP 
            WHERE id = ?
        """, (curr_price, levels["sl"], levels["sl"], levels["t1"], levels["t2"], levels["t3"], curr_price, exec_qty, sl_id, tp1_id, tp2_id, tp3_id, row["id"]))

    sl_note = f" (Placed on Binance: {sl_id})" if sl_id else ""
    await update.message.reply_text(
        f"🟢 <b>ACTIVATED ON BOT — {row['symbol']}</b> (⚡ FAST TRADE)\n\n"
        f"• <b>Fill Mark:</b> ${curr_price:.6f}\n"
        f"• <b>Hard SL:</b> ${levels['sl']:.6f}{sl_note}\n"
        f"• <b>Set TPs:</b> TP1: ${levels['t1']:.6f} {'✅' if tp1_id else ''} | TP2: ${levels['t2']:.6f} {'✅' if tp2_id else ''} | TP3: ${levels['t3']:.6f} {'✅' if tp3_id else ''}",
        parse_mode="HTML"
    )

async def unwatch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update) or not context.args: return
    removed = remove_watched_signal_by_symbol(context.args[0])
    if removed > 0:
        await update.message.reply_text(f"🗑️ <b>Removed {context.args[0].upper()} from tracking.</b> Cancel orders on Binance if needed.", parse_mode="HTML")
    else:
        await update.message.reply_text("ℹ️ Coin not found on watchlist.", parse_mode="HTML")

async def clearwatch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    count = clear_all_watched_signals()
    await update.message.reply_text(f"🧹 <b>Cleared {count} item(s) from watchlist.</b> Slots are now completely open.", parse_mode="HTML")

async def logs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    log_rows = get_recent_system_logs(limit=10)
    lines = [f"<code>[{r['timestamp'].split(' ')[1]}]</code> <b>{r['component']}:</b> {html.escape(r['message'])}" for r in reversed(log_rows)]
    await update.message.reply_text("\n".join(lines) if lines else "No logs recorded.", parse_mode="HTML")

async def ip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    outbound_ip = "Unknown"
    async with aiohttp.ClientSession() as session:
        for url in ("https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=4.0)) as resp:
                    if resp.status == 200:
                        txt = (await resp.text()).strip()
                        if txt:
                            outbound_ip = txt
                            break
            except Exception:
                continue
    await update.message.reply_text(
        f"🌐 <b>Server Outbound IP:</b> <code>{outbound_ip}</code>\n\n"
        f"👉 Copy this IP into <b>Binance API Management → Restrict access to trusted IPs only</b> and ensure <b>Enable Futures</b> is checked.",
        parse_mode="HTML"
    )

async def scheduled_hourly_scan_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        await execute_hourly_discovery_pipeline(context, is_manual_scan=False)
    except Exception as e:
        log_event("ERROR", "JOB_QUEUE", f"Error in scheduled hourly scan: {e}")

def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).post_init(set_bot_menu_commands).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("whale", whale_cmd))
    app.add_handler(CommandHandler("scalp", scalp_cmd))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("watchlist", watchlist_cmd))
    app.add_handler(CommandHandler("addwatch", addwatch_cmd))
    app.add_handler(CommandHandler("unwatch", unwatch_cmd))
    app.add_handler(CommandHandler("clearwatch", clearwatch_cmd))
    app.add_handler(CommandHandler("enter", enter_cmd))
    app.add_handler(CommandHandler("logs", logs_cmd))
    app.add_handler(CommandHandler("ip", ip_cmd))

    if app.job_queue:
        app.job_queue.run_repeating(
            scheduled_hourly_scan_job,
            interval=SCAN_INTERVAL_SECONDS,
            first=20
        )

    log_event("INFO", "AI_SCANNER", f"🚀 [{BOT_NAME}] Hourly AI Scanner Active.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
