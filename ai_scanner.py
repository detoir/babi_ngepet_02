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
    TRADING_MODE,
    DEEPSEEK_API_KEY,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    ACCOUNT_BALANCE_SIMULATED,
    WHALE_MIN_PROBABILITY,
    is_weekend_regime,
    generate_monthly_performance_report,
    export_monthly_report_csv,
    get_exchange_instance,
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
    place_binance_entry_limit,
    place_binance_stop_loss,
    cancel_binance_order,
    emergency_binance_market_close,
)

exchange = get_exchange_instance()
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
        ticker = exchange.fetch_ticker(bridge["ccxt"])
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
        ohlcv = exchange.fetch_ohlcv(bridge["ccxt"], timeframe=timeframe, limit=limit)
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

def calculate_deterministic_trade_levels(price: float, atr_1h: float, direction: str, ema21_1h: float, strat_mode: str = "TREND_MOMENTUM"):
    is_weekend = is_weekend_regime()
    is_sweep = (strat_mode == "SWEEP_REVERSION" or is_weekend)

    # Dynamic risk distance (1.5% to 2.8% max)
    raw_risk = atr_1h * 1.5
    risk_dist = max(raw_risk, price * 0.015)
    risk_dist = min(risk_dist, price * 0.028)

    # Adaptive reward multipliers based on regime
    if is_sweep:
        # Range-bound / Weekend: Compressed targets for quick scalp turnover
        tp1_mult = 0.85
        tp2_mult = 1.5
        tp3_mult = 2.2
    else:
        # Trending: Asymmetric reward multipliers allowing runners to achieve full target
        tp1_mult = 1.2
        tp2_mult = 2.0
        tp3_mult = 3.2

    if not is_sweep:
        if direction == "SHORT":
            # For shorts: Pullback limit entry above current price to avoid shorting the exact low
            bounce_target = min(price * 1.0035, max(price * 1.0012, ema21_1h)) if ema21_1h > price else (price * 1.0018)
            entry = round(bounce_target, 6)
            sl = round(entry + risk_dist, 6)
            t1 = round(entry - (tp1_mult * risk_dist), 6)
            t2 = round(entry - (tp2_mult * risk_dist), 6)
            t3 = round(entry - (tp3_mult * risk_dist), 6)
        else:
            # For longs: Pullback limit entry below current price to avoid buying the exact peak
            pullback_target = max(price * 0.9965, min(price * 0.9988, ema21_1h)) if ema21_1h < price else (price * 0.9982)
            entry = round(pullback_target, 6)
            sl = round(entry - risk_dist, 6)
            t1 = round(entry + (tp1_mult * risk_dist), 6)
            t2 = round(entry + (tp2_mult * risk_dist), 6)
            t3 = round(entry + (tp3_mult * risk_dist), 6)
    else:  # SWEEP_REVERSION / WEEKEND
        if direction == "LONG":
            entry = round(price * 0.994, 6)
            sl = round(entry - risk_dist, 6)
            t1 = round(entry + (tp1_mult * risk_dist), 6)
            t2 = round(entry + (tp2_mult * risk_dist), 6)
            t3 = round(entry + (tp3_mult * risk_dist), 6)
        else:
            entry = round(price * 1.006, 6)
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
        "risk_dist": risk_dist,
        "risk_pct": round((risk_dist / entry) * 100.0, 2) if entry > 0 else 2.0
    }

def calculate_objective_quant_score(direction: str, rsi_1h: float, rsi_4h: float, btc_regime: str, vol_m: float, funding_rate: float, change_24h: float, btc_24h_pct: float, book_ratio: float = 1.0) -> float:
    if direction == "LONG" and btc_24h_pct < -0.8:
        return 0.10

    if direction == "SHORT" and btc_24h_pct > 2.0:
        return 0.10

    score = 0.50
    
    if direction == "LONG":
        if btc_24h_pct >= 0.5: score += 0.15
        if change_24h > (btc_24h_pct + 1.5): score += 0.15
        if 42.0 <= rsi_1h <= 62.0: score += 0.15
        if 45.0 <= rsi_4h <= 65.0: score += 0.15
        if funding_rate <= 0.008: score += 0.10
        if book_ratio >= 1.15: score += 0.10
        # Overbought and chase penalties
        if rsi_1h > 67.0: score -= 0.35
        if change_24h >= 10.0: score -= 0.35

    else:  # SHORT
        if btc_24h_pct <= -1.0: score += 0.25
        if change_24h < (btc_24h_pct - 1.0): score += 0.20
        if 36.0 <= rsi_1h <= 55.0: score += 0.15
        if 35.0 <= rsi_4h <= 58.0: score += 0.15
        if -0.025 <= funding_rate <= 0.012: score += 0.10
        if book_ratio <= 0.85: score += 0.10
        # Oversold exhaustion and dump chase penalties
        if rsi_1h < 33.0: score -= 0.35
        if change_24h <= -9.0: score -= 0.35

    if vol_m >= 35.0: score += 0.10
    elif vol_m >= 10.0: score += 0.05

    return round(max(0.10, min(0.95, score)), 2)

def calculate_dynamic_position_size(quant_score: float, risk_pct: float, balance=ACCOUNT_BALANCE_SIMULATED) -> float:
    risk_alloc = 0.0050 if quant_score < 0.78 else (0.0075 if quant_score < 0.85 else 0.0100)
    risk_dollars = balance * risk_alloc
    pos_size = (risk_dollars / (risk_pct / 100.0)) * 2.0 if risk_pct > 0 else 500.0
    return round(min(pos_size, balance * 0.40), 0)

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
        tickers = await asyncio.to_thread(exchange.fetch_tickers)
    except Exception as e:
        log_event("ERROR", "SCANNER_TICKERS", f"Exchange fetch_tickers failure: {e}")
        return []

    funding_dict = {}
    try:
        premium_data = await asyncio.to_thread(exchange.fapiPublicGetPremiumIndex)
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

def analyze_macro_universe_with_ai(market_universe: list, live_news: str, btc_regime: dict, btc_24h_pct: float, capital_info: dict = None, active_positions: list = None) -> dict:
    user_rules = get_active_strategy_rules()
    lessons = get_contextual_trade_lessons()
    top_candidates = market_universe[:35]

    adx_val = float(btc_regime.get("adx") or 18.0)
    regime_str = btc_regime.get("regime", "RANGING")

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0
    cap_mode = capital_info.get("mode", "SIMULATION") if capital_info else "SIMULATION"

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

    prompt = f"""
    You are the Chief Quantitative Strategist & Portfolio Risk Sentinel for {BOT_NAME}.
    PRIMARY OBJECTIVES:
    1. Maximize Positive Expected Value (+EV) by synthesizing Global Financial News, Central Bank Catalysts, and Order Flow Microstructure.
    2. Adapt strategy dynamically: Unleash TREND_MOMENTUM on institutional weekdays, enforce SWEEP_REVERSION / fast scalp harvesting during Chop/Weekends.
    3. Defend active positions: Detect sudden macro shock reversals and order book flips.

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
        return {"action": "HOLD", "selected_setups": [], "active_position_directives": [], "macro_verdict": f"AI Error: {e}"}

def analyze_whale_microstructure_with_ai(market_universe: list, btc_regime: dict, btc_24h_pct: float, capital_info: dict = None, active_positions: list = None) -> dict:
    top_candidates = market_universe[:35]

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0

    downside_bias = ""
    if btc_24h_pct <= -0.8:
        downside_bias = "[ALERT: BEARISH PRESSURE] Target LIQUIDATION FLUSH SHORTS where retail/whale longs are trapped."

    active_pos_section = "No active positions currently."
    if active_positions:
        active_pos_section = json.dumps(active_positions)

    prompt = f"""
    You are the Senior Institutional Order Flow Specialist & Risk Defender for {BOT_NAME}.
    PRIMARY OBJECTIVES:
    1. Exploit whale positioning WITHOUT falling for spoofed bid walls or crowded liquidation traps.
    2. Protect active profits: Identify sudden institutional distribution or whale flips against open positions.

    [PORTFOLIO CAPITAL ANCHOR]
    • Total Balance: ${cap_total:.2f} USD | Free Margin: ${cap_free:.2f} USD

    [BTC MACRO ANCHOR]
    Regime: {btc_regime.get('regime')} | ADX: {btc_regime.get('adx')} | BTC 24h Change: {btc_24h_pct:+.2f}%
    {downside_bias}

    [ACTIVE POSITIONS - WHALE REVERSAL AUDIT]
    {active_pos_section}

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
        return {"action": "HOLD", "whale_setups": [], "active_position_directives": [], "whale_verdict": f"AI Parsing Error: {e}"}

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

        # 1. Fetch Current Capital & Dynamic Balance
        capital_info = fetch_current_account_capital()
        account_balance = float(capital_info.get("total", 10000.0))
        log_event("INFO", "CAPITAL", f"Current Capital Checked: ${account_balance:.2f} USD ({capital_info.get('mode', 'SIMULATION')})")

        # 2. Fetch Active Positions for Sudden Reversal & Profit Management
        active_positions_payload = []
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM watched_signals WHERE status = 'WATCHING' AND stage >= 1")
            active_rows = c.fetchall()
            for r in active_rows:
                sym = r["symbol"]
                act = r["action"]
                entry = float(r["entry_limit"] or 0.0)
                sl = float(r["current_sl"] or 0.0)
                high_fav = float(r["highest_favorable_price"] or entry)
                curr_price = await asyncio.to_thread(fetch_real_perp_market_price, sym)
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
                        "executed_qty": float(r["executed_qty"] or 0.0)
                    })

        # 3. Market Data Ingestion
        btc_candles_task = asyncio.to_thread(fetch_candles, "BTC/USDT:USDT", "1h", 35)
        btc_ticker_task = asyncio.to_thread(exchange.fetch_ticker, "BTC/USDT:USDT")
        news_task = fetch_live_world_and_crypto_news()
        tickers_task = fetch_full_market_universe_tickers()

        btc_candles, btc_ticker, live_news, market_universe = await asyncio.gather(
            btc_candles_task, btc_ticker_task, news_task, tickers_task
        )

        btc_regime = calculate_adx_regime(btc_candles) if btc_candles else {"regime": "RANGING", "adx": 18.0}
        btc_24h_pct = float(btc_ticker.get("percentage") or 0.0)

        # 4. Concurrently Run AI Macro and AI Whale Discovery Prompts
        macro_task = asyncio.to_thread(
            analyze_macro_universe_with_ai,
            market_universe, live_news, btc_regime, btc_24h_pct, capital_info, active_positions_payload
        )
        whale_task = asyncio.to_thread(
            analyze_whale_microstructure_with_ai,
            market_universe, btc_regime, btc_24h_pct, capital_info, active_positions_payload
        )

        macro_decision, whale_decision = await asyncio.gather(macro_task, whale_task)

        # 5. Apply AI Active Position Sudden Reversal & Profit Management Directives
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

            if d_verdict == "HARVEST_NOW" and match_pos["pnl_pct"] > 0:
                # Emergency Harvest
                if match_pos["sl_order_id"]:
                    await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, match_pos["symbol"], match_pos["action"], match_pos["executed_qty"])

                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET status = 'COMPLETED', realized_pnl = ?, movement_tag = '🎯 AI_REVERSAL_HARVEST', last_updated = CURRENT_TIMESTAMP WHERE id = ?", (match_pos["pnl_pct"], match_pos["id"]))

                log_event("INFO", "AI_REVERSAL", f"Emergency profit harvested on {d_sym}: {match_pos['pnl_pct']:+.2f}%. Reason: {d_reason}")
                if chat_id and context:
                    msg = (
                        f"💰 <b>AI PROFIT HARVEST: REVERSAL DEFENSE</b>\n\n"
                        f"• <b>Contract:</b> <code>{match_pos['symbol']}</code> ({match_pos['action']})\n"
                        f"• <b>Secured Profit:</b> <code>{match_pos['pnl_pct']:+.2f}%</code>\n"
                        f"• <b>AI Diagnostic:</b> <i>{html.escape(d_reason)}</i>\n"
                        f"🤖 <b>Closed immediately on Binance to protect profit.</b>"
                    )
                    try:
                        await context.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML")
                    except Exception:
                        pass

            elif d_verdict == "TIGHTEN_SL":
                tighter_sl = round(match_pos["entry"] * 1.0015, 6) if match_pos["action"] == "LONG" else round(match_pos["entry"] * 0.9985, 6)
                if match_pos["sl_order_id"]:
                    await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                new_sl_id = ""
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    new_sl_id = await asyncio.to_thread(place_binance_stop_loss, match_pos["symbol"], match_pos["action"], match_pos["executed_qty"], tighter_sl)

                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET current_sl = ?, binance_sl_order_id = ?, movement_tag = '🛡️ AI_TIGHTENED_SL', last_updated = CURRENT_TIMESTAMP WHERE id = ?", (tighter_sl, new_sl_id, match_pos["id"]))

                log_event("INFO", "AI_REVERSAL", f"Stop-Loss tightened on {d_sym} to ${tighter_sl:.6f}. Reason: {d_reason}")

        # 6. Profit Prioritization & Candidate Ranking Engine
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

        # Ingest Whale Order Flow Setups
        if whale_decision.get("action") == "TRADE":
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

            quant_score = calculate_objective_quant_score(act, rsi_1h, rsi_4h, btc_regime["regime"], vol_m, funding, chg, btc_24h_pct, book_ratio)
            if cand["source"] == "WHALE_FLOW":
                quant_score = max(quant_score, cand["confidence"])

            if quant_score < CONFIDENCE_THRESHOLD:
                continue

            levels = calculate_deterministic_trade_levels(curr_price, atr_1h, act, ema21_1h, cand["strategy_mode"])
            
            # Expected Profit Index formula: EV = (Prob * Gain_TP1) - ((1 - Prob) * Risk)
            gain_tp1_pct = abs(levels["t1"] - levels["entry"]) / levels["entry"] * 100.0 if levels["entry"] > 0 else 0.0
            risk_pct = levels["risk_pct"]
            profit_index = (quant_score * gain_tp1_pct) - ((1.0 - quant_score) * risk_pct)

            # Profit Bonus: Confluence between Whale & Macro AI
            if any(other["symbol"] == sym and other["source"] != cand["source"] for other in raw_candidates):
                profit_index += 0.50

            # Profit Bonus: Order book ratio and whale alignment
            if (act == "SHORT" and book_ratio < 0.70) or (act == "LONG" and book_ratio > 1.30):
                profit_index += 0.25
            if (act == "SHORT" and whale_ls > 1.8 and btc_24h_pct < -0.5):
                profit_index += 0.30

            processed_candidates.append({
                "cand": cand,
                "bridge": bridge,
                "levels": levels,
                "quant_score": quant_score,
                "profit_index": round(profit_index, 3),
                "curr_price": curr_price,
                "rsi_1h": rsi_1h,
                "funding": funding,
                "book_ratio": book_ratio,
                "atr_1h": atr_1h
            })

        # 7. PRIORITIZE PROFIT: Sort all setups by Expected Profit Index descending
        processed_candidates.sort(key=lambda x: x["profit_index"], reverse=True)

        emitted_count = 0
        for item in processed_candidates:
            if watched_cnt + emitted_count >= MAX_CONCURRENT_POSITIONS:
                break

            cand = item["cand"]
            bridge = item["bridge"]
            act = cand["action"]
            sym = bridge["ccxt"]

            # Enforce correlation / cluster limit
            dir_count = count_active_positions_by_direction(act)
            if dir_count >= MAX_CORRELATED_POSITIONS_PER_SIDE:
                log_event("INFO", "PROFIT_ENGINE", f"Correlated {act} limit ({dir_count}/{MAX_CORRELATED_POSITIONS_PER_SIDE}) reached. Skipping {sym}.")
                continue

            levels = item["levels"]
            quant_score = item["quant_score"]
            curr_price = item["curr_price"]
            
            # Dynamic Capital Position Sizing based on real account balance
            pos_size_usd = calculate_dynamic_position_size(quant_score, levels["risk_pct"], balance=account_balance)

            is_immediate_fill = (curr_price <= levels["entry"]) if act == "LONG" else (curr_price >= levels["entry"])
            init_stage = 1 if is_immediate_fill else 0

            # Place Binance Limit Order if in Live or Testnet mode
            binance_order_id = ""
            if not is_immediate_fill and TRADING_MODE in ("LIVE", "TESTNET"):
                binance_order_id = place_binance_entry_limit(sym, act, pos_size_usd, levels["entry"])

            upsert_watched_signal(
                sym, act, levels["entry"], levels["sl"],
                levels["t1"], levels["t2"], levels["t3"],
                item["atr_1h"], pos_size_usd, cand["trade_speed"], cand["coin_trend"], cand["thesis"],
                confidence=quant_score, stage_init=init_stage,
                binance_entry_order_id=binance_order_id
            )
            emitted_count += 1

            if not is_immediate_fill:
                side_badge = "🟢 BUY / LONG" if act == "LONG" else "🔴 SELL / SHORT"
                source_badge = "🐋 WHALE FLOW" if cand["source"] == "WHALE_FLOW" else "⚡ AI QUANT"
                exec_badge = f"🤖 [AUTO-{TRADING_MODE}]" if TRADING_MODE in ("LIVE", "TESTNET") else "📋 [SIMULATION]"

                msg = (
                    f"🏆 <b>HIGH +EV SETUP DISCOVERED ({source_badge}) {exec_badge}</b>\n"
                    f"📊 <b>Profit Priority Index:</b> <b>+{item['profit_index']:.2f} EV</b> | Score: <b>{quant_score*100:.0f}%</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({side_badge})\n"
                    f"• <b>Allocated Capital:</b> <b>${pos_size_usd:.0f} USD</b> (from ${account_balance:.0f} Total Balance)\n"
                    f"• <b>Metrics:</b> RSI 1H: <code>{item['rsi_1h']}</code> | FR: <code>{item['funding']:+.4f}%</code> | Depth: <code>{item['book_ratio']}x</code>\n"
                    f"• <b>Edge Thesis:</b> <i>{html.escape(cand['thesis'])}</i>\n\n"
                    f"📍 <b>BINANCE LIMIT BRACKETS:</b>\n"
                    f"1. <b>Entry Price:</b> <code>${levels['entry']:.6f}</code>\n"
                    f"2. <b>Stop-Loss:</b> <code>${levels['sl']:.6f}</code> (<code>-{levels['risk_pct']}%</code>)\n\n"
                    f"🎯 <b>PROFIT TARGETS:</b>\n"
                    f"• TP1 (50% Qty): <code>${levels['t1']:.6f}</code> (+1.2R)\n"
                    f"• TP2 (30% Qty): <code>${levels['t2']:.6f}</code> (+2.0R)\n"
                    f"• TP3 (20% Qty): <code>${levels['t3']:.6f}</code> (+3.2R)"
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
                f"• <b>Account Balance:</b> <code>${account_balance:.2f} USD</code> ({capital_info.get('mode', 'SIMULATION')})\n"
                f"• <b>BTC Context:</b> <code>{btc_regime['regime']}</code> (ADX: {btc_regime['adx']}, 24h: {btc_24h_pct:+.2f}%)\n"
                f"• <b>Macro Verdict:</b> <i>{html.escape(macro_decision.get('macro_verdict', 'Stand aside'))}</i>\n"
                f"• <b>Whale Verdict:</b> <i>{html.escape(whale_decision.get('whale_verdict', 'No anomalies'))}</i>\n"
                f"• <b>Action:</b> ⚪ No setups passed the profit expectancy gate."
            )
            try:
                await context.bot.send_message(chat_id=int(chat_id), text=stand_msg, parse_mode="HTML")
            except Exception:
                pass

        total_duration = round(time.time() - t_start, 2)
        log_event("INFO", "PROFIT_ENGINE", f"Unified Hourly Scan completed in {total_duration}s. Emitted: {emitted_count}")

    finally:
        with get_db_connection() as conn:
            conn.execute("UPDATE bot_state SET value = '0' WHERE key = 'eval_running'")

async def execute_hourly_discovery_pipeline(context: ContextTypes.DEFAULT_TYPE = None, is_manual_scan: bool = False):
    await execute_unified_hourly_discovery_pipeline(context, is_manual_scan=is_manual_scan, scan_type="MACRO")

async def execute_whale_discovery_pipeline(context: ContextTypes.DEFAULT_TYPE = None):
    await execute_unified_hourly_discovery_pipeline(context, is_manual_scan=True, scan_type="WHALE")

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
        BotCommand("report", "View & download monthly performance CSV audit"),
        BotCommand("watchlist", "View active orders & positions with speed tag"),
        BotCommand("addwatch", "Add manual pair: /addwatch BTC LONG [entry] [sl] [tp1]"),
        BotCommand("unwatch", "Remove pair: /unwatch <coin>"),
        BotCommand("clearwatch", "Clear all items from watchlist"),
        BotCommand("enter", "Promote pending coin to active position"),
        BotCommand("start", "System status & resume"),
        BotCommand("stop", "Pause market scans"),
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
    await update.message.reply_text("🟢 <b>System Active.</b> Hourly AI discovery and silent 8s Sentinel running.", parse_mode="HTML")

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

async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth_guard(update): return
    
    rep = generate_monthly_performance_report()
    pnl_color = "🟢" if rep["net_pnl"] >= 0 else "🔴"
    
    summary_text = (
        f"📊 <b>MONTHLY PERFORMANCE AUDIT ({rep['month'].upper()})</b>\n"
        f"<i>Cycle Baseline: {rep['start_date']} (Resets monthly)</i>\n\n"
        f"🏛️ <b>Market Regime:</b> <code>{rep['regime']}</code>\n\n"
        f"🔍 <b>DISCOVERY METRICS:</b>\n"
        f"• <b>Total AI Scans:</b> <code>{rep['total_scans']}</code> cycles\n"
        f"• <b>Signals Produced:</b> <code>{rep['total_signals']}</code> setups\n"
        f"• <b>Active Positions:</b> <code>{rep['active_positions']}</code> live\n"
        f"• <b>Pending In-Zone:</b> <code>{rep['pending_orders']}</code> orders\n\n"
        f"📈 <b>EXECUTION OUTCOMES:</b>\n"
        f"• <b>Closed Trades:</b> <code>{rep['total_closed']}</code>\n"
        f"• <b>Wins:</b> <code>{rep['wins']}</code> | <b>Losses:</b> <code>{rep['losses']}</code>\n"
        f"• <b>Win Rate:</b> <b>{rep['win_rate']}%</b>\n"
        f"• <b>Net Realized PnL:</b> {pnl_color} <b>{rep['net_pnl']:+.2f}%</b>\n\n"
        f"📥 <i>Generating and attaching full CSV audit ledger below...</i>"
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

        speed_badge = "⚡ <b>[FAST TRADE]</b>" if speed == "FAST TRADE" else "⏳ <b>[SLOW TRADE]</b>"
        stage_desc = "⏳ Pending Limit Order" if stage == 0 else f"🟢 Active Position (Stage {stage})"
        mode_badge = "🐋 WHALE" if trend == "WHALE_FLOW" else "📈 REGULAR"

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

    upsert_watched_signal(
        sym, direction, entry, sl, t1, t2, t3, atr_1h, 500.0,
        speed_choice, "MANUAL", "Manual Telegram Watchlist Insertion", confidence=0.85
    )

    speed_icon = "⚡" if speed_choice == "FAST TRADE" else "⏳"
    await update.message.reply_text(
        f"✅ <b>Added to Watchlist:</b> <code>{sym}</code> ({direction}) | {speed_icon} <b>[{speed_choice}]</b>\n\n"
        f"• <b>Entry:</b> ${entry:.6f}\n"
        f"• <b>Stop-Loss:</b> ${sl:.6f}\n"
        f"• <b>TP1:</b> ${t1:.6f} | <b>TP2:</b> ${t2:.6f} | <b>TP3:</b> ${t3:.6f}\n"
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

        conn.execute("""
            UPDATE watched_signals 
            SET stage = 1, entry_limit = ?, initial_sl = ?, current_sl = ?, target_1 = ?, target_2 = ?, target_3 = ?, highest_favorable_price = ?, last_updated = CURRENT_TIMESTAMP 
            WHERE id = ?
        """, (curr_price, levels["sl"], levels["sl"], levels["t1"], levels["t2"], levels["t3"], curr_price, row["id"]))

    await update.message.reply_text(
        f"🟢 <b>ACTIVATED ON BOT — {row['symbol']}</b> (⚡ FAST TRADE)\n\n"
        f"• <b>Fill Mark:</b> ${curr_price:.6f}\n"
        f"• <b>Set Hard SL on Binance:</b> ${levels['sl']:.6f}\n"
        f"• <b>Set TPs:</b> TP1: ${levels['t1']:.6f} | TP2: ${levels['t2']:.6f} | TP3: ${levels['t3']:.6f}",
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
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("watchlist", watchlist_cmd))
    app.add_handler(CommandHandler("addwatch", addwatch_cmd))
    app.add_handler(CommandHandler("unwatch", unwatch_cmd))
    app.add_handler(CommandHandler("clearwatch", clearwatch_cmd))
    app.add_handler(CommandHandler("enter", enter_cmd))
    app.add_handler(CommandHandler("logs", logs_cmd))

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
