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
    safe_amount_to_precision,
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
        # Calibrated Scalp Sniper:
        # 0.90% min - 1.20% max ATR stop to survive micro noise while turning over fast
        raw_risk = atr_1h * 0.80
        risk_dist = max(raw_risk, price * 0.0090)
        risk_dist = min(risk_dist, price * 0.0120)

        # Positive-Expectancy Fast-Turnover Targets: 30% at +1.1R, 35% at +2.0R, 35% at +3.2R
        tp1_mult = 1.1
        tp2_mult = 2.0
        tp3_mult = 3.2

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
            "risk_pct": round((risk_dist / entry) * 100.0, 2) if entry > 0 else 0.95
        }

    # Dynamic risk distance for Fast/Swing trades: 1.10% - 1.50% to avoid premature noise stop-outs
    raw_risk = atr_1h * 1.00
    risk_dist = max(raw_risk, price * 0.0110)
    risk_dist = min(risk_dist, price * 0.0150)

    # Adaptive reward multipliers: Bank 30% at TP1, let 70% run to TP2 & TP3
    if is_sweep:
        tp1_mult = 1.2
        tp2_mult = 2.0
        tp3_mult = 3.0
    else:
        tp1_mult = 1.3
        tp2_mult = 2.2
        tp3_mult = 3.5

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

TRADFI_PERP_BASES = {
    "NVDA", "TSLA", "AAPL", "MSFT", "META", "AMZN", "GOOGL", "AMD", "INTC", "MU",
    "SNDK", "SPCX", "SKHY", "SOXL", "COIN", "MSTR", "PLTR", "HOOD", "SPY", "QQQ",
    "XAU", "XAG", "CL", "WTI", "BRENT", "NG", "EUR", "GBP", "JPY"
}

def is_tradfi_symbol(symbol: str) -> bool:
    base = bridge_symbol_formats(symbol).get("base", "").upper()
    return base in TRADFI_PERP_BASES

def calculate_objective_quant_score(direction: str, rsi_1h: float, rsi_4h: float, btc_regime: str, vol_m: float, funding_rate: float, change_24h: float, btc_24h_pct: float, book_ratio: float = 1.0, is_tradfi: bool = False) -> float:
    # Hard Disqualification Gates: Never buy overbought tops / falling knives or short oversold capitulation bottoms
    if direction == "LONG":
        if not is_tradfi and btc_24h_pct < -1.5:
            return 0.10
        if rsi_1h > 65.0 or rsi_1h < 42.0:
            return 0.10
        if change_24h >= 9.0 or funding_rate >= 0.030 or book_ratio < 0.90:
            return 0.10
    else:  # SHORT
        if not is_tradfi and btc_24h_pct > 2.2:
            return 0.10
        if rsi_1h < 41.0 or rsi_1h > 59.0:  # Hard Gate: Never short into oversold bottoms (<41.0)
            return 0.10
        if change_24h <= -8.0 or funding_rate <= -0.035 or book_ratio > 1.15:
            return 0.10

    score = 0.62
    
    if direction == "LONG":
        if is_tradfi or btc_24h_pct >= 0.2: score += 0.08
        if (0.2 <= change_24h <= 6.0) if is_tradfi else (0.2 <= (change_24h - btc_24h_pct) <= 6.0): score += 0.10
        if 45.0 <= rsi_1h <= 61.0: score += 0.10
        if 44.0 <= rsi_4h <= 64.0: score += 0.08
        if funding_rate <= 0.012: score += 0.06
        if book_ratio >= 1.15: score += 0.08
    else:  # SHORT
        if is_tradfi or btc_24h_pct <= -0.3: score += 0.10
        if (-6.5 <= change_24h <= -0.3) if is_tradfi else (-6.5 <= (change_24h - btc_24h_pct) <= -0.3): score += 0.10
        if 42.0 <= rsi_1h <= 55.0: score += 0.12  # Prime swing rollover zone
        if 42.0 <= rsi_4h <= 58.0: score += 0.08
        if -0.025 <= funding_rate <= 0.015: score += 0.06
        if book_ratio <= 0.85: score += 0.08

    if vol_m >= 35.0: score += 0.06
    elif vol_m >= 10.0: score += 0.04

    # Clamp positive bonuses BEFORE applying any edge exhaustion penalties
    score = min(0.92, score)

    if direction == "LONG":
        if rsi_1h > 63.0: score -= 0.20
        if change_24h >= 6.5: score -= 0.20
    else:
        if rsi_1h < 42.0: score -= 0.25  # Heavy penalty for selling into late downward momentum
        if change_24h <= -6.5: score -= 0.20

    return round(max(0.10, min(0.95, score)), 2)

def calculate_dynamic_position_size(quant_score: float, risk_pct: float, balance=None, is_scalp: bool = False) -> float:
    bal = float(balance) if balance is not None else float(ACCOUNT_BALANCE_SIMULATED)
    lev = SCALP_LEVERAGE if is_scalp else DEFAULT_LEVERAGE

    # Halved Margin Allocation (10% per slot standard; 5% when daily profit lock >= +6.0% is active)
    rolling_24h_pnl = check_rolling_24h_drawdown()
    slot_margin_fraction = 0.05 if rolling_24h_pnl >= 6.0 else 0.10

    max_slot_notional = bal * slot_margin_fraction * lev
    risk_alloc = (0.0025 if quant_score < 0.78 else (0.0038 if quant_score < 0.85 else 0.0050)) * POSITION_SIZE_MULTIPLIER
    risk_dollars = bal * risk_alloc
    pos_size = (risk_dollars / (risk_pct / 100.0)) * 2.0 if risk_pct > 0 else max_slot_notional
    sized = round(min(pos_size, max_slot_notional), 0)
    return max(6.0, sized) if bal > 0 else sized

def compute_realtime_candle_telemetry(klines_15m: list, klines_5m: list, book_ratio: float = 1.0) -> dict:
    """Deterministic real-time price, candle momentum, VWAP, EMA, CVD, and failed breakout/breakdown analysis."""
    default_out = {
        "vwap": 0.0, "ema9": 0.0, "ema20": 0.0, "ema50": 0.0,
        "chg_5m_pct": 0.0, "chg_15m_pct": 0.0, "chg_1h_pct": 0.0,
        "rel_vol": 1.0, "body_pct": 50.0, "u_wick_pct": 25.0, "l_wick_pct": 25.0,
        "swing_hi": 0.0, "swing_lo": 0.0, "cvd_bias": "NEUTRAL",
        "completed_candle_sig": "NEUTRAL", "live_candle_sig": "INCOMPLETE_NEUTRAL",
        "failed_breakdown": False, "failed_breakout": False,
        "fast_bull_rev": False, "fast_bear_rev": False
    }
    if not klines_15m or len(klines_15m) < 10:
        return default_out

    try:
        parsed_15m = []
        for k in klines_15m:
            o, h, l, c, v = float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
            taker_buy = float(k[9]) if len(k) > 9 else (v * 0.5)
            taker_sell = max(0.0, v - taker_buy)
            parsed_15m.append({"o": o, "h": h, "l": l, "c": c, "v": v, "delta": taker_buy - taker_sell})

        completed = parsed_15m[:-1]
        live = parsed_15m[-1]
        c_last = completed[-1]
        curr_p = live["c"]

        closes = [x["c"] for x in completed]
        ema9 = round(calculate_ema(closes, 9), 6)
        ema20 = round(calculate_ema(closes, 20), 6)
        ema50 = round(calculate_ema(closes, min(len(closes), 50)), 6)

        tot_vol = sum(x["v"] for x in completed[-20:])
        vwap = round(sum(((x["h"] + x["l"] + x["c"]) / 3.0) * x["v"] for x in completed[-20:]) / tot_vol, 6) if tot_vol > 0 else curr_p

        vol_avg = (tot_vol / min(len(completed), 20)) if tot_vol > 0 else 1.0
        rel_vol = round(c_last["v"] / vol_avg, 2) if vol_avg > 0 else 1.0

        prior_window = completed[-15:-1] if len(completed) > 3 else completed
        swing_hi = round(max(x["h"] for x in prior_window), 6)
        swing_lo = round(min(x["l"] for x in prior_window), 6)

        chg_15m_pct = round(((c_last["c"] - c_last["o"]) / c_last["o"]) * 100.0, 2) if c_last["o"] > 0 else 0.0
        live_15m_pct = round(((curr_p - live["o"]) / live["o"]) * 100.0, 2) if live["o"] > 0 else 0.0
        ref_1h = completed[-4]["c"] if len(completed) >= 4 else completed[0]["o"]
        chg_1h_pct = round(((curr_p - ref_1h) / ref_1h) * 100.0, 2) if ref_1h > 0 else 0.0

        chg_5m_pct = live_15m_pct
        if klines_5m and len(klines_5m) >= 2:
            c5_prev = float(klines_5m[-2][1])
            c5_curr = float(klines_5m[-1][4])
            if c5_prev > 0:
                chg_5m_pct = round(((c5_curr - c5_prev) / c5_prev) * 100.0, 2)

        rng = max(c_last["h"] - c_last["l"], 1e-9)
        body_pct = round((abs(c_last["c"] - c_last["o"]) / rng) * 100.0, 1)
        u_wick_pct = round(((c_last["h"] - max(c_last["o"], c_last["c"])) / rng) * 100.0, 1)
        l_wick_pct = round(((min(c_last["o"], c_last["c"]) - c_last["l"]) / rng) * 100.0, 1)

        recent_delta = sum(x["delta"] for x in completed[-3:]) + live["delta"]
        cvd_bias = "POSITIVE" if recent_delta > 0 else ("NEGATIVE" if recent_delta < 0 else "NEUTRAL")

        # Failed Breakdown & Failed Breakout Detection
        failed_breakdown = bool(
            (c_last["l"] < swing_lo or live["l"] < swing_lo)
            and c_last["c"] >= swing_lo
            and curr_p >= swing_lo
            and (c_last["c"] > c_last["o"] or l_wick_pct >= 40.0)
        )
        failed_breakout = bool(
            (c_last["h"] > swing_hi or live["h"] > swing_hi)
            and c_last["c"] <= swing_hi
            and curr_p <= swing_hi
            and (c_last["c"] < c_last["o"] or u_wick_pct >= 40.0)
        )

        # Fast Reversal Detection (Multi-condition confluence)
        bull_conds = sum([
            curr_p > c_last["c"] and c_last["c"] > c_last["o"],
            len(completed) >= 3 and completed[-1]["l"] > completed[-2]["l"],
            body_pct >= 55.0 and c_last["c"] > c_last["o"],
            rel_vol >= 1.15 and c_last["c"] > c_last["o"],
            curr_p > vwap and c_last["o"] <= vwap,
            curr_p > ema9 and curr_p > ema20,
            failed_breakdown,
            book_ratio >= 1.20,
            cvd_bias == "POSITIVE"
        ])
        bear_conds = sum([
            curr_p < c_last["c"] and c_last["c"] < c_last["o"],
            len(completed) >= 3 and completed[-1]["h"] < completed[-2]["h"],
            body_pct >= 55.0 and c_last["c"] < c_last["o"],
            rel_vol >= 1.15 and c_last["c"] < c_last["o"],
            curr_p < vwap and c_last["o"] >= vwap,
            curr_p < ema9 and curr_p < ema20,
            failed_breakout,
            book_ratio <= 0.82,
            cvd_bias == "NEGATIVE"
        ])
        fast_bull_rev = bull_conds >= 5
        fast_bear_rev = bear_conds >= 5

        if failed_breakdown:
            comp_sig = "FAILED_BREAKDOWN"
        elif failed_breakout:
            comp_sig = "FAILED_BREAKOUT"
        elif l_wick_pct >= 45.0 and c_last["c"] >= c_last["o"]:
            comp_sig = "BULLISH_REJECTION"
        elif u_wick_pct >= 45.0 and c_last["c"] <= c_last["o"]:
            comp_sig = "BEARISH_REJECTION"
        elif c_last["c"] > c_last["o"] and body_pct >= 60.0 and rel_vol >= 1.25:
            comp_sig = "BULLISH_ACCELERATION"
        elif c_last["c"] < c_last["o"] and body_pct >= 60.0 and rel_vol >= 1.25:
            comp_sig = "BEARISH_ACCELERATION"
        elif fast_bull_rev:
            comp_sig = "POSSIBLE_BULLISH_REVERSAL"
        elif fast_bear_rev:
            comp_sig = "POSSIBLE_BEARISH_REVERSAL"
        elif c_last["c"] > c_last["o"] and curr_p > ema9:
            comp_sig = "BULLISH_CONTINUATION"
        elif c_last["c"] < c_last["o"] and curr_p < ema9:
            comp_sig = "BEARISH_CONTINUATION"
        elif body_pct <= 25.0 and rel_vol < 0.85:
            comp_sig = "COMPRESSION"
        else:
            comp_sig = "EXPANSION" if rel_vol >= 1.35 else "UNCERTAINTY"

        # Current forming candle caution (distinguish live incomplete vs completed)
        if live_15m_pct >= 0.45:
            live_sig = "LIVE_BULLISH_EXPANSION"
        elif live_15m_pct <= -0.45:
            live_sig = "LIVE_BEARISH_EXPANSION"
        elif live_15m_pct > 0.08:
            live_sig = "FORMING_BULLISH"
        elif live_15m_pct < -0.08:
            live_sig = "FORMING_BEARISH"
        else:
            live_sig = "INCOMPLETE_NEUTRAL"

        return {
            "vwap": vwap, "ema9": ema9, "ema20": ema20, "ema50": ema50,
            "chg_5m_pct": chg_5m_pct, "chg_15m_pct": chg_15m_pct, "chg_1h_pct": chg_1h_pct,
            "rel_vol": rel_vol, "body_pct": body_pct, "u_wick_pct": u_wick_pct, "l_wick_pct": l_wick_pct,
            "swing_hi": swing_hi, "swing_lo": swing_lo, "cvd_bias": cvd_bias,
            "completed_candle_sig": comp_sig, "live_candle_sig": live_sig,
            "failed_breakdown": failed_breakdown, "failed_breakout": failed_breakout,
            "fast_bull_rev": fast_bull_rev, "fast_bear_rev": fast_bear_rev
        }
    except Exception:
        return default_out

async def _fetch_json_safe(session: aiohttp.ClientSession, url: str, default=None):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=3.0)) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception:
        pass
    return default

async def fetch_microstructure_data_async(session: aiohttp.ClientSession, symbol_clean: str) -> dict:
    oi_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol_clean}"
    depth_url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol_clean}&limit=20"
    ls_ratio_url = f"https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={symbol_clean}&period=5m&limit=1"
    k15m_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol_clean}&interval=15m&limit=25"
    k5m_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol_clean}&interval=5m&limit=6"
    k1h_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol_clean}&interval=1h&limit=30"
    k4h_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol_clean}&interval=4h&limit=25"

    oi_data, depth_data, ls_data, klines_15m, klines_5m, klines_1h, klines_4h = await asyncio.gather(
        _fetch_json_safe(session, oi_url, {}),
        _fetch_json_safe(session, depth_url, {}),
        _fetch_json_safe(session, ls_ratio_url, []),
        _fetch_json_safe(session, k15m_url, []),
        _fetch_json_safe(session, k5m_url, []),
        _fetch_json_safe(session, k1h_url, []),
        _fetch_json_safe(session, k4h_url, []),
    )

    oi_val = float(oi_data.get("openInterest") or 0.0) if isinstance(oi_data, dict) else 0.0
    book_ratio = 1.0
    if isinstance(depth_data, dict):
        bids = sum(float(b[1]) for b in depth_data.get("bids", []))
        asks = sum(float(a[1]) for a in depth_data.get("asks", []))
        if asks > 0:
            book_ratio = round(bids / asks, 2)

    whale_ls_ratio = 1.0
    if isinstance(ls_data, list) and ls_data:
        whale_ls_ratio = round(float(ls_data[0].get("longShortRatio") or 1.0), 2)

    rsi_1h = 50.0
    ema21_1h = 0.0
    atr_1h_pct = 0.0
    if isinstance(klines_1h, list) and len(klines_1h) >= 22:
        completed_1h = [
            {"open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]), "vol": float(k[5])}
            for k in klines_1h[:-1]
        ]
        closes_1h = [c["close"] for c in completed_1h]
        rsi_1h = calculate_rsi(closes_1h)
        ema21_1h = round(calculate_ema(closes_1h, 21), 6)
        atr_1h_val = calculate_atr(completed_1h, 14)
        if closes_1h[-1] > 0 and atr_1h_val > 0:
            atr_1h_pct = round((atr_1h_val / closes_1h[-1]) * 100.0, 2)

    rsi_4h = 50.0
    chg_4h_pct = 0.0
    if isinstance(klines_4h, list) and len(klines_4h) >= 16:
        closes_4h = [float(k[4]) for k in klines_4h[:-1]]
        rsi_4h = calculate_rsi(closes_4h)
        if len(closes_4h) >= 2 and closes_4h[-2] > 0:
            chg_4h_pct = round(((closes_4h[-1] - closes_4h[-2]) / closes_4h[-2]) * 100.0, 2)

    candle_tel = compute_realtime_candle_telemetry(klines_15m or [], klines_5m or [], book_ratio=book_ratio)
    out = {
        "oi": oi_val,
        "book_ratio": book_ratio,
        "whale_ls_ratio": whale_ls_ratio,
        "rsi_1h": rsi_1h,
        "rsi_4h": rsi_4h,
        "ema21_1h": ema21_1h,
        "atr_1h_pct": atr_1h_pct,
        "chg_4h_pct": chg_4h_pct,
    }
    out.update(candle_tel)
    return out

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
    mark_dict = {}
    try:
        premium_data = await asyncio.to_thread(public_exchange.fapiPublicGetPremiumIndex)
        for item in premium_data:
            s_raw = item.get("symbol")
            fr_raw = item.get("lastFundingRate")
            mp_raw = item.get("markPrice")
            if s_raw:
                if fr_raw is not None:
                    funding_dict[s_raw] = float(fr_raw) * 100.0
                if mp_raw is not None:
                    mark_dict[s_raw] = float(mp_raw)
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
        bid_px = float(data.get("bid") or last_price)
        ask_px = float(data.get("ask") or last_price)
        prev_close = float(data.get("previousClose") or data.get("open") or last_price)
        spread_pct = round(abs(ask_px - bid_px) / last_price * 100.0, 4) if last_price > 0 else 0.0

        if last_price <= 0 or quote_vol < 1_000_000:
            continue

        clean_pair = symbol.split(":")[0].replace("/", "")
        funding_rate = funding_dict.get(clean_pair, 0.0)
        mark_price = mark_dict.get(clean_pair, last_price)

        market_universe.append({
            "s": symbol,
            "p": last_price,
            "mark_price": round(mark_price, 6),
            "bid": round(bid_px, 6),
            "ask": round(ask_px, 6),
            "spread_pct": spread_pct,
            "prev_close": round(prev_close, 6),
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
        cand["rsi_1h"] = res.get("rsi_1h", 50.0)
        cand["rsi_4h"] = res.get("rsi_4h", 50.0)
        ema21_1h = float(res.get("ema21_1h") or 0.0)
        cand["ema21_1h"] = ema21_1h
        cand["ema21_dist_pct"] = round(((cand["p"] - ema21_1h) / ema21_1h) * 100.0, 2) if ema21_1h > 0 else 0.0
        cand["atr_1h_pct"] = res.get("atr_1h_pct", 0.0)
        cand["chg_4h_pct"] = res.get("chg_4h_pct", 0.0)
        cand["asset_class"] = "TRADFI" if is_tradfi_symbol(cand["s"]) else "CRYPTO"
        for k_tel in (
            "vwap", "ema9", "ema20", "ema50", "chg_5m_pct", "chg_15m_pct", "chg_1h_pct",
            "rel_vol", "body_pct", "u_wick_pct", "l_wick_pct", "swing_hi", "swing_lo",
            "cvd_bias", "completed_candle_sig", "live_candle_sig",
            "failed_breakdown", "failed_breakout", "fast_bull_rev", "fast_bear_rev"
        ):
            if k_tel in res:
                cand[k_tel] = res[k_tel]

    return market_universe

ANTI_FAKE_SYSTEM_PROMPT = (
    "You are a strict quantitative trading analyst. Base EVERY statement solely on exact numeric values and flags "
    "present in the user's JSON payload. NEVER invent or assume levels, candles, volume, or indicators not provided. "
    "NEVER rationalize a contradicting metric (for example, never write 'despite negative CVD' for a LONG or "
    "'despite bullish book' for a SHORT). If a required condition fails or signals conflict, DISQUALIFY the symbol. "
    "Returning action='HOLD' with an empty setup list [] is always better than forcing a low-quality trade."
)

def analyze_macro_universe_with_ai(market_universe: list, live_news: str, btc_regime: dict, btc_24h_pct: float, capital_info: dict = None, active_positions: list = None, pending_watchlist: list = None, previous_regime: str = "UNCERTAIN") -> dict:
    user_rules = get_active_strategy_rules()
    lessons = get_contextual_trade_lessons()

    swing_fields = (
        "s", "asset_class", "p", "mark_price", "spread_pct", "c",
        "chg_15m_pct", "chg_1h_pct", "chg_4h_pct", "rsi_1h", "rsi_4h",
        "ema21_1h", "ema21_dist_pct", "atr_1h_pct", "v_m", "fr", "oi_m",
        "book_ratio", "whale_ls_ratio", "cvd_bias", "completed_candle_sig",
        "live_candle_sig", "failed_breakdown", "failed_breakout", "hi", "lo"
    )
    top_candidates = [
        {k: m[k] for k in swing_fields if k in m}
        for m in market_universe[:25]
    ]

    adx_val = float(btc_regime.get("adx") or 18.0)
    regime_str = btc_regime.get("regime", "RANGING")

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0
    cap_mode = capital_info.get("mode", TRADING_MODE) if capital_info else TRADING_MODE

    weekend_clause = ""
    if is_weekend_regime():
        weekend_clause = """
        [WEEKEND REGIME ACTIVE: CME CLOSED & LOW INSTITUTIONAL VOLUME]
        - Strictly ban TREND_MOMENTUM breakouts unless backed by strong 1H/4H alignment and positive CVD.
        - Prioritize SWEEP_REVERSION only at clear range extremes, or HOLD.
        """

    macro_alert = ""
    if btc_24h_pct <= -0.8:
        macro_alert = f"""
        [BEARISH 24H MACRO CONTEXT: BTC 24H ({btc_24h_pct:+.2f}%)]
        - Higher-timeframe BTC 24h bias is bearish, BUT check 1H/15m price action and `failed_breakdown` before shorting crypto pairs.
        - If immediate candle momentum has flipped bullish (`failed_breakdown`=true), DO NOT force blind shorts.
        """

    sideways_clause = ""
    if regime_str == "RANGING" or adx_val < 22.0:
        sideways_clause = """
        [SIDEWAYS / RANGE MARKET REGIME ACTIVE]
        - Market is range-bound (ADX < 22).
        - Avoid chasing breakouts; either select clean SWEEP_REVERSION swing setups near range extremes or return HOLD.
        """

    active_pos_section = "No active positions currently."
    if active_positions:
        active_pos_section = json.dumps(active_positions)

    pending_watch_section = "No pending limit orders on watchlist."
    if pending_watchlist:
        pending_watch_section = json.dumps(pending_watchlist)

    prompt = f"""
    You are the Chief Macro Regime & SWING Strategist for {BOT_NAME}.

    ======================================================================
    ROLE DIVISION (SWING ONLY — DO NOT PROPOSE SCALPS):
    ======================================================================
    - Your trade selection responsibility is EXCLUSIVELY **SWING** setups (`FAST TRADE` / `SLOW TRADE`, 4h-24h horizon, ATR Stop-Loss 1.10%-1.50%, TP1 +1.3R [30%] / TP2 +2.2R [35%] / TP3 +3.5R [35%]).
    - DO NOT propose 5m/15m micro-scalps here. A dedicated Scalp Sniper prompt handles short-term scalps separately.
    - Your other two duties are: (1) classifying the current market regime, and (2) auditing active positions & pending orders.

    ======================================================================
    STRICT ANTI-FAKE-ANALYSIS & EVIDENCE RULES:
    ======================================================================
    1. DATA GROUNDING: Cite ONLY exact field values present in the JSON (`p`, `rsi_1h`, `rsi_4h`, `ema21_1h`, `ema21_dist_pct`, `chg_1h_pct`, `chg_4h_pct`, `atr_1h_pct`, `book_ratio`, `cvd_bias`, `whale_ls_ratio`). Never invent S/R zones or patterns not backed by the numbers.
    2. CONTRADICTION BAN: Never excuse a conflicting metric (e.g., never long with `cvd_bias`=="NEGATIVE", never short with `cvd_bias`=="POSITIVE"). If signals conflict, skip the coin.
    3. MANDATORY THESIS CITATIONS: Every `strategic_thesis` MUST include at least 3 exact supplied numeric metrics (e.g., `rsi_1h=54.2, rsi_4h=56.0, ema21_dist=+0.6%, book=1.25`).
    4. PROFIT-HUNTING WITHOUT FAKE DATA: Select `1 to 2` highest-EV SWING setups whenever clean 1H/4H alignment exists (prioritize momentum continuation or `failed_breakdown`/`failed_breakout` sweeps with positive EV). Do NOT waste scan cycles in HOLD when qualifying setups exist; return `"action": "HOLD"` with `[]` ONLY if zero candidates pass the hard gates below.

    ======================================================================
    SWING EVALUATION & REGIME RULES:
    ======================================================================
    1. HIGHER-TIMEFRAME SWING GATES (HARD REQUIREMENTS):
       - For `LONG` Swing:
         * `44.0 <= rsi_1h <= 62.0` AND `rsi_4h >= 44.0`
         * `ema21_dist_pct >= -0.8` (price holding near or above 1H EMA21)
         * `book_ratio >= 0.95` AND `cvd_bias != "NEGATIVE"`
         * `failed_breakout == false` AND `live_candle_sig != "LIVE_BEARISH_EXPANSION"`
         * Not extended near 24h top (`c >= 2.5` and `p >= hi * 0.993`)
       - For `SHORT` Swing:
         * `41.0 <= rsi_1h <= 57.0` AND `rsi_4h <= 58.0`
         * `ema21_dist_pct <= 0.8` (price holding near or below 1H EMA21)
         * `book_ratio <= 1.05` AND `cvd_bias != "POSITIVE"`
         * `whale_ls_ratio < 2.5` (never short into whale long defense)
         * `failed_breakdown == false` AND `live_candle_sig != "LIVE_BULLISH_EXPANSION"`
         * Not extended near 24h bottom (`p <= lo * 1.012`)

    2. ANTI-ANCHORING & REVERSAL DEFENSE:
       The previous market regime (`previous_regime` = "{previous_regime}") is historical context only.
       Evaluate current `chg_1h_pct`, `chg_4h_pct`, `completed_candle_sig`, `live_candle_sig`, `failed_breakdown`, and `failed_breakout` independently.
       - If `failed_breakdown` is true on key assets, do NOT propose new shorts (`new_short_allowed = false` if market-wide).
       - If `failed_breakout` is true on key assets, do NOT propose new longs (`new_long_allowed = false` if market-wide).

    3. ASSET CLASS SEPARATION:
       - If `asset_class == "CRYPTO"`, factor in `btc_regime` and `btc_24h_pct`.
       - If `asset_class == "TRADFI"` (equities/commodities), evaluate the ticker's own 1H/4H structure and order flow without forcing BTC correlation.

    4. REGIME CLASSIFICATION:
       Return exactly one in `market_regime.classification`:
       TREND_UP | TREND_DOWN | RANGE | BREAKOUT_CANDIDATE | BREAKDOWN_CANDIDATE | POSSIBLE_BULLISH_REVERSAL | CONFIRMED_BULLISH_REVERSAL | POSSIBLE_BEARISH_REVERSAL | CONFIRMED_BEARISH_REVERSAL | TRANSITION | HIGH_VOLATILITY | MACRO_SHOCK | LIQUIDITY_STRESS | UNCERTAIN

    5. DECISION PRIORITY:
       1. Data integrity (never fabricate missing data)
       2. Existing position risk (`HOLD` | `REDUCE` | `EXIT` | `MOVE_STOP` | `TAKE_PARTIAL_PROFIT`)
       3. Pending limit orders (`KEEP` | `MODIFY` | `CANCEL`)
       4. Market regime & directional permissions (`new_long_allowed`, `new_short_allowed`)
       5. 0 to 2 A+ SWING opportunities grounded in 1H/4H telemetry

    [CURRENT ACCOUNT CAPITAL & MARGIN CAPACITY]
    • Total Balance: ${cap_total:.2f} USD | Free Margin: ${cap_free:.2f} USD | Mode: {cap_mode}

    [PREVIOUS REGIME & BTC MULTI-TIMEFRAME CONTEXT]
    • Previous Cycle Regime: {previous_regime}
    • BTC Technical Summary: {json.dumps(btc_regime)} | BTC 24h Change: {btc_24h_pct:+.2f}%
    {macro_alert}
    {sideways_clause}
    {weekend_clause}

    {live_news}

    [ACTIVE POSITIONS - DEFENSE & REVERSAL AUDIT]
    Format: [symbol, action, trade_speed, entry, curr_price, pnl_pct, peak_gain_pct, pullback_from_peak]
    {active_pos_section}

    [PENDING WATCHLIST & LIMIT ORDERS AUDIT (Stage 0 - Review for Invalidation)]
    Format: [id, symbol, action, trade_speed, planned_entry, curr_price, dist_to_entry_pct, thesis, pending_mins, order_id]
    {pending_watch_section}

    [SWING UNIVERSE (Top 25 Liquid Perps — 1H/4H Structure & Order Flow)]
    Fields: s, asset_class (CRYPTO|TRADFI), p, mark_price, spread_pct, c (24h%), chg_15m_pct, chg_1h_pct, chg_4h_pct, rsi_1h, rsi_4h, ema21_1h, ema21_dist_pct, atr_1h_pct, v_m (vol M$), fr (funding%), oi_m (OI M$), book_ratio, whale_ls_ratio, cvd_bias (POSITIVE|NEGATIVE|NEUTRAL), completed_candle_sig, live_candle_sig, failed_breakdown, failed_breakout, hi (24h high), lo (24h low)
    {json.dumps(top_candidates)}

    [USER DIRECTIVES]
    {user_rules}

    [RECENT POST-MORTEM LESSONS]
    {lessons}

    Respond ONLY in strict JSON format:
    {{
        "market_regime": {{
            "classification": "TREND_UP" | "TREND_DOWN" | "RANGE" | "BREAKOUT_CANDIDATE" | "BREAKDOWN_CANDIDATE" | "POSSIBLE_BULLISH_REVERSAL" | "CONFIRMED_BULLISH_REVERSAL" | "POSSIBLE_BEARISH_REVERSAL" | "CONFIRMED_BEARISH_REVERSAL" | "TRANSITION" | "HIGH_VOLATILITY" | "MACRO_SHOCK" | "LIQUIDITY_STRESS" | "UNCERTAIN",
            "higher_timeframe_bias": "BULLISH" | "BEARISH" | "NEUTRAL",
            "immediate_momentum": "BULLISH" | "BEARISH" | "NEUTRAL" | "CONFLICTED",
            "new_long_allowed": true,
            "new_short_allowed": true,
            "confidence": 0.85,
            "evidence": "1-sentence citing exact BTC/universe numbers from payload",
            "invalidation_conditions": "1-sentence concrete numeric invalidation trigger"
        }},
        "action": "TRADE" | "HOLD",
        "selected_setups": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "action": "LONG" | "SHORT",
                "strategy_mode": "TREND_MOMENTUM" | "SWEEP_REVERSION",
                "trade_speed": "FAST TRADE" | "SLOW TRADE",
                "coin_trend": "BULLISH UPTREND" | "BEARISH DOWNTREND" | "NEUTRAL CONSOLIDATION",
                "completed_candle_signal": "BULLISH_CONTINUATION" | "BEARISH_CONTINUATION" | "BULLISH_ACCELERATION" | "BEARISH_ACCELERATION" | "BULLISH_REJECTION" | "BEARISH_REJECTION" | "COMPRESSION",
                "live_candle_signal": "LIVE_BULLISH_EXPANSION" | "LIVE_BEARISH_EXPANSION" | "LIVE_BULL_REJECTION" | "LIVE_BEAR_REJECTION" | "FORMING_NEUTRAL",
                "confluence_signals": "1-sentence citing exact 1H/4H and order-flow metrics",
                "contradictory_signals": "None or exact friction metric",
                "strategic_thesis": "1-sentence SWING thesis citing exact rsi_1h, rsi_4h, ema21_dist_pct, chg_4h_pct, and cvd_bias/book_ratio"
            }}
        ],
        "active_position_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "HOLD" | "REDUCE" | "EXIT" | "MOVE_STOP" | "TAKE_PARTIAL_PROFIT",
                "reason": "1-sentence explanation citing real-time candle/price metrics"
            }}
        ],
        "watchlist_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "KEEP" | "MODIFY" | "CANCEL",
                "reason": "1-sentence explanation citing current price/candle metrics"
            }}
        ],
        "macro_verdict": "1-sentence summary of market regime and 1H/4H swing conditions"
    }}
    """

    try:
        t0 = time.time()
        res = ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": ANTI_FAKE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.05
        )
        duration_ms = (time.time() - t0) * 1000.0
        raw_response = res.choices[0].message.content or "{}"
        parsed = extract_clean_json(raw_response)
        log_ai_interaction("GLOBAL_UNIVERSE", "SWING_MACRO_EVALUATION", prompt, raw_response, duration_ms)
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
    1. WHALE LONG DEFENSE & TRAP RULE:
       - If whale_ls_ratio > 2.5, NEVER SHORT. Top institutional accounts are heavily defending support walls.
       - If BTC is red (< -0.5%) and whale_ls_ratio > 1.6, DO NOT LONG.
       - Only short when whale_ls_ratio is balanced (0.8 to 2.2) and 1H RSI is rolling off from swing highs (41.0 to 58.0). Never short the bottom of a drop!
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
                "verdict": "HOLD" | "REDUCE" | "EXIT" | "MOVE_STOP" | "TAKE_PARTIAL_PROFIT",
                "reason": "1-sentence explanation"
            }}
        ],
        "watchlist_directives": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "verdict": "KEEP" | "MODIFY" | "CANCEL",
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
    scalp_fields = (
        "s", "asset_class", "p", "mark_price", "spread_pct", "c",
        "chg_5m_pct", "chg_15m_pct", "vwap", "ema9", "ema20", "swing_hi", "swing_lo",
        "v_m", "rel_vol", "body_pct", "u_wick_pct", "l_wick_pct",
        "cvd_bias", "completed_candle_sig", "live_candle_sig",
        "failed_breakdown", "failed_breakout", "fast_bull_rev", "fast_bear_rev",
        "book_ratio", "whale_ls_ratio", "rsi_1h", "ema21_dist_pct", "atr_1h_pct", "hi", "lo"
    )
    filtered_scalps = [m for m in market_universe if m.get("v_m", 0) >= 10.0][:20]
    if not filtered_scalps:
        filtered_scalps = market_universe[:15]
    top_scalp_candidates = [
        {k: m[k] for k in scalp_fields if k in m}
        for m in filtered_scalps
    ]

    cap_total = capital_info.get("total", 10000.0) if capital_info else 10000.0
    cap_free = capital_info.get("free", 8000.0) if capital_info else 8000.0
    scan_mins = max(1, int(SCAN_INTERVAL_SECONDS // 60))
    min_scalp_prob = int(max(SCALP_MIN_PROBABILITY, 75.0))

    btc_market_bias = ""
    if btc_24h_pct < -0.4:
        btc_market_bias = f"""
    [CRITICAL MARKET REGIME DIRECTIVE: BEARISH / RED BTC ({btc_24h_pct:+.2f}%)]
    - Bitcoin is currently red ({btc_24h_pct:+.2f}%).
    - NEVER propose LONG scalps on falling CRYPTO altcoins (bid walls in red tape can be spoofed exit liquidity).
    - Prioritize SHORT scalps ONLY when `cvd_bias != "POSITIVE"` and `book_ratio <= 0.88`.
    """
    elif btc_24h_pct > 0.8:
        btc_market_bias = f"""
    [CRITICAL MARKET REGIME DIRECTIVE: BULLISH / GREEN BTC ({btc_24h_pct:+.2f}%)]
    - Bitcoin is strong ({btc_24h_pct:+.2f}%).
    - Prioritize LONG scalps ONLY when `cvd_bias != "NEGATIVE"` and `book_ratio >= 1.15`.
    """

    prompt = f"""
    You are the Chief Short-Term Microstructure SCALP Specialist for {BOT_NAME}.

    ======================================================================
    ROLE DIVISION (SCALP ONLY — DO NOT PROPOSE SWINGS):
    ======================================================================
    - Your responsibility is EXCLUSIVELY short-term **SCALP** setups (`SCALP TRADE`, 15m-90m horizon, ATR SL 0.90%-1.20%, TP1 +1.1R [30%] / TP2 +2.0R [35%] / TP3 +3.2R [35%], running on the {scan_mins}-minute cycle).
    - DO NOT propose slow multi-hour swing trades. A separate Swing/Macro prompt handles 1H/4H swings.
    - Max concurrent cap is {MAX_CORRELATED_SCALPS_PER_SIDE} scalps per direction and 2 scalps total alongside swing slots.

    ======================================================================
    STRICT ANTI-FAKE-ANALYSIS & PROFIT-HUNTING RULES:
    ======================================================================
    1. DATA GROUNDING: Cite ONLY exact values from the JSON payload (`p`, `vwap`, `ema9`, `chg_5m_pct`, `chg_15m_pct`, `rel_vol`, `book_ratio`, `cvd_bias`, `rsi_1h`, `ema21_dist_pct`). Never invent S/R levels, order-flow claims, or patterns not in the payload.
    2. CONTRADICTION BAN: NEVER rationalize a contradicting signal (e.g., never long with `cvd_bias == "NEGATIVE"`, never short with `cvd_bias == "POSITIVE"`).
    3. SPOOFING FILTER: L2 `book_ratio` alone is insufficient without non-opposing CVD and price/candle confirmation (`p > vwap` or `fast_bull_rev` for LONG; `p < vwap` or `fast_bear_rev` for SHORT).
    4. MANDATORY THESIS CITATIONS: Every `scalp_thesis` MUST cite at least 3 exact supplied numeric values (e.g., `book_ratio=1.45, chg_15m=+0.42%, rel_vol=1.6, rsi_1h=54.1, cvd=POSITIVE`).
    5. PROFIT-HUNTING PRIORITY: Actively identify the top `1 to 2` highest-EV, fastest-turnover scalp setups with clean book skew + momentum/sweep alignment (`failed_breakdown`/`failed_breakout`/`fast_bull_rev`/`fast_bear_rev`). Do NOT stay idle in HOLD if qualifying setups exist; return `"HOLD"` with `[]` ONLY if zero setups pass the hard gates below.

    [PORTFOLIO CAPITAL ANCHOR]
    • Total Balance: ${cap_total:.2f} USD | Free Margin: ${cap_free:.2f} USD

    [BTC MACRO ANCHOR]
    Regime: {btc_regime.get('regime')} | ADX: {btc_regime.get('adx')} | BTC 24h Change: {btc_24h_pct:+.2f}%
    {btc_market_bias}

    [SCALP MICROSTRUCTURE UNIVERSE (Top 20 Liquid Perps)]
    Fields: s, asset_class (CRYPTO|TRADFI), p, mark_price, spread_pct, c (24h%), chg_5m_pct, chg_15m_pct, vwap, ema9, ema20, swing_hi, swing_lo, v_m (vol M$), rel_vol, body_pct, u_wick_pct, l_wick_pct, cvd_bias (POSITIVE|NEGATIVE|NEUTRAL), completed_candle_sig, live_candle_sig, failed_breakdown, failed_breakout, fast_bull_rev, fast_bear_rev, book_ratio, whale_ls_ratio, rsi_1h, ema21_dist_pct, atr_1h_pct, hi (24h high), lo (24h low)
    {json.dumps(top_scalp_candidates)}

    HARD SCALP QUALIFICATION GATES (ALL MUST PASS):
    1. DEAD CHOP & OVER-EXTENDED FILTER:
       - Disqualify dead chop (`rel_vol < 0.75` or `atr_1h_pct < 0.45`).
       - Disqualify assets already up > +9% or down < -8% in 24h.
    2. LONG SCALP (ALL REQUIRED):
       - `book_ratio >= 1.15` AND `cvd_bias != "NEGATIVE"` (prefer `"POSITIVE"`)
       - `p > vwap` or `p > ema9` or `fast_bull_rev == true`
       - `44.0 <= rsi_1h <= 63.0` AND `ema21_dist_pct >= -0.8`
       - `failed_breakout == false` AND `fast_bear_rev == false` AND `live_candle_sig != "LIVE_BEARISH_EXPANSION"`
       - Not chasing within 0.7% of 24h high (`c >= 2.5` and `p >= hi * 0.993`)
    3. SHORT SCALP (ALL REQUIRED):
       - `book_ratio <= 0.88` AND `cvd_bias != "POSITIVE"` (prefer `"NEGATIVE"`)
       - `p < vwap` or `p < ema9` or `fast_bear_rev == true`
       - `41.0 <= rsi_1h <= 57.0` AND `ema21_dist_pct <= 0.8` AND `whale_ls_ratio < 2.5`
       - `failed_breakdown == false` AND `fast_bull_rev == false` AND `live_candle_sig != "LIVE_BULLISH_EXPANSION"`
       - Not chasing within 1.2% of 24h low (`p <= lo * 1.012`)
    4. ASSET CLASS AWARENESS:
       - If `asset_class == "TRADFI"`, judge purely by its own microstructure, `cvd_bias`, `book_ratio`, and `rsi_1h` rather than BTC direction.

    SCALPING PROBABILITY ASSESSMENT:
    Compute an honest integer `scalp_probability` (70–95) from actual metric confluence. Return the top 1 to 2 setups with `scalp_probability >= {min_scalp_prob}` when available, or `"action": "HOLD"` with `"scalp_setups": []` if none qualify.

    Respond ONLY in strict JSON:
    {{
        "action": "TRADE" | "HOLD",
        "scalp_setups": [
            {{
                "symbol": "SYMBOL/USDT:USDT",
                "action": "LONG" | "SHORT",
                "scalp_probability": 82,
                "expected_profit_potential": "VERY_HIGH" | "HIGH",
                "scalp_type": "ORDERBOOK_IMBALANCE" | "LIQUIDITY_SWEEP" | "VOLATILITY_EXPANSION",
                "trade_speed": "SCALP TRADE",
                "coin_trend": "SCALP_SNIPER",
                "scalp_thesis": "1-sentence SCALP thesis citing exact book_ratio, cvd_bias, chg_15m_pct, rel_vol, and rsi_1h"
            }}
        ],
        "scalp_verdict": "1-sentence summary of current 5m-15m scalp conditions"
    }}
    """

    try:
        t0 = time.time()
        res = ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": ANTI_FAKE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.05
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
                            "trade_speed": r["trade_speed"] or "FAST TRADE",
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
                        "trade_speed": r["trade_speed"] or "FAST TRADE",
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
        btc_4h_task = asyncio.to_thread(fetch_candles, "BTC/USDT:USDT", "4h", 15)
        btc_ticker_task = asyncio.to_thread(_safe_fetch_btc_ticker)
        async def _noop_news():
            return "News skipped for targeted scan."

        news_task = fetch_live_world_and_crypto_news() if scan_type in ("ALL", "MACRO") else _noop_news()
        tickers_task = fetch_full_market_universe_tickers()

        btc_candles, btc_4h_candles, btc_ticker, live_news, market_universe = await asyncio.gather(
            btc_candles_task, btc_4h_task, btc_ticker_task, news_task, tickers_task
        )

        btc_regime = calculate_adx_regime(btc_candles) if btc_candles else {"regime": "RANGING", "adx": 18.0}
        btc_24h_pct = float(btc_ticker.get("percentage") or 0.0)
        if btc_candles and len(btc_candles) >= 5:
            btc_1h_chg = round((btc_candles[-1]["close"] - btc_candles[-2]["close"]) / btc_candles[-2]["close"] * 100.0, 2) if btc_candles[-2]["close"] > 0 else 0.0
            btc_regime["chg_1h_pct"] = btc_1h_chg
        if btc_4h_candles and len(btc_4h_candles) >= 2:
            btc_4h_chg = round((btc_4h_candles[-1]["close"] - btc_4h_candles[-2]["close"]) / btc_4h_candles[-2]["close"] * 100.0, 2) if btc_4h_candles[-2]["close"] > 0 else 0.0
            btc_regime["chg_4h_pct"] = btc_4h_chg
            btc_regime["bias_4h"] = "BULLISH" if btc_4h_chg > 0.25 else ("BEARISH" if btc_4h_chg < -0.25 else "NEUTRAL")
        btc_cand_tel = next((m for m in market_universe if m["s"] == "BTC/USDT:USDT"), None)
        if btc_cand_tel:
            btc_regime["chg_15m_pct"] = btc_cand_tel.get("chg_15m_pct", 0.0)
            btc_regime["chg_5m_pct"] = btc_cand_tel.get("chg_5m_pct", 0.0)
            btc_regime["completed_candle_sig"] = btc_cand_tel.get("completed_candle_sig", "NEUTRAL")
            btc_regime["live_candle_sig"] = btc_cand_tel.get("live_candle_sig", "FORMING_NEUTRAL")

        previous_regime = "UNCERTAIN"
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM bot_state WHERE key = 'last_market_regime'")
            r_reg = c.fetchone()
            if r_reg and r_reg["value"]:
                previous_regime = str(r_reg["value"])

        # 4. Concurrently Run Required AI Prompts (Divided Swing/Macro vs Scalp; skip unused modes)
        async def _noop_ai(default_payload):
            return default_payload

        macro_task = (
            asyncio.to_thread(
                analyze_macro_universe_with_ai,
                market_universe, live_news, btc_regime, btc_24h_pct, capital_info, active_positions_payload, pending_watchlist_payload, previous_regime
            )
            if scan_type in ("ALL", "MACRO")
            else _noop_ai({"action": "HOLD", "selected_setups": [], "active_position_directives": [], "watchlist_directives": [], "macro_verdict": "Skipped for targeted scan."})
        )
        whale_task = (
            asyncio.to_thread(
                analyze_whale_microstructure_with_ai,
                market_universe, btc_regime, btc_24h_pct, capital_info, active_positions_payload, pending_watchlist_payload
            )
            if (ENABLE_WHALE_SIGNALS or scan_type == "WHALE")
            else _noop_ai({"action": "HOLD", "whale_setups": [], "active_position_directives": [], "watchlist_directives": [], "whale_verdict": "Whale setups frozen."})
        )
        scalp_task = (
            asyncio.to_thread(
                analyze_scalp_opportunities_with_ai,
                market_universe, btc_regime, btc_24h_pct, capital_info, active_positions_payload, pending_watchlist_payload
            )
            if scan_type in ("ALL", "SCALP")
            else _noop_ai({"action": "HOLD", "scalp_setups": [], "scalp_verdict": "Skipped for targeted scan."})
        )

        macro_decision, whale_decision, scalp_decision = await asyncio.gather(macro_task, whale_task, scalp_task)

        # 5. Market Regime Logging & Anti-Anchoring State Persistence
        regime_str = btc_regime.get("regime", "RANGING")
        market_regime_info = macro_decision.get("market_regime", {})
        new_long_allowed = True
        new_short_allowed = True
        if market_regime_info:
            reg_name = market_regime_info.get("classification") or regime_str
            reg_conf = float(market_regime_info.get("confidence") or 0.85)
            reg_ev = market_regime_info.get("evidence", "")
            htf_bias = market_regime_info.get("higher_timeframe_bias", "NEUTRAL")
            imm_mom = market_regime_info.get("immediate_momentum", "NEUTRAL")
            if isinstance(market_regime_info.get("new_long_allowed"), bool):
                new_long_allowed = market_regime_info.get("new_long_allowed")
            if isinstance(market_regime_info.get("new_short_allowed"), bool):
                new_short_allowed = market_regime_info.get("new_short_allowed")
            with get_db_connection() as conn:
                conn.execute(
                    "INSERT INTO bot_state (key, value) VALUES ('last_market_regime', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (reg_name,)
                )
            log_event(
                "INFO", "AI_REGIME",
                f"Institutional Market Regime: {reg_name} (Prev: {previous_regime} | HTF: {htf_bias} | Immediate: {imm_mom} | Conf: {reg_conf:.2f}). Evidence: {reg_ev}"
            )

        # 6. Apply AI Watchlist Applicability Directives (Stage 0 Audit)
        all_watchlist_directives = (
            macro_decision.get("watchlist_directives", []) +
            whale_decision.get("watchlist_directives", [])
        )

        for w_dir in all_watchlist_directives:
            w_sym = w_dir.get("symbol")
            w_verdict = str(w_dir.get("verdict") or "KEEP").upper()
            w_reason = w_dir.get("reason", "")
            if not w_sym or w_verdict not in ("CANCEL", "CANCEL_INVALID"):
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

            log_event("INFO", "AI_WATCHLIST", f"Pending setup {w_sym} cancelled by AI audit ({w_verdict}): {w_reason}")
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

        # 7. Apply Institutional Position Defense Directives (HOLD, REDUCE, EXIT, MOVE_STOP, TAKE_PARTIAL_PROFIT)
        all_reversal_directives = (
            macro_decision.get("active_position_directives", []) +
            whale_decision.get("active_position_directives", [])
        )

        for directive in all_reversal_directives:
            d_sym = directive.get("symbol")
            d_verdict = str(directive.get("verdict") or "HOLD").upper()
            d_reason = directive.get("reason", "")
            if not d_sym or d_verdict == "HOLD":
                continue

            match_pos = next((p for p in active_positions_payload if p["symbol"] == d_sym), None)
            if not match_pos:
                continue

            # EXIT / HARVEST_NOW: Full emergency exit on thesis invalidation or momentum collapse
            if d_verdict in ("EXIT", "HARVEST_NOW"):
                if match_pos["pnl_pct"] >= 0.50 or any(k in d_reason.upper() for k in ["REVERSAL", "SHOCK", "INVALID", "FLIP", "COLLAPSE"]):
                    if match_pos["sl_order_id"]:
                        await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                    if match_pos.get("tp1_order_id"):
                        await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["tp1_order_id"])
                    if TRADING_MODE in ("LIVE", "TESTNET"):
                        close_qty = match_pos["executed_qty"] * match_pos["remaining_ratio"]
                        if close_qty > 0:
                            await asyncio.to_thread(emergency_binance_market_close, match_pos["symbol"], match_pos["action"], close_qty)

                    net_leg_pnl = match_pos["pnl_pct"] - ROUND_TRIP_FEE_PCT
                    final_pnl = round(match_pos["partial_realized_pnl"] + (match_pos["remaining_ratio"] * net_leg_pnl), 2)
                    with get_db_connection() as conn:
                        conn.execute("UPDATE watched_signals SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, exit_reason = 'AI_DEFENSE_EXIT', movement_tag = '🎯 AI_DEFENSE_EXIT', last_updated = CURRENT_TIMESTAMP WHERE id = ?", (final_pnl, match_pos["id"]))

                    log_event("INFO", "AI_DEFENSE", f"Position exited on {d_sym} ({d_verdict}): {final_pnl:+.2f}% net. Reason: {d_reason}")
                    if chat_id and context:
                        msg = (
                            f"🛡️ <b>AI DEFENSE: POSITION EXIT ({d_verdict})</b>\n\n"
                            f"• <b>Contract:</b> <code>{match_pos['symbol']}</code> ({match_pos['action']})\n"
                            f"• <b>Exit Mark:</b> ${match_pos['curr_price']:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                            f"• <b>Total Realized Trade PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                            f"• <b>AI Diagnostic:</b> <i>{html.escape(d_reason)}</i>\n"
                            f"🤖 <b>Closed immediately on Binance to defend capital.</b>"
                        )
                        try:
                            await context.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML")
                        except Exception:
                            pass

            # REDUCE / TAKE_PARTIAL_PROFIT: Bank 50% partial profits and move stop to break-even
            elif d_verdict in ("REDUCE", "TAKE_PARTIAL_PROFIT") and match_pos["pnl_pct"] >= 0.45 and match_pos["remaining_ratio"] > 0.50:
                reduce_ratio = 0.50
                reduce_qty = safe_amount_to_precision(match_pos["symbol"], match_pos["executed_qty"] * reduce_ratio)
                if reduce_qty > 0 and TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, match_pos["symbol"], match_pos["action"], reduce_qty)
                
                net_partial_pnl = match_pos["pnl_pct"] - ROUND_TRIP_FEE_PCT
                accrued_pnl = round(match_pos["partial_realized_pnl"] + (reduce_ratio * net_partial_pnl), 2)
                new_remaining = round(match_pos["remaining_ratio"] - reduce_ratio, 2)
                
                # Move stop to break-even on the remaining half
                be_sl = round(match_pos["entry"] * 1.0015, 6) if match_pos["action"] == "LONG" else round(match_pos["entry"] * 0.9985, 6)
                if match_pos["sl_order_id"]:
                    await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                new_sl_id = ""
                rem_qty = safe_amount_to_precision(match_pos["symbol"], match_pos["executed_qty"] * new_remaining)
                if rem_qty > 0 and TRADING_MODE in ("LIVE", "TESTNET"):
                    new_sl_id = await asyncio.to_thread(place_binance_stop_loss, match_pos["symbol"], match_pos["action"], rem_qty, be_sl)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET remaining_ratio = ?, partial_realized_pnl = ?, current_sl = ?, binance_sl_order_id = ?, movement_tag = '💰 AI_PARTIAL_HARVEST', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ? AND status = 'WATCHING'
                    """, (new_remaining, accrued_pnl, be_sl, new_sl_id, match_pos["id"]))

                log_event("INFO", "AI_DEFENSE", f"Partial 50% harvested on {d_sym} ({d_verdict}): {net_partial_pnl:+.2f}%. Remaining: {new_remaining*100:.0f}%. Reason: {d_reason}")
                if chat_id and context:
                    msg = (
                        f"💰 <b>AI PROFIT BANK: PARTIAL HARVEST ({d_verdict})</b>\n\n"
                        f"• <b>Contract:</b> <code>{match_pos['symbol']}</code> ({match_pos['action']})\n"
                        f"• <b>Harvest PnL:</b> <code>{net_partial_pnl:+.2f}%</code> (Secured: <code>{accrued_pnl:+.2f}%</code>)\n"
                        f"• <b>Remaining Size:</b> {new_remaining*100:.0f}% (SL moved to ${be_sl:.6f})\n"
                        f"• <b>Reason:</b> <i>{html.escape(d_reason)}</i>"
                    )
                    try:
                        await context.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML")
                    except Exception:
                        pass

            # MOVE_STOP / TIGHTEN_SL: Dynamic stop tightening to break-even + profit
            elif d_verdict in ("MOVE_STOP", "TIGHTEN_SL") and match_pos["pnl_pct"] >= 0.55:
                tighter_sl = round(match_pos["entry"] * 1.0020, 6) if match_pos["action"] == "LONG" else round(match_pos["entry"] * 0.9980, 6)
                is_better = (tighter_sl > match_pos["current_sl"]) if match_pos["action"] == "LONG" else (tighter_sl < match_pos["current_sl"])
                is_safe_side = (match_pos["curr_price"] >= tighter_sl * 1.0020) if match_pos["action"] == "LONG" else (match_pos["curr_price"] <= tighter_sl * 0.9980)
                if is_better and is_safe_side:
                    if match_pos["sl_order_id"]:
                        await asyncio.to_thread(cancel_binance_order, match_pos["symbol"], match_pos["sl_order_id"])
                    new_sl_id = ""
                    rem_qty = safe_amount_to_precision(match_pos["symbol"], match_pos["executed_qty"] * match_pos["remaining_ratio"])
                    if rem_qty > 0 and TRADING_MODE in ("LIVE", "TESTNET"):
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, match_pos["symbol"], match_pos["action"], rem_qty, tighter_sl)

                    with get_db_connection() as conn:
                        conn.execute("UPDATE watched_signals SET current_sl = ?, binance_sl_order_id = ?, movement_tag = '🛡️ AI_TIGHTENED_SL', last_updated = CURRENT_TIMESTAMP WHERE id = ? AND status = 'WATCHING'", (tighter_sl, new_sl_id, match_pos["id"]))

                    log_event("INFO", "AI_DEFENSE", f"Stop-Loss tightened on {d_sym} ({d_verdict}) to ${tighter_sl:.6f}. Reason: {d_reason}")

        # 7. Profit Prioritization & Candidate Ranking Engine
        raw_candidates = []

        # Ingest AI Swing/Macro Setups (Strictly SWING: FAST TRADE / SLOW TRADE)
        if macro_decision.get("action") == "TRADE":
            for setup in macro_decision.get("selected_setups", []):
                raw_speed = str(setup.get("trade_speed") or "FAST TRADE").upper()
                swing_speed = "SLOW TRADE" if "SLOW" in raw_speed else "FAST TRADE"
                raw_candidates.append({
                    "source": "AI_MACRO",
                    "symbol": setup.get("symbol"),
                    "action": setup.get("action"),
                    "strategy_mode": setup.get("strategy_mode", "TREND_MOMENTUM"),
                    "trade_speed": swing_speed,
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

        # Ingest AI Scalp Sniper Setups (Strictly SCALP)
        min_scalp_prob = max(SCALP_MIN_PROBABILITY, 75.0)
        if scalp_decision.get("action") == "TRADE":
            for setup in scalp_decision.get("scalp_setups", []):
                prob = float(setup.get("scalp_probability") or setup.get("movement_probability") or 0.0)
                if prob >= min_scalp_prob:
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
            thesis_txt = str(cand.get("thesis") or "").strip()
            if not sym or act not in ["LONG", "SHORT"]:
                continue
            # Anti-Fake Guard: reject setups without concrete numeric thesis grounding
            if len(thesis_txt) < 15 or not re.search(r"\d", thesis_txt):
                log_event("INFO", "ANTI_FAKE_GATE", f"Rejected {act} on {sym} ({cand['source']}): Thesis lacked concrete numeric evidence.")
                continue

            bridge = bridge_symbol_formats(sym)
            cand_info = next((m for m in market_universe if m["s"] == bridge["ccxt"] or m["s"] == sym), None)
            curr_price = float(cand_info["p"]) if cand_info else await asyncio.to_thread(fetch_real_perp_market_price, bridge["ccxt"])
            if curr_price <= 0:
                continue

            c1h = await asyncio.to_thread(fetch_candles, bridge["ccxt"], "1h", 30)
            c4h = await asyncio.to_thread(fetch_candles, bridge["ccxt"], "4h", 30)

            rsi_1h = calculate_rsi([c["close"] for c in c1h]) if c1h else float(cand_info.get("rsi_1h", 50.0) if cand_info else 50.0)
            rsi_4h = calculate_rsi([c["close"] for c in c4h]) if c4h else float(cand_info.get("rsi_4h", 50.0) if cand_info else 50.0)
            atr_1h = calculate_atr(c1h, 14) if c1h else (curr_price * (float(cand_info.get("atr_1h_pct", 1.5)) / 100.0) if cand_info and cand_info.get("atr_1h_pct") else curr_price * 0.015)
            ema21_1h = calculate_ema([c["close"] for c in c1h], 21) if c1h else float(cand_info.get("ema21_1h", curr_price) if cand_info else curr_price)

            vol_m = cand_info["v_m"] if cand_info else 10.0
            funding = cand_info["fr"] if cand_info else 0.0
            chg = cand_info["c"] if cand_info else 0.0
            book_ratio = cand_info.get("book_ratio", 1.0) if cand_info else 1.0
            whale_ls = cand_info.get("whale_ls_ratio", 1.0) if cand_info else 1.0
            hi_24h = float(cand_info.get("hi") or curr_price) if cand_info else curr_price
            lo_24h = float(cand_info.get("lo") or curr_price) if cand_info else curr_price
            failed_breakdown = bool(cand_info.get("failed_breakdown", False)) if cand_info else False
            failed_breakout = bool(cand_info.get("failed_breakout", False)) if cand_info else False
            fast_bull_rev = bool(cand_info.get("fast_bull_rev", False)) if cand_info else False
            fast_bear_rev = bool(cand_info.get("fast_bear_rev", False)) if cand_info else False
            completed_candle_sig = str(cand_info.get("completed_candle_sig", "NEUTRAL")) if cand_info else "NEUTRAL"
            live_candle_sig = str(cand_info.get("live_candle_sig", "FORMING_NEUTRAL")) if cand_info else "FORMING_NEUTRAL"
            cvd_bias = str(cand_info.get("cvd_bias", "NEUTRAL")) if cand_info else "NEUTRAL"

            tradfi_asset = is_tradfi_symbol(bridge["ccxt"])
            quant_score = calculate_objective_quant_score(act, rsi_1h, rsi_4h, btc_regime["regime"], vol_m, funding, chg, btc_24h_pct, book_ratio, is_tradfi=tradfi_asset)
            # Hard Quantitative Safety Veto: Never allow AI confidence to override a failed quant safety check (< 0.50)
            if quant_score < 0.50:
                log_event("INFO", "QUANT_VETO", f"Rejected {act} on {sym}: Objective quant score {quant_score:.2f} failed safety gate (RSI 1H: {rsi_1h:.1f}, BTC 24h: {btc_24h_pct:+.2f}%, Book: {book_ratio}x).")
                continue

            if cand["source"] in ("WHALE_FLOW", "AI_SCALP"):
                quant_score = round((quant_score * 0.65) + (float(cand["confidence"]) * 0.35), 2)

            if quant_score < CONFIDENCE_THRESHOLD:
                continue

            is_scalp = (cand["source"] == "AI_SCALP")

            # Strict Trend, Orderbook, CVD, 24h Range Room & Real-Time Candle Protection Gate (Enforced on ALL trades):
            if act == "LONG":
                min_book_long = 1.15 if is_scalp else 0.95
                cvd_veto_long = (cvd_bias == "NEGATIVE")
                near_24h_top = (chg >= 2.5 and hi_24h > 0 and curr_price >= hi_24h * 0.993)
                btc_veto_long = (not tradfi_asset and btc_24h_pct < -1.20)
                candle_veto_long = (failed_breakout or fast_bear_rev or live_candle_sig == "LIVE_BEARISH_EXPANSION" or not new_long_allowed)
                if curr_price < (ema21_1h * 0.992) or rsi_1h < 44.0 or rsi_1h > 63.0 or book_ratio < min_book_long or cvd_veto_long or btc_veto_long or near_24h_top or candle_veto_long:
                    l_reason = "Failed Breakout / Live Bearish Reversal" if candle_veto_long else ("Negative CVD Conflict" if cvd_veto_long else "Failed alignment")
                    log_event("INFO", "ALIGNMENT_GATE", f"Filtered LONG on {sym}: {l_reason} (Price ${curr_price:.4f} vs EMA21 ${ema21_1h:.4f}, 24h High ${hi_24h:.4f}, RSI {rsi_1h:.1f}, Book {book_ratio}x, CVD {cvd_bias}, Candle {completed_candle_sig}/{live_candle_sig}).")
                    continue
            elif act == "SHORT":
                max_book_short = 0.88 if is_scalp else 1.05
                cvd_veto_short = (cvd_bias == "POSITIVE")
                near_24h_bottom = (lo_24h > 0 and curr_price <= lo_24h * 1.012)
                whale_long_wall = (whale_ls >= 2.5)
                btc_veto_short = (not tradfi_asset and btc_24h_pct > 1.80)
                candle_veto_short = (failed_breakdown or fast_bull_rev or live_candle_sig == "LIVE_BULLISH_EXPANSION" or not new_short_allowed)
                if curr_price > (ema21_1h * 1.008) or rsi_1h > 57.0 or rsi_1h < 41.0 or book_ratio > max_book_short or cvd_veto_short or btc_veto_short or near_24h_bottom or whale_long_wall or candle_veto_short:
                    reason = (
                        "Failed Breakdown / Fast Bullish Reversal" if candle_veto_short
                        else ("Positive CVD Conflict" if cvd_veto_short else ("Whale Long Defense (L/S >= 2.5)" if whale_long_wall else ("Near 24h Low (<1.2%)" if near_24h_bottom else "Failed Alignment")))
                    )
                    log_event("INFO", "ALIGNMENT_GATE", f"Filtered SHORT on {sym}: {reason} (Price ${curr_price:.4f} vs 24h Low ${lo_24h:.4f}, RSI {rsi_1h:.1f}, Whale L/S {whale_ls:.2f}x, Book {book_ratio}x, CVD {cvd_bias}, Candle {completed_candle_sig}/{live_candle_sig}).")
                    continue

            levels = calculate_deterministic_trade_levels(curr_price, atr_1h, act, ema21_1h, cand["strategy_mode"], change_24h=chg, is_scalp=is_scalp)
            
            # Dynamic Position Sizing used to compute EXPECTED CASH PROFIT
            pos_size_calc = calculate_dynamic_position_size(quant_score, levels["risk_pct"], balance=account_balance, is_scalp=is_scalp)

            # Expected Profit Index formula: EV% = (Prob * Gain_TP1) - ((1 - Prob) * Risk)
            gain_tp1_pct = abs(levels["t1"] - levels["entry"]) / levels["entry"] * 100.0 if levels["entry"] > 0 else 0.0
            risk_pct = levels["risk_pct"]
            ev_pct = (quant_score * gain_tp1_pct) - ((1.0 - quant_score) * risk_pct)
            expected_cash_profit = pos_size_calc * (ev_pct / 100.0)

            # Fair ranking between divided Scalp and Swing modes
            if is_scalp:
                profit_index = (expected_cash_profit * 1.15) + 3.0
            else:
                profit_index = expected_cash_profit + (5.0 if ((act == "LONG" and rsi_4h >= 48.0) or (act == "SHORT" and rsi_4h <= 52.0)) else 0.0)

            # Confluence Bonuses: Confluence between Whale, Macro, & Scalp AI
            if any(other["symbol"] == sym and other["source"] != cand["source"] for other in raw_candidates):
                profit_index += 8.0

            # Order book ratio imbalance bonus
            if (act == "SHORT" and book_ratio < 0.70) or (act == "LONG" and book_ratio > 1.30):
                profit_index += 6.0

            # Real-Time Completed Candle & CVD Momentum Bonus
            if act == "LONG" and completed_candle_sig in ("BULLISH_ACCELERATION", "BULLISH_CONTINUATION") and cvd_bias == "POSITIVE":
                profit_index += 7.0
            elif act == "SHORT" and completed_candle_sig in ("BEARISH_ACCELERATION", "BEARISH_CONTINUATION") and cvd_bias == "NEGATIVE":
                profit_index += 7.0

            # Sweet-Spot Rollover / Pullback Confluence:
            if (act == "SHORT" and 44.0 <= rsi_1h <= 54.0) or (act == "LONG" and 46.0 <= rsi_1h <= 56.0):
                profit_index += 8.0

            # Balanced Institutional Flow Bonus (Whale L/S between 0.8 and 2.2, avoiding whale defense traps):
            if (act == "SHORT" and 0.8 <= whale_ls <= 2.2 and (tradfi_asset or btc_24h_pct < -0.4)):
                profit_index += 6.0
            elif (act == "LONG" and 1.1 <= whale_ls <= 2.2 and (tradfi_asset or btc_24h_pct > 0.4)):
                profit_index += 6.0

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

        # 7. PRIORITIZE PROFIT & DEDUPLICATE BY SYMBOL:
        processed_candidates.sort(key=lambda x: x["profit_index"], reverse=True)
        deduped_candidates = []
        seen_cand_symbols = set()
        for item in processed_candidates:
            c_sym = item["bridge"]["ccxt"]
            if c_sym not in seen_cand_symbols:
                seen_cand_symbols.add(c_sym)
                deduped_candidates.append(item)
        processed_candidates = deduped_candidates

        watched_cnt, _ = count_active_and_daily_trades()
        active_scalp_cnt = sum(1 for p in (active_positions_payload + pending_watchlist_payload) if p.get("trade_speed") == "SCALP TRADE")
        active_swing_cnt = sum(1 for p in (active_positions_payload + pending_watchlist_payload) if p.get("trade_speed") != "SCALP TRADE")
        max_concurrent_scalps = max(1, MAX_CONCURRENT_POSITIONS // 2)
        max_concurrent_swings = max(1, MAX_CONCURRENT_POSITIONS - max_concurrent_scalps)

        emitted_count = 0
        emitted_scalps = 0
        emitted_swings = 0
        emitted_symbols = set()
        emitted_directions = []
        for item in processed_candidates:
            if watched_cnt + emitted_count >= MAX_CONCURRENT_POSITIONS:
                break

            cand = item["cand"]
            bridge = item["bridge"]
            act = cand["action"]
            sym = bridge["ccxt"]
            is_scalp = item.get("is_scalp", False)

            if is_scalp and (active_scalp_cnt + emitted_scalps) >= max_concurrent_scalps:
                log_event("INFO", "PROFIT_ENGINE", f"Scalp slot cap reached ({active_scalp_cnt + emitted_scalps}/{max_concurrent_scalps}). Skipping {sym}.")
                continue
            if not is_scalp and (active_swing_cnt + emitted_swings) >= max_concurrent_swings:
                log_event("INFO", "PROFIT_ENGINE", f"Swing slot cap reached ({active_swing_cnt + emitted_swings}/{max_concurrent_swings}). Skipping {sym}.")
                continue

            # Prevent Duplicate Orders: Never open a new setup on a symbol that is already active/watching
            active_symbols = [p["symbol"] for p in active_positions_payload] + [p["symbol"] for p in pending_watchlist_payload] + list(emitted_symbols)
            if sym in active_symbols:
                log_event("INFO", "PROFIT_ENGINE", f"Symbol {sym} already active on watchlist/position. Skipping duplicate emission.")
                continue

            # Enforce directional correlation limit (Scalps & Trades up to 4 concurrent slots in trend direction)
            dir_count = count_active_positions_by_direction(act)
            already_emitted_same_dir = emitted_directions.count(act)
            max_allowed_dir = MAX_CORRELATED_SCALPS_PER_SIDE if is_scalp else MAX_CORRELATED_POSITIONS_PER_SIDE
            if (dir_count + already_emitted_same_dir) >= max_allowed_dir:
                log_event("INFO", "PROFIT_ENGINE", f"Correlated {act} limit reached ({dir_count + already_emitted_same_dir}/{max_allowed_dir}). Skipping {sym}.")
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
                tp1_m = levels.get("tp1_mult", 1.1 if is_scalp else 1.3)
                tp2_m = levels.get("tp2_mult", 2.0 if is_scalp else 2.2)
                tp3_m = levels.get("tp3_mult", 3.2 if is_scalp else 3.5)
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
            if is_scalp:
                emitted_scalps += 1
            else:
                emitted_swings += 1
            emitted_symbols.add(sym)
            emitted_directions.append(act)

            side_badge = "🟢 BUY / LONG" if act == "LONG" else "🔴 SELL / SHORT"
            if cand["source"] == "WHALE_FLOW":
                source_badge = "🐋 WHALE FLOW"
            elif cand["source"] == "AI_SCALP":
                source_badge = "⚡ SCALP SNIPER"
            else:
                source_badge = "🌐 SWING FLOW"
            exec_badge = f"🤖 [AUTO-{TRADING_MODE}]" if TRADING_MODE in ("LIVE", "TESTNET") else "📋 [SIMULATION]"

            tp_desc = (
                f"• TP1 (30% Qty): <code>${levels['t1']:.6f}</code> (+{levels.get('tp1_mult', 1.2)}R) {'✅' if binance_tp1_id else ''}\n"
                f"• TP2 (35% Qty): <code>${levels['t2']:.6f}</code> (+{levels.get('tp2_mult', 2.2)}R) {'✅' if binance_tp2_id else ''}\n"
                f"• TP3 (35% Qty): <code>${levels['t3']:.6f}</code> (+{levels.get('tp3_mult', 3.5)}R) {'✅' if binance_tp3_id else ''}"
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
