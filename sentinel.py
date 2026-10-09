"""
================================================================================
MODULE: sentinel.py
ROLE  : Manual Trader's Assistant & Risk Defense Sentinel
        (Strict Stage 0 Anchor, Peak Giveback Exit, Auto Break-Even, Non-Cascading Checks)
================================================================================
"""

import os
import sys
import time
import datetime
import asyncio
import aiohttp
import re
from config_db import (
    BOT_NAME,
    WATCH_INTERVAL_SECONDS,
    PENDING_TTL_HOURS,
    STAGNATION_MINS,
    STAGNATION_PNL_FLOOR,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TRADING_MODE,
    DEFAULT_LEVERAGE,
    SCALP_LEVERAGE,
    is_weekend_regime,
    utc_now,
    get_exchange_instance,
    get_db_connection,
    log_event,
    bridge_symbol_formats,
    is_system_shutdown_requested,
    fetch_current_account_capital,
    place_binance_stop_loss,
    cancel_binance_order,
    emergency_binance_market_close,
    fetch_binance_order_status,
    format_order_quantity,
    set_symbol_leverage_and_margin,
)

exchange = get_exchange_instance()

LAST_BTC_PRICE = 0.0
LAST_BTC_CHECK_TIME = 0.0

async def send_telegram_alert(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_chat_id = str(TELEGRAM_CHAT_ID).strip()
    payload = {"chat_id": clean_chat_id, "text": message, "parse_mode": "HTML"}
    
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=6.0)) as response:
            if response.status != 200:
                plain_text = re.sub(r"<[^>]+>", "", message)
                fallback_payload = {"chat_id": clean_chat_id, "text": plain_text}
                await session.post(url, json=fallback_payload, timeout=aiohttp.ClientTimeout(total=6.0))
    except Exception as e:
        log_event("ERROR", "SENTINEL_TG", f"Telegram alert delivery exception: {e}")

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
        log_event("WARN", "SENTINEL_PRICE", f"Price ticker fetch error on {symbol}: {e}")
        return 0.0

async def check_btc_flash_shock() -> str:
    global LAST_BTC_PRICE, LAST_BTC_CHECK_TIME
    try:
        btc_price = await asyncio.to_thread(fetch_real_perp_market_price, "BTC/USDT:USDT")
        now = time.time()
        
        if LAST_BTC_PRICE > 0 and (now - LAST_BTC_CHECK_TIME) <= 300:
            pct_change = (btc_price - LAST_BTC_PRICE) / LAST_BTC_PRICE * 100.0
            if pct_change <= -2.0:
                log_event("EMERGENCY", "SENTINEL", f"BTC Shock Dump: {pct_change:.2f}% in <5m.")
                return "DUMP"
            elif pct_change >= 2.0:
                log_event("EMERGENCY", "SENTINEL", f"BTC Shock Pump: {pct_change:+.2f}% in <5m.")
                return "PUMP"
        
        LAST_BTC_PRICE = btc_price
        LAST_BTC_CHECK_TIME = now
    except Exception:
        pass
    return "NONE"

def parse_database_timestamp(raw_ts: str) -> datetime.datetime:
    if not raw_ts:
        return utc_now()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.datetime.strptime(raw_ts.split("+")[0].split("Z")[0], fmt)
        except ValueError:
            pass
    return utc_now()

async def run_sentinel_cycle(session: aiohttp.ClientSession):
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM watched_signals WHERE status = 'WATCHING'")
        watched_items = c.fetchall()

    if not watched_items:
        return

    btc_shock = await check_btc_flash_shock()

    for item in watched_items:
        w_id = item["id"]
        sym = item["symbol"]
        act = item["action"]
        entry = float(item["entry_limit"] or 0.0)
        current_sl = float(item["current_sl"] or 0.0)
        t1 = float(item["target_1"] or 0.0)
        t2 = float(item["target_2"] or 0.0)
        t3 = float(item["target_3"] or 0.0)
        stage = int(item["stage"] or 0)
        pos_size_usd = float(item["position_size_usd"] or 500.0)
        executed_qty = float(item["executed_qty"] or 0.0)
        item_dict = dict(item)
        remaining_ratio = float(item_dict.get("remaining_ratio", 1.0) or 1.0)
        partial_realized_pnl = float(item_dict.get("partial_realized_pnl", 0.0) or 0.0)
        entry_order_id = str(item["binance_entry_order_id"] or "")
        sl_order_id = str(item["binance_sl_order_id"] or "")
        try:
            high_fav = float(item["highest_favorable_price"] or entry if entry > 0 else 1.0)
        except (ValueError, TypeError):
            high_fav = entry if entry > 0 else 1.0
            corrupt_val = str(item["highest_favorable_price"] or "")
            with get_db_connection() as conn_repair:
                conn_repair.execute("UPDATE watched_signals SET highest_favorable_price = ?, movement_tag = ? WHERE id = ?", (high_fav, corrupt_val or "🎯 TP1_LOCKED", w_id))
        tag_current = item["movement_tag"] or ""
        speed_tag = item["trade_speed"] or "FAST TRADE"
        coin_trend_str = str(item["coin_trend"] or "").upper()
        is_scalp_trade = (speed_tag == "SCALP TRADE" or "SCALP" in coin_trend_str)

        if entry <= 0 or current_sl <= 0:
            continue

        try:
            curr_price = await asyncio.to_thread(fetch_real_perp_market_price, sym)
            if curr_price <= 0:
                continue
        except Exception as e:
            log_event("WARN", "SENTINEL_TICK", f"Price fetch failed on {sym}: {e}")
            continue

        # 1. EMERGENCY MACRO SHOCK DEFENSE
        if (btc_shock == "DUMP" and act == "LONG") or (btc_shock == "PUMP" and act == "SHORT"):
            if stage == 0:
                if entry_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, entry_order_id)
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET status = 'INVALIDATED', realized_pnl = 0.0, last_updated = CURRENT_TIMESTAMP WHERE id = ?", (w_id,))
                msg = (
                    f"⚠️ <b>MACRO ALERT: SETUP CANCELLED</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Reason:</b> BTC 2.0%+ Sudden {btc_shock}.\n"
                    f"🤖 <b>Cancelled pending Binance limit order.</b>"
                )
                await send_telegram_alert(session, msg)
                continue
            elif stage >= 1 and "SHOCK_BE_DEFENDED" not in tag_current:
                tighter_sl = entry
                if sl_order_id and executed_qty > 0:
                    await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                    new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, tighter_sl)
                else:
                    new_sl_id = sl_order_id

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET current_sl = ?, binance_sl_order_id = ?, movement_tag = '⚠️ SHOCK_BE_DEFENDED', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (tighter_sl, new_sl_id, w_id))
                current_sl = tighter_sl
                msg = (
                    f"🛡️ <b>EMERGENCY ACTION: MOVE SL TO BREAK-EVEN</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Reason:</b> BTC Flash {btc_shock}.\n"
                    f"🤖 <b>Adjusted live Binance Stop-Loss to Entry: ${entry:.6f}.</b>"
                )
                await send_telegram_alert(session, msg)

        # 2. STAGE 0: PENDING LIMIT ORDER TRACKING & AUTO FILL
        if stage == 0:
            created_at = parse_database_timestamp(item["timestamp"])
            minutes_pending = (utc_now() - created_at).total_seconds() / 60.0
            pending_limit_mins = 45.0 if is_scalp_trade else (PENDING_TTL_HOURS * 60.0)

            # EXPIRATION WITH BINANCE CANCELLATION
            if minutes_pending >= pending_limit_mins:
                if entry_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, entry_order_id)
                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = 'EXPIRED', realized_pnl = 0.0, movement_tag = '⏱️ EXPIRED_TTL', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (w_id,))

                if is_scalp_trade:
                    speed_badge = "⚡ [SCALP TRADE]"
                elif speed_tag == "FAST TRADE":
                    speed_badge = "🚀 [FAST TRADE]"
                else:
                    speed_badge = "⏳ [SLOW TRADE]"

                expired_msg = (
                    f"⏱️ <b>SETUP EXPIRED (Unfilled Limit Order)</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act}) | {speed_badge}\n"
                    f"• <b>Planned Entry:</b> <code>${entry:.6f}</code>\n"
                    f"• <b>Pending Duration:</b> <code>{int(minutes_pending // 60)}h {int(minutes_pending % 60)}m</code>\n"
                    f"🤖 <b>Cancelled pending limit order on Binance. Slot freed.</b>"
                )
                await send_telegram_alert(session, expired_msg)
                continue

            # Check fill: either price touched entry or Binance order is FILLED/CLOSED
            is_entry_touched = (curr_price <= entry) if act == "LONG" else (curr_price >= entry)
            is_order_filled = False

            if entry_order_id and TRADING_MODE in ("LIVE", "TESTNET"):
                ord_status = await asyncio.to_thread(fetch_binance_order_status, sym, entry_order_id)
                if ord_status.get("status") == "CLOSED":
                    is_order_filled = True
                    if ord_status.get("filled", 0) > 0:
                        executed_qty = ord_status["filled"]

            if is_entry_touched or is_order_filled:
                stage = 1
                target_leverage = SCALP_LEVERAGE if is_scalp_trade else DEFAULT_LEVERAGE
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(set_symbol_leverage_and_margin, sym, target_leverage)

                if executed_qty <= 0:
                    executed_qty = format_order_quantity(sym, pos_size_usd, curr_price)

                # Automatically submit Hard Stop-Loss order on Binance
                new_sl_id = ""
                if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                    new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, current_sl)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET stage = 1, highest_favorable_price = ?, executed_qty = ?, remaining_ratio = 1.0, partial_realized_pnl = 0.0, binance_sl_order_id = ?, last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (entry, executed_qty, new_sl_id, w_id))

                if is_scalp_trade:
                    speed_badge = "⚡ SCALP SNIPER"
                    tp_display = (
                        f"   • TP1 (60% Size): <code>${t1:.6f}</code> (+1.0R)\n"
                        f"   • TP2 (25% Size): <code>${t2:.6f}</code> (+1.8R)\n"
                        f"   • TP3 (15% Size): <code>${t3:.6f}</code> (+2.6R)"
                    )
                else:
                    speed_badge = "🚀 FAST TRADE" if speed_tag == "FAST TRADE" else "⏳ SLOW TRADE"
                    tp_display = (
                        f"   • TP1 (50% Size): <code>${t1:.6f}</code> (+1.2R)\n"
                        f"   • TP2 (30% Size): <code>${t2:.6f}</code> (+2.0R)\n"
                        f"   • TP3 (20% Size): <code>${t3:.6f}</code> (+3.2R)"
                    )

                exec_mode_tag = f"🤖 [AUTO-{TRADING_MODE}]" if TRADING_MODE in ("LIVE", "TESTNET") else "📋 [SIMULATION]"
                msg = (
                    f"🟢 <b>TRADE TRIGGERED / ENTRY FILLED ({speed_badge}) {exec_mode_tag}</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> (<code>{act}</code>)\n"
                    f"• <b>Fill Price:</b> <b>${curr_price:.6f}</b>\n"
                    f"• <b>Position Qty:</b> <code>{executed_qty} contracts</code> (${pos_size_usd:.0f} USD)\n\n"
                    f"📋 <b>ACTIVE RISK BRACKETS:</b>\n"
                    f"1. <b>Hard Stop-Loss:</b> <code>${current_sl:.6f}</code> {'(Active on Binance)' if new_sl_id else ''}\n"
                    f"2. <b>Take-Profit Targets:</b>\n"
                    f"{tp_display}"
                )
                await send_telegram_alert(session, msg)

            is_sl_blown = (curr_price <= current_sl) if act == "LONG" else (curr_price >= current_sl)
            if is_sl_blown and not is_entry_touched and not is_order_filled:
                if entry_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, entry_order_id)
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET status = 'INVALIDATED', realized_pnl = 0.0, last_updated = CURRENT_TIMESTAMP WHERE id = ?", (w_id,))
                msg = (
                    f"🚨 <b>SETUP INVALIDATED (Never Filled)</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Reason:</b> Price directly hit invalidation level without filling entry.\n"
                    f"🤖 <b>Cancelled pending limit orders on Binance.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            continue

        # 3. STAGE 1+: ACTIVE POSITION RISK DEFENSE & DYNAMIC TP/SL MANAGEMENT
        if stage >= 1:
            created_at = parse_database_timestamp(item["timestamp"])
            holding_mins = (utc_now() - created_at).total_seconds() / 60.0
            pnl_approx = ((curr_price - entry) / entry * 100.0) if act == "LONG" else ((entry - curr_price) / entry * 100.0)

            if act == "LONG":
                peak_price = max(high_fav, curr_price)
                peak_gain_pct = (peak_price - entry) / entry * 100.0
                pullback_from_peak = (peak_price - curr_price) / peak_price * 100.0
            else:
                peak_price = min(high_fav, curr_price)
                peak_gain_pct = (entry - peak_price) / entry * 100.0
                pullback_from_peak = (curr_price - peak_price) / peak_price * 100.0

            updated_high_fav = peak_price

            # --- DYNAMIC REGIME TUNING ---
            coin_trend_str = str(item["coin_trend"] or "").upper()
            is_weekend = is_weekend_regime()
            is_sideways = ("NEUTRAL" in coin_trend_str or "CONSOLIDATION" in coin_trend_str or speed_tag == "SWEEP_REVERSION")

            if is_scalp_trade:
                # HIGH-SPEED SCALP SNIPER REGIME: Fast turnover, early cash harvesting
                regime_label = "⚡ HIGH-SPEED SCALP SNIPER"
                be_r_trigger = 0.5            # Milestone bank at +0.5R
                reversal_min_gain = 0.85      # Catch reversals starting at +0.85%
                reversal_pullback_threshold = 0.35
                stagnation_limit_mins = 35.0  # Rapid capital recycling (>35m stall is closed)
            elif is_weekend:
                # WEEKEND ANOMALY REGIME: Low CME liquidity & erratic manipulation
                regime_label = "WEEKEND ANOMALY"
                be_r_trigger = 0.5
                reversal_min_gain = 1.0
                reversal_pullback_threshold = 0.4
                stagnation_limit_mins = 60.0
            elif is_sideways:
                # SIDEWAYS / CHOP REGIME: Quick range-bound mean reversion
                regime_label = "SIDEWAYS CHOP"
                be_r_trigger = 0.6
                reversal_min_gain = 1.4
                reversal_pullback_threshold = 0.5
                stagnation_limit_mins = 90.0
            else:
                # TRENDING MOMENTUM REGIME (Weekday Institutional Flow):
                # Allow winners room to breathe so full TP1 (+1.2R) and TP2 (+2.0R) can be achieved
                regime_label = "TRENDING MOMENTUM"
                be_r_trigger = 1.0
                reversal_min_gain = 2.2
                reversal_pullback_threshold = 1.0
                stagnation_limit_mins = STAGNATION_MINS  # 180.0

            # RULE A: ADAPTIVE AUTO BREAK-EVEN & MILESTONE PROFIT SCALE-OUT
            risk_span = abs(entry - float(item["initial_sl"] or current_sl))
            if risk_span > 0:
                r_gain = (curr_price - entry) / risk_span if act == "LONG" else (entry - curr_price) / risk_span
                is_milestone_banked = ("BE_LOCKED" in tag_current or "BANKED" in tag_current or remaining_ratio < 0.90)
                if r_gain >= be_r_trigger and not is_milestone_banked:
                    be_sl = round(entry * 1.0015, 6) if act == "LONG" else round(entry * 0.9985, 6)
                    
                    # Scale out milestone: Scalps bank 50% immediately; Swings bank 33%
                    bank_ratio = 0.50 if is_scalp_trade else 0.33
                    bank_fraction = round(remaining_ratio * bank_ratio, 4)
                    banked_gain = round(bank_fraction * pnl_approx, 4)
                    new_partial_pnl = round(partial_realized_pnl + banked_gain, 4)
                    new_remaining_ratio = round(remaining_ratio - bank_fraction, 4)

                    new_sl_id = sl_order_id
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        bank_qty = executed_qty * bank_ratio
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, bank_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        rem_qty = max(0.0, executed_qty - bank_qty)
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, rem_qty, be_sl)
                    else:
                        rem_qty = executed_qty * (1.0 - bank_ratio) if executed_qty > 0 else 0.0

                    tag_current = f"🛡️ BE_LOCKED + {int(bank_ratio*100)}% BANKED"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET current_sl = ?, binance_sl_order_id = ?, executed_qty = ?, remaining_ratio = ?, partial_realized_pnl = ?, movement_tag = ? 
                            WHERE id = ?
                        """, (be_sl, new_sl_id, rem_qty, new_remaining_ratio, new_partial_pnl, tag_current, w_id))
                    
                    current_sl = be_sl
                    sl_order_id = new_sl_id
                    executed_qty = rem_qty
                    remaining_ratio = new_remaining_ratio
                    partial_realized_pnl = new_partial_pnl
                    msg = (
                        f"🛡️ <b>BREAK-EVEN & MILESTONE PROFIT SECURED (+{be_r_trigger:.1f}R [{regime_label}])</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f} (<code>{pnl_approx:+.2f}%</code>)\n"
                        f"• <b>Banked to Realized Cash:</b> <code>+{banked_gain:.2f}%</code> ({int(bank_ratio*100)}% scaled out)\n"
                        f"🤖 <b>Updated Stop-Loss to Break-Even: ${be_sl:.6f}</b>.\n"
                        f"• Trade is now completely risk-free with guaranteed cash gain."
                    )
                    await send_telegram_alert(session, msg)

            # RULE A.2: DYNAMIC TIERED PROFIT FLOOR RATCHET
            # As peak gain expands, ratchet current_sl up to lock in guaranteed profit floors
            target_floor_sl = None
            floor_label = None

            if is_scalp_trade:
                # High-speed scalp micro-floors
                if peak_gain_pct >= 2.5:
                    target_floor_sl = round(entry * 1.018, 6) if act == "LONG" else round(entry * 0.982, 6)
                    floor_label = "🔒 SCALP_FLOOR_+1.8%"
                elif peak_gain_pct >= 1.8:
                    target_floor_sl = round(entry * 1.012, 6) if act == "LONG" else round(entry * 0.988, 6)
                    floor_label = "🔒 SCALP_FLOOR_+1.2%"
                elif peak_gain_pct >= 1.2:
                    target_floor_sl = round(entry * 1.007, 6) if act == "LONG" else round(entry * 0.993, 6)
                    floor_label = "🔒 SCALP_FLOOR_+0.7%"
                elif peak_gain_pct >= 0.8:
                    target_floor_sl = round(entry * 1.0035, 6) if act == "LONG" else round(entry * 0.9965, 6)
                    floor_label = "🔒 SCALP_FLOOR_+0.35%"
            else:
                if peak_gain_pct >= 3.0:
                    target_floor_sl = round(entry * 1.020, 6) if act == "LONG" else round(entry * 0.980, 6)
                    floor_label = "🔒 PROFIT_FLOOR_+2.0%"
                elif peak_gain_pct >= 2.2:
                    target_floor_sl = round(entry * 1.012, 6) if act == "LONG" else round(entry * 0.988, 6)
                    floor_label = "🔒 PROFIT_FLOOR_+1.2%"
                elif peak_gain_pct >= 1.5:
                    target_floor_sl = round(entry * 1.006, 6) if act == "LONG" else round(entry * 0.994, 6)
                    floor_label = "🔒 PROFIT_FLOOR_+0.6%"

            if target_floor_sl is not None:
                is_better_sl = (target_floor_sl > current_sl) if act == "LONG" else (target_floor_sl < current_sl)
                if is_better_sl:
                    new_sl_id = sl_order_id
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, target_floor_sl)

                    tag_current = f"{tag_current} | {floor_label}" if ("BE_LOCKED" in tag_current or "BANKED" in tag_current) else floor_label
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET current_sl = ?, binance_sl_order_id = ?, movement_tag = ? 
                            WHERE id = ?
                        """, (target_floor_sl, new_sl_id, tag_current, w_id))
                    current_sl = target_floor_sl
                    sl_order_id = new_sl_id
                    msg = (
                        f"<b>{floor_label} (Peak: +{peak_gain_pct:.2f}%)</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f}\n"
                        f"🤖 <b>Ratcheted Stop-Loss to: ${current_sl:.6f}</b> (Locking guaranteed profit floor)."
                    )
                    await send_telegram_alert(session, msg)

            # RULE B: ADAPTIVE SUDDEN REVERSAL & PROFIT HARVEST DEFENSE
            is_sudden_reversal = (peak_gain_pct >= reversal_min_gain and pullback_from_peak >= reversal_pullback_threshold and pnl_approx > 0.3)
            is_peak_giveback = (peak_gain_pct >= 2.8 and pullback_from_peak >= 1.2)

            if (is_sudden_reversal or is_peak_giveback) and stage == 1 and "PEAK_EXIT" not in tag_current:
                if sl_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                exit_price = curr_price
                exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                final_pnl = round(partial_realized_pnl + (remaining_ratio * exit_pnl), 2)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, movement_tag = '🎯 REVERSAL_HARVEST', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (final_pnl, w_id))
                msg = (
                    f"💰 <b>PROFIT HARVEST: SUDDEN REVERSAL DEFENDED [{regime_label}]</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Peak Gain:</b> <code>+{peak_gain_pct:.2f}%</code>\n"
                    f"• <b>Secured Exit:</b> ${curr_price:.6f} (<code>{exit_pnl:+.2f}%</code>)\n"
                    f"• <b>Total Realized Trade PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                    f"• <b>Pullback from High:</b> <code>-{pullback_from_peak:.2f}%</code>\n"
                    f"🤖 <b>Closed on Binance to preserve profit before market flipped.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            # Fast Scalp Stagnation Kill (Recycle capital if scalp goes nowhere after 35m)
            if is_scalp_trade and stage == 1 and holding_mins >= stagnation_limit_mins and pnl_approx <= 0.20:
                if sl_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                exit_price = curr_price
                exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                final_pnl = round(partial_realized_pnl + (remaining_ratio * exit_pnl), 2)

                close_status = "COMPLETED" if final_pnl >= 0 else "STOPPED_OUT"
                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = ?, realized_pnl = ?, remaining_ratio = 0.0, movement_tag = '⚡ SCALP_STAGNATION_KILL', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (close_status, final_pnl, w_id))
                    conn.execute("""
                        INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                        VALUES (?, ?, ?, ?, ?, 'SCALP_STAGNATION', ?, 'SCALP_SNIPER', ?)
                    """, (sym, act, entry, exit_price, current_sl, holding_mins, f"Scalp stalled at {pnl_approx:+.2f}% after {holding_mins:.0f}m."))

                msg = (
                    f"⚡ <b>SCALP STAGNATION KILL ({holding_mins:.0f}m)</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Exit Mark:</b> ${exit_price:.6f} (<code>{exit_pnl:+.2f}%</code>)\n"
                    f"• <b>Total Realized Trade PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                    f"🤖 <b>Closed on Binance to recycle capital for fresh active movers.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            # Stagnation Defense: Adaptive time in chop (Standard Swings)
            if stage == 1 and holding_mins >= stagnation_limit_mins and pnl_approx <= STAGNATION_PNL_FLOOR and "STAGNANT" not in tag_current:
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET movement_tag = '⚠️ STAGNANT_WARNED' WHERE id = ?", (w_id,))
                    conn.execute("""
                        INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                        VALUES (?, ?, ?, ?, ?, 'STAGNATION', ?, ?, ?)
                    """, (sym, act, entry, curr_price, current_sl, holding_mins, regime_label, f"Stalled below {STAGNATION_PNL_FLOOR}% after {holding_mins:.0f}m."))
                tag_current = "⚠️ STAGNANT_WARNED"
                msg = (
                    f"⏳ <b>ADVISORY: STAGNANT TRADE (>{int(stagnation_limit_mins)}m [{regime_label}])</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Current Mark:</b> ${curr_price:.6f} (<code>{pnl_approx:+.2f}%</code>)\n"
                    f"• <b>Notice:</b> Momentum stalled. Capital may be freed if desired."
                )
                await send_telegram_alert(session, msg)

            is_sl_hit = (curr_price <= current_sl) if act == "LONG" else (curr_price >= current_sl)
            if is_sl_hit:
                if sl_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                # In SIMULATION mode, the resting stop order executes at current_sl
                # In LIVE/TESTNET mode, exchange execution fills at current_sl (with small slippage)
                exit_price = current_sl if TRADING_MODE == "SIMULATION" else curr_price
                exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                final_pnl = round(partial_realized_pnl + (remaining_ratio * exit_pnl), 2)

                cooldown_time = (utc_now() + datetime.timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
                close_status = "COMPLETED" if final_pnl >= 0 else "STOPPED_OUT"
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET status = ?, realized_pnl = ?, remaining_ratio = 0.0, last_updated = CURRENT_TIMESTAMP WHERE id = ?", (close_status, final_pnl, w_id))
                    if final_pnl < 0:
                        conn.execute("INSERT OR REPLACE INTO symbol_cooldowns (symbol, cooldown_until) VALUES (?, ?)", (sym, cooldown_time))
                        conn.execute("""
                            INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                            VALUES (?, ?, ?, ?, ?, 'STOP_LOSS', ?, 'UNCERTAIN', ?)
                        """, (sym, act, entry, exit_price, current_sl, holding_mins, f"Hit SL at ${current_sl:.6f}."))

                emoji = "🛑 <b>STOP-LOSS HIT ON BINANCE</b>" if final_pnl < 0 else "🛡️ <b>BREAK-EVEN / PROFIT STOP TRIGGERED</b>"
                msg = (
                    f"{emoji}\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Exit Price:</b> ${exit_price:.6f}\n"
                    f"• <b>Total Realized Result:</b> <code>{final_pnl:+.2f}%</code>\n"
                    f"🤖 <b>Position closed and orders cleaned up on Binance.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            # Strict non-cascading TP execution
            if stage == 1:
                hit_t1 = (curr_price >= t1) if act == "LONG" else (curr_price <= t1)
                if hit_t1:
                    stage = 2
                    be_sl = round(entry * 1.0015, 6) if act == "LONG" else round(entry * 0.9985, 6)
                    current_sl = be_sl
                    
                    # Close position on Binance: 60% for Scalp, 50% for Swing
                    tp1_close_ratio = 0.60 if is_scalp_trade else 0.50
                    closed_fraction = round(remaining_ratio * tp1_close_ratio, 4)
                    tp1_gain = round(closed_fraction * pnl_approx, 4)
                    new_partial_pnl = round(partial_realized_pnl + tp1_gain, 4)
                    new_remaining_ratio = round(remaining_ratio - closed_fraction, 4)

                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        close_qty = executed_qty * tp1_close_ratio
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, close_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        remaining_qty = max(0.0, executed_qty - close_qty)
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, remaining_qty, be_sl)
                    else:
                        remaining_qty = executed_qty * (1.0 - tp1_close_ratio) if executed_qty > 0 else 0.0
                        new_sl_id = sl_order_id

                    tag_current = "🎯 TP1_LOCKED"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET stage = 2, current_sl = ?, binance_sl_order_id = ?, executed_qty = ?, remaining_ratio = ?, partial_realized_pnl = ?, highest_favorable_price = ?, movement_tag = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (be_sl, new_sl_id, remaining_qty, new_remaining_ratio, new_partial_pnl, updated_high_fav, tag_current, w_id))
                    
                    executed_qty = remaining_qty
                    remaining_ratio = new_remaining_ratio
                    partial_realized_pnl = new_partial_pnl
                    sl_order_id = new_sl_id
                    tp1_badge = "(+1.0R SCALP TP1)" if is_scalp_trade else "(+1.2R)"
                    msg = (
                        f"🎯 <b>TP 1 HIT {tp1_badge} — {int(tp1_close_ratio*100)}% OF REMAINING LOCKED</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f}\n"
                        f"• <b>Banked on Leg:</b> <code>+{tp1_gain:.2f}%</code>\n\n"
                        f"🤖 <b>AUTOMATED ACTIONS EXECUTED:</b>\n"
                        f"1. <b>Closed {int(tp1_close_ratio*100)}% of position</b> on Binance.\n"
                        f"2. <b>Moved Stop-Loss to Break-Even: ${be_sl:.6f}</b>.\n"
                        f"• Trade is completely risk-free."
                    )
                    await send_telegram_alert(session, msg)
                    continue

            elif stage == 2:
                hit_t2 = (curr_price >= t2) if act == "LONG" else (curr_price <= t2)
                if hit_t2:
                    stage = 3
                    current_sl = t1
                    
                    # Close 60% of remaining position on Binance (leaving runner for TP3)
                    closed_fraction = round(remaining_ratio * 0.6, 4)
                    tp2_gain = round(closed_fraction * pnl_approx, 4)
                    new_partial_pnl = round(partial_realized_pnl + tp2_gain, 4)
                    new_remaining_ratio = round(remaining_ratio - closed_fraction, 4)

                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        tp2_qty = executed_qty * 0.6
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, tp2_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        remaining_qty = max(0.0, executed_qty - tp2_qty)
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, remaining_qty, t1)
                    else:
                        remaining_qty = executed_qty * 0.4 if executed_qty > 0 else 0.0
                        new_sl_id = sl_order_id

                    tag_current = "🎯🎯 TP2_LOCKED"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET stage = 3, current_sl = ?, binance_sl_order_id = ?, executed_qty = ?, remaining_ratio = ?, partial_realized_pnl = ?, highest_favorable_price = ?, movement_tag = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (t1, new_sl_id, remaining_qty, new_remaining_ratio, new_partial_pnl, updated_high_fav, tag_current, w_id))
                    
                    executed_qty = remaining_qty
                    remaining_ratio = new_remaining_ratio
                    partial_realized_pnl = new_partial_pnl
                    sl_order_id = new_sl_id
                    msg = (
                        f"🎯🎯 <b>TP 2 HIT (+2.0R) — 60% OF REMAINING LOCKED</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f}\n"
                        f"• <b>Banked on Leg:</b> <code>+{tp2_gain:.2f}%</code>\n\n"
                        f"🤖 <b>AUTOMATED ACTIONS EXECUTED:</b>\n"
                        f"1. <b>Closed 60% of position</b> on Binance.\n"
                        f"2. <b>Trailed Stop-Loss to TP1: ${t1:.6f}</b>."
                    )
                    await send_telegram_alert(session, msg)
                    continue

            elif stage == 3:
                hit_t3 = (curr_price >= t3) if act == "LONG" else (curr_price <= t3)
                if hit_t3:
                    if sl_order_id:
                        await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                    if TRADING_MODE in ("LIVE", "TESTNET"):
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                    final_pnl = round(partial_realized_pnl + (remaining_ratio * pnl_approx), 2)
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, movement_tag = '🏆 TP3_FULL', last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (final_pnl, w_id))
                    msg = (
                        f"🏆 <b>FULL TAKE-PROFIT ACHIEVED (+3.2R)!</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Final Mark:</b> ${curr_price:.6f}\n"
                        f"• <b>Total Net Trade Return:</b> <code>{final_pnl:+.2f}%</code>\n\n"
                        f"🤖 <b>Closed remaining position on Binance. Trade complete!</b>"
                    )
                    await send_telegram_alert(session, msg)
                    continue

            with get_db_connection() as conn:
                conn.execute("UPDATE watched_signals SET highest_favorable_price = ? WHERE id = ?", (updated_high_fav, w_id))

async def main():
    log_event("INFO", "SENTINEL", f"🚀 [{BOT_NAME}] Sentinel Active ({WATCH_INTERVAL_SECONDS}s Loop, Silent Radar).")
    async with aiohttp.ClientSession() as session:
        while True:
            if is_system_shutdown_requested():
                break
            try:
                await run_sentinel_cycle(session)
            except Exception as e:
                log_event("ERROR", "SENTINEL_LOOP", f"Sentinel cycle error: {e}")
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
