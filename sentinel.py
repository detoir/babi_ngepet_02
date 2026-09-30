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
        return datetime.datetime.utcnow()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.datetime.strptime(raw_ts.split("+")[0].split("Z")[0], fmt)
        except ValueError:
            pass
    return datetime.datetime.utcnow()

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
        entry_order_id = str(item["binance_entry_order_id"] or "")
        sl_order_id = str(item["binance_sl_order_id"] or "")
        high_fav = float(item["highest_favorable_price"] or entry if entry > 0 else 1.0)
        tag_current = item["movement_tag"] or ""
        speed_tag = item["trade_speed"] or "FAST TRADE"

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
            minutes_pending = (datetime.datetime.utcnow() - created_at).total_seconds() / 60.0

            # EXPIRATION WITH BINANCE CANCELLATION
            if minutes_pending >= (PENDING_TTL_HOURS * 60.0):
                if entry_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, entry_order_id)
                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = 'EXPIRED', realized_pnl = 0.0, movement_tag = '⏱️ EXPIRED_TTL', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (w_id,))

                speed_badge = "⚡ [FAST TRADE]" if speed_tag == "FAST TRADE" else "⏳ [SLOW TRADE]"
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
                if executed_qty <= 0:
                    executed_qty = format_order_quantity(sym, pos_size_usd, curr_price)

                # Automatically submit Hard Stop-Loss order on Binance
                new_sl_id = ""
                if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                    new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, current_sl)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET stage = 1, highest_favorable_price = ?, executed_qty = ?, binance_sl_order_id = ?, last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (entry, executed_qty, new_sl_id, w_id))

                speed_badge = "⚡ FAST TRADE" if speed_tag == "FAST TRADE" else "⏳ SLOW TRADE"
                exec_mode_tag = f"🤖 [AUTO-{TRADING_MODE}]" if TRADING_MODE in ("LIVE", "TESTNET") else "📋 [SIMULATION]"
                msg = (
                    f"🟢 <b>TRADE TRIGGERED / ENTRY FILLED ({speed_badge}) {exec_mode_tag}</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> (<code>{act}</code>)\n"
                    f"• <b>Fill Price:</b> <b>${curr_price:.6f}</b>\n"
                    f"• <b>Position Qty:</b> <code>{executed_qty} contracts</code> (${pos_size_usd:.0f} USD)\n\n"
                    f"📋 <b>ACTIVE RISK BRACKETS:</b>\n"
                    f"1. <b>Hard Stop-Loss:</b> <code>${current_sl:.6f}</code> {'(Active on Binance)' if new_sl_id else ''}\n"
                    f"2. <b>Take-Profit Targets:</b>\n"
                    f"   • TP1 (50% Size): <code>${t1:.6f}</code> (+1.2R)\n"
                    f"   • TP2 (30% Size): <code>${t2:.6f}</code> (+2.0R)\n"
                    f"   • TP3 (20% Size): <code>${t3:.6f}</code> (+3.2R)"
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
            holding_mins = (datetime.datetime.utcnow() - created_at).total_seconds() / 60.0
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

            # RULE A: AUTO BREAK-EVEN (At +0.6R, lock entry + fees)
            risk_span = abs(entry - float(item["initial_sl"] or current_sl))
            if risk_span > 0:
                r_gain = (curr_price - entry) / risk_span if act == "LONG" else (entry - curr_price) / risk_span
                if r_gain >= 0.6 and "BE_LOCKED" not in tag_current:
                    be_sl = round(entry * 1.0015, 6) if act == "LONG" else round(entry * 0.9985, 6)
                    
                    # Update Stop-Loss on Binance
                    new_sl_id = sl_order_id
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, be_sl)

                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET current_sl = ?, binance_sl_order_id = ?, movement_tag = '🛡️ BE_LOCKED' 
                            WHERE id = ?
                        """, (be_sl, new_sl_id, w_id))
                    current_sl = be_sl
                    sl_order_id = new_sl_id
                    tag_current = "🛡️ BE_LOCKED"
                    msg = (
                        f"🛡️ <b>BREAK-EVEN AUTO-LOCKED (+0.6R Reached)</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f} (<code>{pnl_approx:+.2f}%</code>)\n"
                        f"🤖 <b>Updated Binance Stop-Loss to: ${be_sl:.6f}</b>.\n"
                        f"• Trade is now completely risk-free."
                    )
                    await send_telegram_alert(session, msg)

            # RULE B: SUDDEN REVERSAL & PROFIT HARVEST DEFENSE
            # Protects profit if trade gained >= +1.2% and gives back >= 40% from peak, or hits standard peak giveback (>=2.5% gain & >=1.0% pullback)
            is_sudden_reversal = (peak_gain_pct >= 1.2 and pullback_from_peak >= (peak_gain_pct * 0.40) and pullback_from_peak >= 0.6 and pnl_approx > 0.3)
            is_peak_giveback = (peak_gain_pct >= 2.5 and pullback_from_peak >= 1.0)

            if (is_sudden_reversal or is_peak_giveback) and stage == 1 and "PEAK_EXIT" not in tag_current:
                if sl_order_id:
                    await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = 'COMPLETED', realized_pnl = ?, movement_tag = '🎯 REVERSAL_HARVEST', last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (pnl_approx, w_id))
                msg = (
                    f"💰 <b>PROFIT HARVEST: SUDDEN REVERSAL DEFENDED</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Peak Gain:</b> <code>+{peak_gain_pct:.2f}%</code>\n"
                    f"• <b>Secured Exit:</b> ${curr_price:.6f} (<code>{pnl_approx:+.2f}%</code>)\n"
                    f"• <b>Pullback from High:</b> <code>-{pullback_from_peak:.2f}%</code>\n"
                    f"🤖 <b>Closed on Binance to preserve profit before market flipped.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            # Stagnation Defense: 3 hours in chop
            if stage == 1 and holding_mins >= STAGNATION_MINS and pnl_approx <= STAGNATION_PNL_FLOOR and "STAGNANT" not in tag_current:
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET movement_tag = '⚠️ STAGNANT_WARNED' WHERE id = ?", (w_id,))
                    conn.execute("""
                        INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                        VALUES (?, ?, ?, ?, ?, 'STAGNATION', ?, 'CHOP', ?)
                    """, (sym, act, entry, curr_price, current_sl, holding_mins, f"Stalled below {STAGNATION_PNL_FLOOR}% after {holding_mins:.0f}m."))
                tag_current = "⚠️ STAGNANT_WARNED"
                msg = (
                    f"⏳ <b>ADVISORY: STAGNANT TRADE (>{int(STAGNATION_MINS/60)} Hours)</b>\n\n"
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

                cooldown_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
                with get_db_connection() as conn:
                    conn.execute("UPDATE watched_signals SET status = 'INVALIDATED', realized_pnl = ?, last_updated = CURRENT_TIMESTAMP WHERE id = ?", (pnl_approx, w_id))
                    if pnl_approx < 0:
                        conn.execute("INSERT OR REPLACE INTO symbol_cooldowns (symbol, cooldown_until) VALUES (?, ?)", (sym, cooldown_time))
                        conn.execute("""
                            INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                            VALUES (?, ?, ?, ?, ?, 'STOP_LOSS', ?, 'UNCERTAIN', ?)
                        """, (sym, act, entry, curr_price, current_sl, holding_mins, f"Hit SL at ${current_sl:.6f}."))

                emoji = "🛑 <b>STOP-LOSS HIT ON BINANCE</b>" if pnl_approx < 0 else "🛡️ <b>BREAK-EVEN STOP TRIGGERED</b>"
                msg = (
                    f"{emoji}\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Exit Price:</b> ${curr_price:.6f}\n"
                    f"• <b>Result:</b> <code>{pnl_approx:+.2f}%</code>\n"
                    f"🤖 <b>Position closed and orders cleaned up on Binance.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            # Strict non-cascading TP execution
            if stage == 1:
                hit_t1 = (curr_price >= t1) if act == "LONG" else (curr_price <= t1)
                if hit_t1:
                    stage = 2
                    be_sl = round(entry * 1.001, 6) if act == "LONG" else round(entry * 0.999, 6)
                    current_sl = be_sl
                    
                    # Close 50% on Binance
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        half_qty = executed_qty * 0.5
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, half_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        remaining_qty = executed_qty - half_qty
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, remaining_qty, be_sl)
                    else:
                        new_sl_id = sl_order_id

                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET stage = 2, current_sl = ?, binance_sl_order_id = ?, highest_favorable_price = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (be_sl, new_sl_id, updated_high_fav, w_id))
                    msg = (
                        f"🎯 <b>TP 1 HIT (+1.2R) — 50% PROFIT LOCKED</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f}\n\n"
                        f"🤖 <b>AUTOMATED ACTIONS EXECUTED:</b>\n"
                        f"1. <b>Closed 50% of position</b> on Binance.\n"
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
                    
                    # Close 30% on Binance
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        tp2_qty = executed_qty * 0.3
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, tp2_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        remaining_qty = executed_qty * 0.2
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, remaining_qty, t1)
                    else:
                        new_sl_id = sl_order_id

                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET stage = 3, current_sl = ?, binance_sl_order_id = ?, highest_favorable_price = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (t1, new_sl_id, updated_high_fav, w_id))
                    msg = (
                        f"🎯🎯 <b>TP 2 HIT (+2.0R) — 30% PROFIT LOCKED</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Current Mark:</b> ${curr_price:.6f}\n\n"
                        f"🤖 <b>AUTOMATED ACTIONS EXECUTED:</b>\n"
                        f"1. <b>Closed 30% of position</b> on Binance.\n"
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
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty * 0.2)

                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET status = 'COMPLETED', realized_pnl = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (pnl_approx, w_id))
                    msg = (
                        f"🏆 <b>FULL TAKE-PROFIT ACHIEVED (+3.2R)!</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Final Mark:</b> ${curr_price:.6f}\n"
                        f"• <b>Total Net Movement:</b> <code>{pnl_approx:+.2f}%</code>\n\n"
                        f"🤖 <b>Closed remaining 20% on Binance. Trade complete!</b>"
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
