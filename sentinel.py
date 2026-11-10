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
    get_public_exchange_instance,
    get_db_connection,
    log_event,
    bridge_symbol_formats,
    is_system_shutdown_requested,
    fetch_current_account_capital,
    place_binance_stop_loss,
    place_binance_take_profit,
    place_binance_tp_sl_brackets,
    split_tp_quantities,
    cancel_binance_order,
    emergency_binance_market_close,
    fetch_binance_order_status,
    fetch_live_binance_position,
    get_symbol_min_qty,
    safe_amount_to_precision,
    format_order_quantity,
    set_symbol_leverage_and_margin,
)

exchange = get_exchange_instance()
public_exchange = get_public_exchange_instance()
ROUND_TRIP_FEE_PCT = 0.10  # 0.05% entry + 0.05% exit Binance USD-M Futures fee allowance

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
        ticker = public_exchange.fetch_ticker(bridge["ccxt"])
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
        tp1_order_id = str(item_dict.get("binance_tp1_id", "") or "")
        tp2_order_id = str(item_dict.get("binance_tp2_id", "") or "")
        tp3_order_id = str(item_dict.get("binance_tp3_id", "") or "")
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
                for oid in (entry_order_id, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                    if oid:
                        await asyncio.to_thread(cancel_binance_order, sym, oid)
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
                # Only move SL to entry if current price is safely on the profitable side (>=0.20% cushion)
                tighter_sl = round(entry * 1.0015, 6) if act == "LONG" else round(entry * 0.9985, 6)
                is_better = (tighter_sl > current_sl) if act == "LONG" else (tighter_sl < current_sl)
                is_safe_side = (curr_price >= tighter_sl * 1.0018) if act == "LONG" else (curr_price <= tighter_sl * 0.9982)
                if is_better and is_safe_side:
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
                    sl_order_id = new_sl_id
                    msg = (
                        f"🛡️ <b>EMERGENCY ACTION: MOVE SL TO BREAK-EVEN</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Reason:</b> BTC Flash {btc_shock}.\n"
                        f"🤖 <b>Adjusted live Binance Stop-Loss to Break-Even: ${tighter_sl:.6f}.</b>"
                    )
                    await send_telegram_alert(session, msg)

        # 2. STAGE 0: PENDING LIMIT ORDER TRACKING & AUTO FILL
        if stage == 0:
            created_at = parse_database_timestamp(item["timestamp"])
            minutes_pending = (utc_now() - created_at).total_seconds() / 60.0
            pending_limit_mins = 45.0 if is_scalp_trade else (PENDING_TTL_HOURS * 60.0)

            # Check fill FIRST before expiring so a filled order is never wrongly marked EXPIRED
            is_entry_touched = (curr_price <= entry) if act == "LONG" else (curr_price >= entry)
            is_order_filled = False

            if TRADING_MODE in ("LIVE", "TESTNET") and entry_order_id and not entry_order_id.startswith("SIMULATED"):
                ord_status = await asyncio.to_thread(fetch_binance_order_status, sym, entry_order_id)
                st_code = ord_status.get("status", "UNKNOWN")
                if st_code == "CLOSED" or ord_status.get("filled", 0) > 0:
                    is_order_filled = True
                    if ord_status.get("filled", 0) > 0:
                        executed_qty = float(ord_status["filled"])
                    if ord_status.get("average", 0) > 0:
                        entry = float(ord_status["average"])
                elif st_code in ("CANCELED", "NOT_FOUND"):
                    # Verify if order filled into a live position or was cancelled/removed on exchange
                    pos_check = await asyncio.to_thread(fetch_live_binance_position, sym)
                    if pos_check.get("ok") and pos_check.get("contracts", 0.0) > 0:
                        is_order_filled = True
                        executed_qty = float(pos_check["contracts"])
                        if pos_check.get("entryPrice", 0.0) > 0:
                            entry = float(pos_check["entryPrice"])
                    elif pos_check.get("ok"):
                        for oid in (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                            if oid:
                                await asyncio.to_thread(cancel_binance_order, sym, oid)
                        with get_db_connection() as conn:
                            conn.execute("""
                                UPDATE watched_signals 
                                SET status = 'EXPIRED', realized_pnl = 0.0, movement_tag = '🚫 EXCHANGE_ORDER_GONE', last_updated = CURRENT_TIMESTAMP 
                                WHERE id = ?
                            """, (w_id,))
                        log_event("INFO", "SENTINEL", f"Pending limit order {entry_order_id} on {sym} no longer exists on Binance (0 position). Freed slot.")
                        continue
            elif TRADING_MODE == "SIMULATION":
                is_order_filled = is_entry_touched

            # EXPIRATION WITH BINANCE CANCELLATION (Only if not filled)
            if not is_order_filled and minutes_pending >= pending_limit_mins:
                for oid in (entry_order_id, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                    if oid:
                        await asyncio.to_thread(cancel_binance_order, sym, oid)
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

            if is_order_filled:
                stage = 1
                target_leverage = SCALP_LEVERAGE if is_scalp_trade else DEFAULT_LEVERAGE
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(set_symbol_leverage_and_margin, sym, target_leverage)

                if executed_qty <= 0:
                    executed_qty = format_order_quantity(sym, pos_size_usd, entry)

                # Ensure Hard Stop-Loss and Resting Limit TP1/TP2/TP3 are all placed on Binance
                new_sl_id = sl_order_id
                new_tp1_id = tp1_order_id
                new_tp2_id = tp2_order_id
                new_tp3_id = tp3_order_id
                if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                    tp1_q, tp2_q, tp3_q = split_tp_quantities(sym, executed_qty)
                    if not new_sl_id:
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, current_sl)
                    if not new_tp1_id and t1 > 0 and tp1_q > 0:
                        new_tp1_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp1_q, t1)
                    if not new_tp2_id and t2 > 0 and tp2_q > 0:
                        new_tp2_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp2_q, t2)
                    if not new_tp3_id and t3 > 0 and tp3_q > 0:
                        new_tp3_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp3_q, t3)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET stage = 1, entry_limit = ?, highest_favorable_price = ?, executed_qty = ?, remaining_ratio = 1.0, partial_realized_pnl = 0.0, binance_sl_order_id = ?, binance_tp1_id = ?, binance_tp2_id = ?, binance_tp3_id = ?, timestamp = CURRENT_TIMESTAMP, last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (entry, entry, executed_qty, new_sl_id, new_tp1_id, new_tp2_id, new_tp3_id, w_id))

                if is_scalp_trade:
                    speed_badge = "⚡ SCALP SNIPER"
                    tp_display = (
                        f"   • TP1 (30% Size): <code>${t1:.6f}</code> (+1.2R) {'✅' if new_tp1_id else ''}\n"
                        f"   • TP2 (35% Size): <code>${t2:.6f}</code> (+2.2R) {'✅' if new_tp2_id else ''}\n"
                        f"   • TP3 (35% Size): <code>${t3:.6f}</code> (+3.5R) {'✅' if new_tp3_id else ''}"
                    )
                else:
                    speed_badge = "🚀 FAST TRADE" if speed_tag == "FAST TRADE" else "⏳ SLOW TRADE"
                    tp_display = (
                        f"   • TP1 (30% Size): <code>${t1:.6f}</code> (+1.3R) {'✅' if new_tp1_id else ''}\n"
                        f"   • TP2 (35% Size): <code>${t2:.6f}</code> (+2.2R) {'✅' if new_tp2_id else ''}\n"
                        f"   • TP3 (35% Size): <code>${t3:.6f}</code> (+3.5R) {'✅' if new_tp3_id else ''}"
                    )

                exec_mode_tag = f"🤖 [AUTO-{TRADING_MODE}]" if TRADING_MODE in ("LIVE", "TESTNET") else "📋 [SIMULATION]"
                msg = (
                    f"🟢 <b>TRADE TRIGGERED / ENTRY FILLED ({speed_badge}) {exec_mode_tag}</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> (<code>{act}</code>)\n"
                    f"• <b>Fill Price:</b> <b>${entry:.6f}</b>\n"
                    f"• <b>Position Qty:</b> <code>{executed_qty} contracts</code> (${pos_size_usd:.0f} USD)\n\n"
                    f"📋 <b>ACTIVE RISK BRACKETS:</b>\n"
                    f"1. <b>Hard Stop-Loss:</b> <code>${current_sl:.6f}</code> {'(Active on Binance)' if new_sl_id else ''}\n"
                    f"2. <b>Take-Profit Targets:</b>\n"
                    f"{tp_display}"
                )
                await send_telegram_alert(session, msg)

            is_sl_blown = (curr_price <= current_sl) if act == "LONG" else (curr_price >= current_sl)
            if is_sl_blown and not is_order_filled:
                for oid in (entry_order_id, sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                    if oid:
                        await asyncio.to_thread(cancel_binance_order, sym, oid)
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

            # STEP 3.0: Verify if the native Binance Stop-Loss order ALREADY filled or position closed on exchange
            if TRADING_MODE in ("LIVE", "TESTNET") and sl_order_id and not sl_order_id.startswith("SIMULATED"):
                sl_ord_info = await asyncio.to_thread(fetch_binance_order_status, sym, sl_order_id)
                sl_status = sl_ord_info.get("status", "UNKNOWN")

                # If order returned NOT_FOUND (-2013) or CANCELED, check if the position is still open on Binance
                if sl_status in ("NOT_FOUND", "CANCELED"):
                    pos_info = await asyncio.to_thread(fetch_live_binance_position, sym)
                    if pos_info.get("ok"):
                        live_contracts = float(pos_info.get("contracts") or 0.0)
                        if live_contracts <= 0:
                            # Position is already closed on Binance! Reconcile immediately
                            sl_status = "CLOSED"
                        else:
                            # Position is still active on Binance, but SL order ID is stale/cancelled; re-place SL below
                            executed_qty = live_contracts
                            sl_order_id = ""

                if sl_status == "CLOSED":
                    # Native SL or external close triggered on Binance! Clean up any open TP orders
                    for tp_oid in (tp1_order_id, tp2_order_id, tp3_order_id):
                        if tp_oid:
                            await asyncio.to_thread(cancel_binance_order, sym, tp_oid)
                    exit_price = float(sl_ord_info.get("average") or current_sl)
                    raw_exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                    net_leg_pnl = raw_exit_pnl - ROUND_TRIP_FEE_PCT
                    final_pnl = round(partial_realized_pnl + (remaining_ratio * net_leg_pnl), 2)
                    cooldown_time = (utc_now() + datetime.timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
                    close_status = "COMPLETED" if final_pnl >= 0 else "STOPPED_OUT"
                    exit_reason_text = "BE_PROFIT_STOP" if final_pnl >= 0 else "HARD_STOP_LOSS"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET status = ?, realized_pnl = ?, remaining_ratio = 0.0, exit_reason = ?,
                                binance_sl_order_id = '', binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '',
                                last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (close_status, final_pnl, exit_reason_text, w_id))
                        if final_pnl < 0:
                            conn.execute("INSERT OR REPLACE INTO symbol_cooldowns (symbol, cooldown_until) VALUES (?, ?)", (sym, cooldown_time))
                            conn.execute("""
                                INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                                VALUES (?, ?, ?, ?, ?, 'STOP_LOSS', ?, 'LIVE_EXCHANGE_SL', ?)
                            """, (sym, act, entry, exit_price, current_sl, holding_mins, f"Exchange SL filled at ${exit_price:.6f} after {holding_mins:.0f}m ({final_pnl:+.2f}% net PnL)."))
                    log_event("INFO", "SENTINEL_EXIT", f"Trade {sym} {act} closed by exchange SL ({exit_reason_text}). Net Realized: {final_pnl:+.2f}%, Duration: {holding_mins:.0f}m")
                    emoji = "🛑 <b>STOP-LOSS FILLED ON BINANCE</b>" if final_pnl < 0 else "🛡️ <b>PROFIT / BREAK-EVEN STOP FILLED</b>"
                    msg = (
                        f"{emoji}\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Exit Price:</b> ${exit_price:.6f}\n"
                        f"• <b>Holding Time:</b> <code>{holding_mins:.0f} mins</code>\n"
                        f"• <b>Net Realized Result (after fees):</b> <code>{final_pnl:+.2f}%</code>\n"
                        f"🤖 <b>Position closed on Binance and resting TP orders cleaned up.</b>"
                    )
                    await send_telegram_alert(session, msg)
                    continue

            # SAFETY GUARD: Ensure every active live position has a resting Stop-Loss and TP order on Binance
            if TRADING_MODE in ("LIVE", "TESTNET") and current_sl > 0:
                if executed_qty <= 0:
                    pos_info = await asyncio.to_thread(fetch_live_binance_position, sym)
                    if pos_info.get("ok") and pos_info.get("contracts", 0.0) > 0:
                        executed_qty = float(pos_info["contracts"])
                    elif pos_info.get("ok") and pos_info.get("contracts", 0.0) <= 0:
                        # No live position exists on Binance for this stage >= 1 row
                        for oid in (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                            if oid:
                                await asyncio.to_thread(cancel_binance_order, sym, oid)
                        with get_db_connection() as conn_flat:
                            conn_flat.execute("""
                                UPDATE watched_signals
                                SET status = 'COMPLETED', remaining_ratio = 0.0, exit_reason = 'EXCHANGE_CLOSED_SYNC',
                                    binance_sl_order_id = '', binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '',
                                    last_updated = CURRENT_TIMESTAMP
                                WHERE id = ?
                            """, (w_id,))
                        log_event("INFO", "SENTINEL_SYNC", f"Reconciled flat Binance position on {sym}. Closed zombie WATCHING row.")
                        continue
                    if executed_qty <= 0:
                        executed_qty = format_order_quantity(sym, pos_size_usd, curr_price)

                if executed_qty > 0 and (not sl_order_id or sl_order_id.startswith("SIMULATED")):
                    log_event("WARN", "SL_SAFETY_GUARD", f"Missing on-exchange SL detected on {sym} ({act}). Placing Stop-Loss at ${current_sl:.6f} for {executed_qty} contracts...")
                    recovered_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, current_sl)
                    if recovered_sl_id:
                        sl_order_id = recovered_sl_id
                        with get_db_connection() as conn_sl:
                            conn_sl.execute("UPDATE watched_signals SET binance_sl_order_id = ?, executed_qty = ? WHERE id = ?", (recovered_sl_id, executed_qty, w_id))
                        log_event("INFO", "SL_SAFETY_GUARD", f"Recovered and bound live Stop-Loss {recovered_sl_id} for {sym}")

                if executed_qty > 0 and stage == 1:
                    tp1_q, tp2_q, tp3_q = split_tp_quantities(sym, executed_qty)
                    if (not tp1_order_id or tp1_order_id.startswith("SIMULATED")) and t1 > 0 and tp1_q > 0:
                        recovered_tp1_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp1_q, t1)
                        if recovered_tp1_id:
                            tp1_order_id = recovered_tp1_id
                            with get_db_connection() as conn_tp:
                                conn_tp.execute("UPDATE watched_signals SET binance_tp1_id = ? WHERE id = ?", (recovered_tp1_id, w_id))
                    if (not tp2_order_id or tp2_order_id.startswith("SIMULATED")) and t2 > 0 and tp2_q > 0:
                        recovered_tp2_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp2_q, t2)
                        if recovered_tp2_id:
                            tp2_order_id = recovered_tp2_id
                            with get_db_connection() as conn_tp2:
                                conn_tp2.execute("UPDATE watched_signals SET binance_tp2_id = ? WHERE id = ?", (recovered_tp2_id, w_id))
                    if (not tp3_order_id or tp3_order_id.startswith("SIMULATED")) and t3 > 0 and tp3_q > 0:
                        recovered_tp3_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp3_q, t3)
                        if recovered_tp3_id:
                            tp3_order_id = recovered_tp3_id
                            with get_db_connection() as conn_tp3:
                                conn_tp3.execute("UPDATE watched_signals SET binance_tp3_id = ? WHERE id = ?", (recovered_tp3_id, w_id))
                elif executed_qty > 0 and stage == 2 and (not tp2_order_id or tp2_order_id.startswith("SIMULATED")) and t2 > 0:
                    min_lot = get_symbol_min_qty(sym)
                    tp2_q = executed_qty if (executed_qty * 0.50) < min_lot else (executed_qty * 0.50)
                    recovered_tp2_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp2_q, t2)
                    if recovered_tp2_id:
                        tp2_order_id = recovered_tp2_id
                        with get_db_connection() as conn_tp2:
                            conn_tp2.execute("UPDATE watched_signals SET binance_tp2_id = ? WHERE id = ?", (recovered_tp2_id, w_id))
                elif executed_qty > 0 and stage == 3 and (not tp3_order_id or tp3_order_id.startswith("SIMULATED")) and t3 > 0:
                    recovered_tp3_id = await asyncio.to_thread(place_binance_take_profit, sym, act, executed_qty, t3)
                    if recovered_tp3_id:
                        tp3_order_id = recovered_tp3_id
                        with get_db_connection() as conn_tp3:
                            conn_tp3.execute("UPDATE watched_signals SET binance_tp3_id = ? WHERE id = ?", (recovered_tp3_id, w_id))

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
                regime_label = "⚡ HIGH-SPEED SCALP SNIPER"
                reversal_min_gain = 1.35      # Allow trade to reach TP1 (+1.2R) before reversal harvest engages
                reversal_pullback_threshold = 0.55  # Harvest if pullback exceeds 0.55% from peak
                stagnation_limit_mins = 120.0
            elif is_weekend:
                regime_label = "WEEKEND ANOMALY"
                reversal_min_gain = 1.25
                reversal_pullback_threshold = 0.55
                stagnation_limit_mins = 90.0
            elif is_sideways:
                regime_label = "SIDEWAYS CHOP"
                reversal_min_gain = 1.30
                reversal_pullback_threshold = 0.60
                stagnation_limit_mins = 120.0
            else:
                regime_label = "TRENDING MOMENTUM"
                reversal_min_gain = 1.50
                reversal_pullback_threshold = 0.70
                stagnation_limit_mins = STAGNATION_MINS  # 180.0

            # ------------------------------------------------------------------
            # STEP 3.1: TAKE-PROFIT EXECUTION (Check Resting Binance TP Orders & Targets FIRST)
            # Scale out: 30% at TP1 -> 35% at TP2 -> 35% at TP3 (or 100% at TP1 if position is minimum lot)
            # ------------------------------------------------------------------
            if stage == 1:
                tp1_order_filled = False
                tp1_fill_price = curr_price
                if TRADING_MODE in ("LIVE", "TESTNET") and tp1_order_id and not tp1_order_id.startswith("SIMULATED"):
                    tp1_info = await asyncio.to_thread(fetch_binance_order_status, sym, tp1_order_id)
                    if tp1_info.get("status") == "CLOSED":
                        tp1_order_filled = True
                        if tp1_info.get("average", 0) > 0:
                            tp1_fill_price = float(tp1_info["average"])

                hit_t1 = tp1_order_filled or ((curr_price >= t1) if act == "LONG" else (curr_price <= t1))
                if hit_t1:
                    min_lot = get_symbol_min_qty(sym)
                    tp1_q_est, tp2_q_est, _ = split_tp_quantities(sym, executed_qty)
                    is_single_min_lot = (executed_qty > 0 and tp2_q_est <= 0.0)
                    tp1_close_ratio = 1.0 if is_single_min_lot else 0.30

                    raw_leg_pnl = ((tp1_fill_price - entry) / entry * 100.0) if act == "LONG" else ((entry - tp1_fill_price) / entry * 100.0)
                    net_leg_pnl = raw_leg_pnl - ROUND_TRIP_FEE_PCT
                    closed_fraction = round(remaining_ratio * tp1_close_ratio, 4)
                    tp1_gain = round(closed_fraction * net_leg_pnl, 4)
                    new_partial_pnl = round(partial_realized_pnl + tp1_gain, 4)
                    new_remaining_ratio = round(max(0.0, remaining_ratio - closed_fraction), 4)

                    if is_single_min_lot or new_remaining_ratio <= 0.0:
                        # Entire min-lot position closed at TP1
                        for oid in (sl_order_id, tp2_order_id, tp3_order_id):
                            if oid:
                                await asyncio.to_thread(cancel_binance_order, sym, oid)
                        if TRADING_MODE in ("LIVE", "TESTNET"):
                            if not tp1_order_filled and tp1_order_id:
                                await asyncio.to_thread(cancel_binance_order, sym, tp1_order_id)
                            await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)
                        final_pnl = round(new_partial_pnl, 2)
                        with get_db_connection() as conn:
                            conn.execute("""
                                UPDATE watched_signals 
                                SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '', binance_sl_order_id = '', exit_reason = 'TP1_FULL_TARGET', movement_tag = '🎯 TP1_COMPLETE', last_updated = CURRENT_TIMESTAMP 
                                WHERE id = ?
                            """, (final_pnl, w_id))
                        msg = (
                            f"🎯 <b>TP 1 HIT — FULL MIN-LOT POSITION BANKED</b>\n\n"
                            f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                            f"• <b>Fill Price:</b> ${tp1_fill_price:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                            f"• <b>Total Net Realized PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                            f"🤖 <b>Position closed at TP1 (single minimum lot contract).</b>"
                        )
                        await send_telegram_alert(session, msg)
                        continue

                    stage = 2
                    # Move SL to Break-Even + 0.30% (covers 0.10% fees + 0.20% net profit floor on runner)
                    be_sl = round(entry * 1.0030, 6) if act == "LONG" else round(entry * 0.9970, 6)
                    new_sl_target = max(current_sl, be_sl) if act == "LONG" else min(current_sl, be_sl)

                    new_tp2_id = tp2_order_id
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        close_qty = float(tp1_info.get("filled") or 0.0) if (tp1_order_filled and float(tp1_info.get("filled") or 0.0) > 0) else (tp1_q_est if tp1_q_est > 0 else (executed_qty * tp1_close_ratio))
                        if not tp1_order_filled:
                            if tp1_order_id:
                                await asyncio.to_thread(cancel_binance_order, sym, tp1_order_id)
                            await asyncio.to_thread(emergency_binance_market_close, sym, act, close_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        remaining_qty = max(0.0, round(executed_qty - close_qty, 8))
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, remaining_qty, new_sl_target)
                        if not new_tp2_id:
                            tp2_qty = remaining_qty if (remaining_qty * 0.50) < min_lot else (remaining_qty * 0.50)
                            new_tp2_id = await asyncio.to_thread(place_binance_take_profit, sym, act, tp2_qty, t2)
                    else:
                        remaining_qty = executed_qty * (1.0 - tp1_close_ratio) if executed_qty > 0 else 0.0
                        new_sl_id = sl_order_id

                    current_sl = new_sl_target
                    tag_current = "🎯 TP1_LOCKED"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET stage = 2, current_sl = ?, binance_sl_order_id = ?, binance_tp1_id = '', binance_tp2_id = ?, executed_qty = ?, remaining_ratio = ?, partial_realized_pnl = ?, highest_favorable_price = ?, movement_tag = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (current_sl, new_sl_id, new_tp2_id, remaining_qty, new_remaining_ratio, new_partial_pnl, updated_high_fav, tag_current, w_id))

                    executed_qty = remaining_qty
                    remaining_ratio = new_remaining_ratio
                    partial_realized_pnl = new_partial_pnl
                    sl_order_id = new_sl_id
                    tp1_order_id = ""
                    tp2_order_id = new_tp2_id
                    tp1_badge = "(+1.2R SCALP TP1)" if is_scalp_trade else "(+1.3R)"
                    msg = (
                        f"🎯 <b>TP 1 HIT {tp1_badge} — 30% POSITION BANKED</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Fill Price:</b> ${tp1_fill_price:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                        f"• <b>Banked on Leg:</b> <code>+{tp1_gain:.2f}%</code>\n\n"
                        f"🤖 <b>AUTOMATED ACTIONS EXECUTED:</b>\n"
                        f"1. <b>Closed 30% of position</b> on Binance.\n"
                        f"2. <b>Locked Stop-Loss at: ${current_sl:.6f}</b>.\n"
                        f"3. <b>Active Limit TP2 at: ${t2:.6f}</b>."
                    )
                    await send_telegram_alert(session, msg)
                    continue

            elif stage == 2:
                tp2_order_filled = False
                tp2_fill_price = curr_price
                if TRADING_MODE in ("LIVE", "TESTNET") and tp2_order_id and not tp2_order_id.startswith("SIMULATED"):
                    tp2_info = await asyncio.to_thread(fetch_binance_order_status, sym, tp2_order_id)
                    if tp2_info.get("status") == "CLOSED":
                        tp2_order_filled = True
                        if tp2_info.get("average", 0) > 0:
                            tp2_fill_price = float(tp2_info["average"])

                hit_t2 = tp2_order_filled or ((curr_price >= t2) if act == "LONG" else (curr_price <= t2))
                if hit_t2:
                    min_lot = get_symbol_min_qty(sym)
                    is_single_min_lot = (executed_qty > 0 and (executed_qty * 0.50) < min_lot)
                    tp2_close_ratio = 1.0 if is_single_min_lot else 0.50

                    raw_leg_pnl = ((tp2_fill_price - entry) / entry * 100.0) if act == "LONG" else ((entry - tp2_fill_price) / entry * 100.0)
                    net_leg_pnl = raw_leg_pnl - ROUND_TRIP_FEE_PCT
                    closed_fraction = round(remaining_ratio * tp2_close_ratio, 4)
                    tp2_gain = round(closed_fraction * net_leg_pnl, 4)
                    new_partial_pnl = round(partial_realized_pnl + tp2_gain, 4)
                    new_remaining_ratio = round(max(0.0, remaining_ratio - closed_fraction), 4)

                    if is_single_min_lot or new_remaining_ratio <= 0.0:
                        for oid in (sl_order_id, tp1_order_id, tp3_order_id):
                            if oid:
                                await asyncio.to_thread(cancel_binance_order, sym, oid)
                        if TRADING_MODE in ("LIVE", "TESTNET"):
                            if not tp2_order_filled and tp2_order_id:
                                await asyncio.to_thread(cancel_binance_order, sym, tp2_order_id)
                            await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)
                        final_pnl = round(new_partial_pnl, 2)
                        with get_db_connection() as conn:
                            conn.execute("""
                                UPDATE watched_signals 
                                SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, binance_tp2_id = '', binance_tp3_id = '', binance_sl_order_id = '', exit_reason = 'TP2_FULL_TARGET', movement_tag = '🎯🎯 TP2_COMPLETE', last_updated = CURRENT_TIMESTAMP 
                                WHERE id = ?
                            """, (final_pnl, w_id))
                        msg = (
                            f"🎯🎯 <b>TP 2 HIT — POSITION COMPLETE!</b>\n\n"
                            f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                            f"• <b>Fill Price:</b> ${tp2_fill_price:.6f}\n"
                            f"• <b>Total Net Realized PnL:</b> <code>{final_pnl:+.2f}%</code>"
                        )
                        await send_telegram_alert(session, msg)
                        continue

                    stage = 3
                    new_sl_target = max(current_sl, t1) if act == "LONG" else min(current_sl, t1)

                    new_tp3_id = tp3_order_id
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        tp2_qty = float(tp2_info.get("filled") or 0.0) if (tp2_order_filled and float(tp2_info.get("filled") or 0.0) > 0) else safe_amount_to_precision(sym, executed_qty * 0.50)
                        if not tp2_order_filled:
                            if tp2_order_id:
                                await asyncio.to_thread(cancel_binance_order, sym, tp2_order_id)
                            await asyncio.to_thread(emergency_binance_market_close, sym, act, tp2_qty)
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        remaining_qty = max(0.0, round(executed_qty - tp2_qty, 8))
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, remaining_qty, new_sl_target)
                        if not new_tp3_id:
                            new_tp3_id = await asyncio.to_thread(place_binance_take_profit, sym, act, remaining_qty, t3)
                    else:
                        remaining_qty = executed_qty * 0.50 if executed_qty > 0 else 0.0
                        new_sl_id = sl_order_id

                    current_sl = new_sl_target
                    tag_current = "🎯🎯 TP2_LOCKED"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET stage = 3, current_sl = ?, binance_sl_order_id = ?, binance_tp2_id = '', binance_tp3_id = ?, executed_qty = ?, remaining_ratio = ?, partial_realized_pnl = ?, highest_favorable_price = ?, movement_tag = ?, last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (current_sl, new_sl_id, new_tp3_id, remaining_qty, new_remaining_ratio, new_partial_pnl, updated_high_fav, tag_current, w_id))

                    executed_qty = remaining_qty
                    remaining_ratio = new_remaining_ratio
                    partial_realized_pnl = new_partial_pnl
                    sl_order_id = new_sl_id
                    tp2_order_id = ""
                    tp3_order_id = new_tp3_id
                    msg = (
                        f"🎯🎯 <b>TP 2 HIT (+2.2R) — 65% TOTAL POSITION BANKED</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Fill Price:</b> ${tp2_fill_price:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                        f"• <b>Banked on Leg:</b> <code>+{tp2_gain:.2f}%</code>\n\n"
                        f"🤖 <b>AUTOMATED ACTIONS EXECUTED:</b>\n"
                        f"1. <b>Closed 50% of remaining</b> on Binance.\n"
                        f"2. <b>Trailed Stop-Loss to TP1: ${current_sl:.6f}</b>.\n"
                        f"3. <b>Active Limit TP3 at: ${t3:.6f}</b>."
                    )
                    await send_telegram_alert(session, msg)
                    continue

            elif stage == 3:
                tp3_order_filled = False
                tp3_fill_price = curr_price
                if TRADING_MODE in ("LIVE", "TESTNET") and tp3_order_id and not tp3_order_id.startswith("SIMULATED"):
                    tp3_info = await asyncio.to_thread(fetch_binance_order_status, sym, tp3_order_id)
                    if tp3_info.get("status") == "CLOSED":
                        tp3_order_filled = True
                        if tp3_info.get("average", 0) > 0:
                            tp3_fill_price = float(tp3_info["average"])

                hit_t3 = tp3_order_filled or ((curr_price >= t3) if act == "LONG" else (curr_price <= t3))
                if hit_t3:
                    if sl_order_id:
                        await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                    if TRADING_MODE in ("LIVE", "TESTNET"):
                        if not tp3_order_filled and tp3_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, tp3_order_id)
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                    raw_leg_pnl = ((tp3_fill_price - entry) / entry * 100.0) if act == "LONG" else ((entry - tp3_fill_price) / entry * 100.0)
                    net_leg_pnl = raw_leg_pnl - ROUND_TRIP_FEE_PCT
                    final_pnl = round(partial_realized_pnl + (remaining_ratio * net_leg_pnl), 2)
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, binance_tp3_id = '', binance_sl_order_id = '', exit_reason = 'TP3_FULL_TARGET', movement_tag = '🏆 TP3_FULL', last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (final_pnl, w_id))
                    msg = (
                        f"🏆 <b>FULL TAKE-PROFIT ACHIEVED (+3.5R)!</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Final Fill:</b> ${tp3_fill_price:.6f}\n"
                        f"• <b>Total Net Trade Return:</b> <code>{final_pnl:+.2f}%</code>\n\n"
                        f"🤖 <b>Closed final runner on Binance. Trade complete!</b>"
                    )
                    await send_telegram_alert(session, msg)
                    continue

            # ------------------------------------------------------------------
            # STEP 3.2: DYNAMIC TIERED PROFIT FLOOR RATCHET (RUNNING STOP-LOSS)
            # Early Break-Even Shield: Protect early winning positions (+0.55% scalp / +0.70% swing)
            # so a trade that starts winning NEVER turns into a full stop-loss!
            # ------------------------------------------------------------------
            target_floor_sl = None
            floor_label = None

            if is_scalp_trade:
                if peak_gain_pct >= 2.40 and pnl_approx >= 1.85:
                    target_floor_sl = round(entry * 1.0145, 6) if act == "LONG" else round(entry * 0.9855, 6)
                    floor_label = "🔒 SCALP_FLOOR_+1.45%"
                elif peak_gain_pct >= 1.75 and pnl_approx >= 1.30:
                    target_floor_sl = round(entry * 1.0090, 6) if act == "LONG" else round(entry * 0.9910, 6)
                    floor_label = "🔒 SCALP_FLOOR_+0.90%"
                elif peak_gain_pct >= 1.25 and pnl_approx >= 0.85:
                    # Give trade full room to hit TP1 (+1.2R); only ratchet once solidly past +1.25% peak
                    target_floor_sl = round(entry * 1.0040, 6) if act == "LONG" else round(entry * 0.9960, 6)
                    floor_label = "🛡️ SCALP_PROFIT_SHIELD_+0.40%"
            else:
                if peak_gain_pct >= 2.40 and pnl_approx >= 1.90:
                    target_floor_sl = round(entry * 1.0155, 6) if act == "LONG" else round(entry * 0.9845, 6)
                    floor_label = "🔒 PROFIT_FLOOR_+1.55%"
                elif peak_gain_pct >= 1.70 and pnl_approx >= 1.30:
                    target_floor_sl = round(entry * 1.0095, 6) if act == "LONG" else round(entry * 0.9905, 6)
                    floor_label = "🔒 PROFIT_FLOOR_+0.95%"
                elif peak_gain_pct >= 1.15 and pnl_approx >= 0.85:
                    target_floor_sl = round(entry * 1.0040, 6) if act == "LONG" else round(entry * 0.9960, 6)
                    floor_label = "🛡️ PROFIT_BE_SHIELD_+0.40%"

            if target_floor_sl is not None:
                is_better_sl = (target_floor_sl > current_sl) if act == "LONG" else (target_floor_sl < current_sl)
                has_safe_buffer = (curr_price >= target_floor_sl * 1.0015) if act == "LONG" else (curr_price <= target_floor_sl * 0.9985)
                if is_better_sl and has_safe_buffer:
                    new_sl_id = sl_order_id
                    if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                        if sl_order_id:
                            await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                        new_sl_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, target_floor_sl)

                    tag_current = f"{tag_current} | {floor_label}" if ("TP" in tag_current or "LOCKED" in tag_current) else floor_label
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
                        f"• <b>Current Mark:</b> ${curr_price:.6f} (<code>{pnl_approx:+.2f}%</code>)\n"
                        f"🤖 <b>Ratcheted Stop-Loss to: ${current_sl:.6f}</b> (Locking guaranteed profit floor)."
                    )
                    await send_telegram_alert(session, msg)

            # ------------------------------------------------------------------
            # STEP 3.3: ADAPTIVE SUDDEN REVERSAL & PROFIT HARVEST DEFENSE
            # ------------------------------------------------------------------
            min_harvest_pnl = 0.55 if is_scalp_trade else 0.55
            is_sudden_reversal = (peak_gain_pct >= reversal_min_gain and pullback_from_peak >= reversal_pullback_threshold and pnl_approx >= min_harvest_pnl)
            is_peak_giveback = (peak_gain_pct >= 2.0 and pullback_from_peak >= 0.90 and pnl_approx >= 0.55)

            if (is_sudden_reversal or is_peak_giveback) and stage == 1 and "PEAK_EXIT" not in tag_current:
                for oid in (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                    if oid:
                        await asyncio.to_thread(cancel_binance_order, sym, oid)
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                exit_price = curr_price
                raw_exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                net_leg_pnl = raw_exit_pnl - ROUND_TRIP_FEE_PCT
                final_pnl = round(partial_realized_pnl + (remaining_ratio * net_leg_pnl), 2)

                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = 'COMPLETED', realized_pnl = ?, remaining_ratio = 0.0, exit_reason = 'REVERSAL_HARVEST', movement_tag = '🎯 REVERSAL_HARVEST',
                            binance_sl_order_id = '', binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '',
                            last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (final_pnl, w_id))
                log_event("INFO", "SENTINEL_EXIT", f"Trade {sym} {act} closed via REVERSAL_HARVEST at {final_pnl:+.2f}% net after {holding_mins:.0f}m")
                msg = (
                    f"💰 <b>PROFIT HARVEST: SUDDEN REVERSAL DEFENDED [{regime_label}]</b>\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Peak Gain:</b> <code>+{peak_gain_pct:.2f}%</code>\n"
                    f"• <b>Secured Exit:</b> ${curr_price:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                    f"• <b>Total Realized Trade PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                    f"• <b>Pullback from High:</b> <code>-{pullback_from_peak:.2f}%</code>\n"
                    f"🤖 <b>Closed on Binance to preserve profit before market flipped.</b>"
                )
                await send_telegram_alert(session, msg)
                continue

            # ------------------------------------------------------------------
            # STEP 3.4: STAGNATION DEFENSE (Safe Buffer Enforced)
            # ------------------------------------------------------------------
            if is_scalp_trade and stage == 1 and holding_mins >= stagnation_limit_mins:
                if pnl_approx >= 0.75 and "STAGNANT" not in tag_current:
                    # Stagnant but safely green (>= +0.75%): Lock SL to Break-Even + Profit (+0.30%) with >=0.45% buffer
                    be_safe_sl = round(entry * 1.0030, 6) if act == "LONG" else round(entry * 0.9970, 6)
                    is_better_sl = (be_safe_sl > current_sl) if act == "LONG" else (be_safe_sl < current_sl)
                    has_safe_buffer = (curr_price >= be_safe_sl * 1.0045) if act == "LONG" else (curr_price <= be_safe_sl * 0.9955)
                    if is_better_sl and has_safe_buffer:
                        if TRADING_MODE in ("LIVE", "TESTNET") and executed_qty > 0:
                            if sl_order_id:
                                await asyncio.to_thread(cancel_binance_order, sym, sl_order_id)
                            sl_order_id = await asyncio.to_thread(place_binance_stop_loss, sym, act, executed_qty, be_safe_sl)
                        current_sl = be_safe_sl
                        tag_current = "⏳ STAGNANT_BE_LOCKED"
                        with get_db_connection() as conn:
                            conn.execute("UPDATE watched_signals SET current_sl = ?, binance_sl_order_id = ?, movement_tag = ? WHERE id = ?", (current_sl, sl_order_id, tag_current, w_id))
                        log_event("INFO", "SENTINEL_DEFENSE", f"Stagnant scalp {sym} ({pnl_approx:+.2f}% after {holding_mins:.0f}m) secured at BE Stop-Loss ${current_sl:.6f}")
                elif holding_mins >= 480.0 and abs(pnl_approx) <= 0.15:
                    # Flat tape stagnation (> 8 hours near break-even): Safely recycle capital at scratch without taking an arbitrary forced loss
                    for oid in (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                        if oid:
                            await asyncio.to_thread(cancel_binance_order, sym, oid)
                    if TRADING_MODE in ("LIVE", "TESTNET"):
                        await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                    exit_price = curr_price
                    raw_exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                    net_leg_pnl = raw_exit_pnl - ROUND_TRIP_FEE_PCT
                    final_pnl = round(partial_realized_pnl + (remaining_ratio * net_leg_pnl), 2)

                    close_status = "COMPLETED" if final_pnl >= 0 else "STOPPED_OUT"
                    with get_db_connection() as conn:
                        conn.execute("""
                            UPDATE watched_signals 
                            SET status = ?, realized_pnl = ?, remaining_ratio = 0.0, exit_reason = 'SCALP_STAGNATION_KILL', movement_tag = '⚡ SCALP_STAGNATION_KILL',
                                binance_sl_order_id = '', binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '',
                                last_updated = CURRENT_TIMESTAMP 
                            WHERE id = ?
                        """, (close_status, final_pnl, w_id))
                        conn.execute("""
                            INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                            VALUES (?, ?, ?, ?, ?, 'SCALP_STAGNATION', ?, 'SCALP_SNIPER', ?)
                        """, (sym, act, entry, exit_price, current_sl, holding_mins, f"Scalp stalled at scratch {pnl_approx:+.2f}% after {holding_mins:.0f}m."))

                    log_event("INFO", "SENTINEL_EXIT", f"Trade {sym} {act} closed via SCALP_STAGNATION_KILL at {final_pnl:+.2f}% net after {holding_mins:.0f}m")

                    msg = (
                        f"⚡ <b>SCALP FLAT STAGNATION RECYCLE ({holding_mins:.0f}m)</b>\n\n"
                        f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                        f"• <b>Exit Mark:</b> ${exit_price:.6f} (<code>{net_leg_pnl:+.2f}% net</code>)\n"
                        f"• <b>Total Realized Trade PnL:</b> <code>{final_pnl:+.2f}%</code>\n"
                        f"🤖 <b>Flat stalled trade recycled after 8+ hours.</b>"
                    )
                    await send_telegram_alert(session, msg)
                    continue

            # Stagnation Defense: Advisory for Standard Swings
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

            # ------------------------------------------------------------------
            # STEP 3.5: STOP-LOSS HIT CHECK
            # ------------------------------------------------------------------
            is_sl_hit = (curr_price <= current_sl) if act == "LONG" else (curr_price >= current_sl)
            if is_sl_hit:
                for oid in (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id):
                    if oid:
                        await asyncio.to_thread(cancel_binance_order, sym, oid)
                if TRADING_MODE in ("LIVE", "TESTNET"):
                    await asyncio.to_thread(emergency_binance_market_close, sym, act, executed_qty)

                exit_price = current_sl if TRADING_MODE == "SIMULATION" else curr_price
                raw_exit_pnl = ((exit_price - entry) / entry * 100.0) if act == "LONG" else ((entry - exit_price) / entry * 100.0)
                net_leg_pnl = raw_exit_pnl - ROUND_TRIP_FEE_PCT
                final_pnl = round(partial_realized_pnl + (remaining_ratio * net_leg_pnl), 2)

                cooldown_time = (utc_now() + datetime.timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
                close_status = "COMPLETED" if final_pnl >= 0 else "STOPPED_OUT"
                exit_reason_text = "BE_PROFIT_STOP" if final_pnl >= 0 else "HARD_STOP_LOSS"
                with get_db_connection() as conn:
                    conn.execute("""
                        UPDATE watched_signals 
                        SET status = ?, realized_pnl = ?, remaining_ratio = 0.0, exit_reason = ?,
                            binance_sl_order_id = '', binance_tp1_id = '', binance_tp2_id = '', binance_tp3_id = '',
                            last_updated = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (close_status, final_pnl, exit_reason_text, w_id))
                    if final_pnl < 0:
                        conn.execute("INSERT OR REPLACE INTO symbol_cooldowns (symbol, cooldown_until) VALUES (?, ?)", (sym, cooldown_time))
                        conn.execute("""
                            INSERT INTO loss_diagnostics (symbol, action, entry_price, exit_price, stop_loss_price, exit_type, holding_duration_mins, market_regime, diagnostic_reason)
                            VALUES (?, ?, ?, ?, ?, 'STOP_LOSS', ?, ?, ?)
                        """, (sym, act, entry, exit_price, current_sl, holding_mins, regime_label, f"Hit SL at ${current_sl:.6f} after {holding_mins:.0f}m ({final_pnl:+.2f}% net PnL)."))

                log_event("INFO", "SENTINEL_EXIT", f"Trade {sym} {act} closed via {exit_reason_text}. Net Realized: {final_pnl:+.2f}%, Duration: {holding_mins:.0f}m")

                emoji = "🛑 <b>STOP-LOSS HIT ON BINANCE</b>" if final_pnl < 0 else "🛡️ <b>BREAK-EVEN / PROFIT STOP TRIGGERED</b>"
                msg = (
                    f"{emoji}\n\n"
                    f"• <b>Contract:</b> <code>{sym}</code> ({act})\n"
                    f"• <b>Exit Price:</b> ${exit_price:.6f}\n"
                    f"• <b>Holding Time:</b> <code>{holding_mins:.0f} mins</code>\n"
                    f"• <b>Net Realized Result (after fees):</b> <code>{final_pnl:+.2f}%</code>\n"
                    f"🤖 <b>Position closed and orders cleaned up on Binance.</b>"
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
