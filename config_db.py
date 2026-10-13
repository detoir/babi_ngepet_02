"""
================================================================================
MODULE: config_db.py
ROLE  : Dynamic Database Schema, Speed Tracking & Target Lock Engine
================================================================================
"""

import os
import sys
import time
import sqlite3
import datetime
import re
import csv
import io
import ccxt

BOT_NAME = "babi_ngepet_01"
BOT_VERSION = "38.0.0-ENTERPRISE-STABLE"

def get_env_str(key: str, default: str = "") -> str:
    val = os.getenv(key)
    return val.strip().strip('"').strip("'") if val is not None else default

def get_env_int(key: str, default: int) -> int:
    try:
        val = os.getenv(key)
        return int(val.strip()) if val is not None else default
    except (ValueError, AttributeError):
        return default

def get_env_float(key: str, default: float) -> float:
    try:
        val = os.getenv(key)
        return float(val.strip()) if val is not None else default
    except (ValueError, AttributeError):
        return default

# Credentials & API Keys
DEEPSEEK_API_KEY = get_env_str("DEEPSEEK_API_KEY", "")
TELEGRAM_BOT_TOKEN = get_env_str("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = get_env_str("TELEGRAM_CHAT_ID", "")
BINANCE_API_KEY = get_env_str("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = get_env_str("BINANCE_SECRET_KEY", "")

# Operational Cadence & Loops
SCAN_INTERVAL_SECONDS = get_env_int("SCAN_INTERVAL_SECONDS", 3600)
WATCH_INTERVAL_SECONDS = get_env_int("WATCH_INTERVAL_SECONDS", 8)
PENDING_TTL_HOURS = get_env_int("PENDING_TTL_HOURS", 3)

# Trading Execution Mode & Limits
# TRADING_MODE options: "SIMULATION" (virtual alerts/tracking), "TESTNET" (Binance Futures Testnet), "LIVE" (Real Binance Futures)
TRADING_MODE = get_env_str("TRADING_MODE", "SIMULATION").upper()
DEFAULT_LEVERAGE = get_env_int("DEFAULT_LEVERAGE", 2)
SCALP_LEVERAGE = get_env_int("SCALP_LEVERAGE", 5)  # More aggressive leverage for scalps
SCALP_ENTRY_TYPE = get_env_str("SCALP_ENTRY_TYPE", "MARKET").upper()  # MARKET for immediate fills, LIMIT for resting
MAX_CORRELATED_POSITIONS_PER_SIDE = get_env_int("MAX_CORRELATED_POSITIONS_PER_SIDE", 3)
MAX_CORRELATED_SCALPS_PER_SIDE = get_env_int("MAX_CORRELATED_SCALPS_PER_SIDE", 4)

# Risk & Gate Parameters
MIN_RRR = get_env_float("MIN_RRR", 1.8)
CONFIDENCE_THRESHOLD = get_env_float("CONFIDENCE_THRESHOLD", 0.75)
MAX_24H_LOSS_THRESHOLD = get_env_float("MAX_24H_LOSS_THRESHOLD", -25.0)
MAX_CONCURRENT_POSITIONS = get_env_int("MAX_CONCURRENT_POSITIONS", 30)
MAX_DAILY_TRADES = get_env_int("MAX_DAILY_TRADES", 999999)  # Fully uncapped by default
ACCOUNT_BALANCE_SIMULATED = get_env_float("ACCOUNT_BALANCE_SIM", 10000.0)

# Stagnation & Whale Thresholds
STAGNATION_MINS = get_env_float("STAGNATION_MINS", 180.0)
STAGNATION_PNL_FLOOR = get_env_float("STAGNATION_PNL_FLOOR", -1.5)
WHALE_MIN_PROBABILITY = get_env_float("WHALE_MIN_PROBABILITY", 75.0)
SCALP_MIN_PROBABILITY = get_env_float("SCALP_MIN_PROBABILITY", 70.0)

# Database Storage Path
DATA_DIR = get_env_str("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, "scanner_memory.db")

def get_exchange_instance():
    config = {
        "enableRateLimit": True,
        "timeout": 25000,
        "options": {"defaultType": "future", "adjustForTimeDifference": True}
    }
    if BINANCE_API_KEY and BINANCE_SECRET_KEY:
        config["apiKey"] = BINANCE_API_KEY
        config["secret"] = BINANCE_SECRET_KEY
    ex = ccxt.binanceusdm(config)
    if TRADING_MODE == "TESTNET":
        try:
            ex.set_sandbox_mode(True)
        except Exception as e:
            print(f"[{BOT_NAME}] Sandbox mode warning: {e}")
    try:
        ex.load_markets()
    except Exception as e:
        print(f"[{BOT_NAME}] Exchange initialization warning: {e}")
    return ex

# Initialize module-level exchange instance for trading functions in config_db
exchange = get_exchange_instance()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=60.0, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 60000;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id TEXT DEFAULT 'babi_ngepet_01',
                symbol TEXT,
                action TEXT,
                confidence REAL,
                rrr REAL,
                entry_limit REAL,
                stop_loss REAL,
                target_1 REAL,
                target_2 REAL,
                target_3 REAL,
                macro_bias TEXT,
                reason TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS watched_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE,
                action TEXT,
                entry_limit REAL,
                initial_sl REAL,
                current_sl REAL,
                target_1 REAL,
                target_2 REAL,
                target_3 REAL,
                atr_val REAL DEFAULT 0.0,
                position_size_usd REAL DEFAULT 0.0,
                stage INTEGER DEFAULT 0,
                highest_favorable_price REAL,
                status TEXT DEFAULT 'WATCHING',
                realized_pnl REAL DEFAULT 0.0,
                confidence REAL DEFAULT 0.85,
                trade_speed TEXT DEFAULT 'FAST TRADE',
                coin_trend TEXT DEFAULT 'NEUTRAL',
                strategic_thesis TEXT DEFAULT '',
                movement_tag TEXT DEFAULT '',
                binance_entry_order_id TEXT DEFAULT '',
                binance_sl_order_id TEXT DEFAULT '',
                binance_tp1_id TEXT DEFAULT '',
                binance_tp2_id TEXT DEFAULT '',
                binance_tp3_id TEXT DEFAULT '',
                executed_qty REAL DEFAULT 0.0,
                remaining_ratio REAL DEFAULT 1.0,
                partial_realized_pnl REAL DEFAULT 0.0,
                trading_mode TEXT DEFAULT 'SIMULATION',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Dynamic migrations for existing databases
        for col_name, col_def in [
            ("binance_entry_order_id", "TEXT DEFAULT ''"),
            ("binance_sl_order_id", "TEXT DEFAULT ''"),
            ("binance_tp1_id", "TEXT DEFAULT ''"),
            ("binance_tp2_id", "TEXT DEFAULT ''"),
            ("binance_tp3_id", "TEXT DEFAULT ''"),
            ("executed_qty", "REAL DEFAULT 0.0"),
            ("remaining_ratio", "REAL DEFAULT 1.0"),
            ("partial_realized_pnl", "REAL DEFAULT 0.0"),
            ("trading_mode", "TEXT DEFAULT 'SIMULATION'"),
            ("leverage", "INTEGER DEFAULT 1"),
            ("rsi_1h", "REAL DEFAULT 50.0"),
            ("funding_rate", "REAL DEFAULT 0.0"),
            ("book_ratio", "REAL DEFAULT 1.0"),
            ("whale_ls_ratio", "REAL DEFAULT 1.0"),
            ("btc_24h_pct", "REAL DEFAULT 0.0"),
            ("profit_index", "REAL DEFAULT 0.0"),
            ("exit_reason", "TEXT DEFAULT ''")
        ]:
            try:
                c.execute(f"ALTER TABLE watched_signals ADD COLUMN {col_name} {col_def};")
            except sqlite3.OperationalError:
                pass

        # Self-healing migration: Repair rows where highest_favorable_price was saved as text (e.g. TP tags)
        try:
            c.execute("""
                UPDATE watched_signals 
                SET movement_tag = highest_favorable_price, highest_favorable_price = entry_limit 
                WHERE typeof(highest_favorable_price) = 'text' AND (highest_favorable_price LIKE '%TP%' OR highest_favorable_price LIKE '%LOCK%');
            """)
        except Exception:
            pass
        c.execute("""
            CREATE TABLE IF NOT EXISTS loss_diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                action TEXT,
                entry_price REAL,
                exit_price REAL,
                stop_loss_price REAL,
                exit_type TEXT,
                holding_duration_mins REAL,
                market_regime TEXT,
                diagnostic_reason TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS symbol_cooldowns (
                symbol TEXT PRIMARY KEY,
                cooldown_until DATETIME
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS ai_audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                task_type TEXT,
                prompt_payload TEXT,
                response_payload TEXT,
                execution_time_ms REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS strategy_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id TEXT DEFAULT 'babi_ngepet_01',
                directive TEXT,
                active INTEGER DEFAULT 1,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                component TEXT,
                message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        c.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signal_history(timestamp);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_watched_status ON watched_signals(status);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON system_logs(timestamp);")

        c.execute("INSERT OR IGNORE INTO bot_state (key, value) VALUES ('scanning_active', '1');")
        c.execute("INSERT OR IGNORE INTO bot_state (key, value) VALUES ('circuit_breaker_until', '0');")
        c.execute("INSERT OR IGNORE INTO bot_state (key, value) VALUES ('system_shutdown', '0');")
        c.execute("INSERT OR IGNORE INTO bot_state (key, value) VALUES ('eval_running', '0');")

init_db()

def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

def parse_database_timestamp(raw_ts: str) -> datetime.datetime:
    if not raw_ts:
        return utc_now()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.datetime.strptime(raw_ts.split("+")[0].split("Z")[0], fmt)
        except ValueError:
            pass
    return utc_now()

def log_event(level: str, component: str, message: str):
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level:<5}] [{component:<14}] {message}")
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO system_logs (level, component, message, timestamp) VALUES (?, ?, ?, ?)",
                (level.upper(), component, message, timestamp)
            )
    except Exception:
        pass

def is_weekend_regime() -> bool:
    now_utc = utc_now()
    weekday = now_utc.weekday()
    hour = now_utc.hour
    if weekday == 4 and hour >= 22:
        return True
    if weekday == 5:
        return True
    if weekday == 6 and hour < 22:
        return True
    return False

def bridge_symbol_formats(raw_symbol: str) -> dict:
    cleaned = raw_symbol.upper().replace(" ", "")
    if ":" in cleaned:
        ccxt_symbol = cleaned
        pair_part = ccxt_symbol.split(":")[0]
        base_asset = pair_part.split("/")[0]
        return {"ccxt": ccxt_symbol, "base": base_asset}

    if "/" in cleaned:
        base_asset = cleaned.split("/")[0]
        quote_asset = cleaned.split("/")[1]
        return {"ccxt": f"{cleaned}:{quote_asset}", "base": base_asset}

    if cleaned.endswith("USDC"):
        base_asset = cleaned[:-4]
        return {"ccxt": f"{base_asset}/USDC:USDC", "base": base_asset}

    if cleaned.endswith("USDT"):
        base_asset = cleaned[:-4]
        return {"ccxt": f"{base_asset}/USDT:USDT", "base": base_asset}

    base_asset = cleaned
    return {"ccxt": f"{base_asset}/USDT:USDT", "base": base_asset}

def is_scanning_enabled() -> bool:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT value FROM bot_state WHERE key = 'scanning_active'")
        row = c.fetchone()
        active = row["value"] == "1" if row else True

        c.execute("SELECT value FROM bot_state WHERE key = 'circuit_breaker_until'")
        cb_row = c.fetchone()
        cb_until = float(cb_row["value"]) if cb_row else 0.0
        if time.time() < cb_until:
            return False
        return active

def is_system_shutdown_requested() -> bool:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT value FROM bot_state WHERE key = 'system_shutdown'")
        row = c.fetchone()
        return row["value"] == "1" if row else False

def set_system_shutdown(state="1"):
    with get_db_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('system_shutdown', ?)", (state,))

def check_rolling_24h_drawdown() -> float:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT SUM(realized_pnl) as total_pnl FROM watched_signals 
            WHERE status IN ('INVALIDATED', 'COMPLETED') 
            AND last_updated >= datetime('now', '-1 day')
        """)
        row = c.fetchone()
        return float(row["total_pnl"] or 0.0)

def count_active_and_daily_trades() -> tuple:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as watched_cnt FROM watched_signals WHERE status = 'WATCHING'")
        watched_cnt = c.fetchone()["watched_cnt"]

        c.execute("""
            SELECT COUNT(*) as daily_cnt FROM watched_signals 
            WHERE stage >= 1 AND timestamp >= datetime('now', '-1 day')
        """)
        daily_cnt = c.fetchone()["daily_cnt"]
        return watched_cnt, daily_cnt

def count_active_positions_by_direction(direction: str) -> int:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM watched_signals WHERE status = 'WATCHING' AND action = ?", (direction.upper(),))
        row = c.fetchone()
        return row["cnt"] if row else 0

def fetch_current_account_capital() -> dict:
    try:
        if TRADING_MODE in ("LIVE", "TESTNET"):
            if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
                log_event("WARN", "CAPITAL_FETCH", f"TRADING_MODE is {TRADING_MODE} but BINANCE_API_KEY / SECRET is missing. Using simulated balance.")
            else:
                bal = exchange.fetch_balance()
                total_bal = 0.0
                free_bal = 0.0

                # 1. Try standard CCXT USDT balance
                usdt_info = bal.get("USDT", {})
                total_bal = float(usdt_info.get("total") or bal.get("total", {}).get("USDT") or 0.0)
                free_bal = float(usdt_info.get("free") or bal.get("free", {}).get("USDT") or 0.0)

                # 2. If 0, inspect native Binance USD-M futures response structure
                if total_bal <= 0:
                    info_assets = bal.get("info", {}).get("assets", [])
                    for a in info_assets:
                        if a.get("asset") in ("USDT", "USDC"):
                            wb = float(a.get("walletBalance") or a.get("marginBalance") or 0.0)
                            av = float(a.get("availableBalance") or a.get("maxWithdrawAmount") or 0.0)
                            if wb > total_bal:
                                total_bal = wb
                                free_bal = av

                # 3. If still 0, check top-level totalWalletBalance / availableBalance
                if total_bal <= 0:
                    raw_info = bal.get("info", {})
                    total_bal = float(raw_info.get("totalWalletBalance") or raw_info.get("totalMarginBalance") or 0.0)
                    free_bal = float(raw_info.get("availableBalance") or 0.0)

                if total_bal > 0:
                    with get_db_connection() as conn:
                        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('current_capital_usd', ?)", (str(total_bal),))
                    return {"total": round(total_bal, 2), "free": round(free_bal, 2), "mode": TRADING_MODE}
                else:
                    log_event("WARN", "CAPITAL_FETCH", f"Live balance fetched as 0.0 USDT. Check Futures wallet funding.")
    except Exception as e:
        log_event("WARN", "CAPITAL_FETCH", f"Exchange balance fetch error: {e}")

    # Fallback to simulated balance + realized PnL
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT SUM(realized_pnl) as total_pnl FROM watched_signals WHERE status IN ('COMPLETED', 'INVALIDATED', 'STOPPED_OUT')")
        row = c.fetchone()
        realized_pnl_pct = float(row["total_pnl"] or 0.0)
        sim_balance = ACCOUNT_BALANCE_SIMULATED * (1.0 + (realized_pnl_pct / 100.0))
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('current_capital_usd', ?)", (str(sim_balance),))
        return {"total": round(sim_balance, 2), "free": round(sim_balance * 0.8, 2), "mode": "SIMULATION"}

def set_symbol_leverage_and_margin(symbol: str, leverage: int = DEFAULT_LEVERAGE, margin_mode: str = "ISOLATED") -> bool:
    if TRADING_MODE not in ("LIVE", "TESTNET"):
        return True
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    try:
        exchange.set_leverage(leverage, target_sym)
    except Exception as e:
        log_event("WARN", "BINANCE_EXEC", f"Leverage set error on {target_sym}: {e}")
    try:
        exchange.set_margin_mode(margin_mode.upper(), target_sym)
    except Exception:
        # Error -4046 means margin mode is already set
        pass
    return True

def format_order_quantity(symbol: str, pos_size_usd: float, price: float) -> float:
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    if price <= 0:
        return 0.0
    raw_qty = pos_size_usd / price
    try:
        market = exchange.market(target_sym)
        min_qty = float(market.get("limits", {}).get("amount", {}).get("min") or 0.001)
        min_notional = float(market.get("limits", {}).get("cost", {}).get("min") or 5.0)
        if (raw_qty * price) < min_notional:
            raw_qty = (min_notional * 1.05) / price
        if raw_qty < min_qty:
            raw_qty = min_qty
        formatted = float(exchange.amount_to_precision(target_sym, raw_qty))
        return formatted
    except Exception as e:
        log_event("WARN", "BINANCE_PRECISION", f"Qty precision formatting error on {target_sym}: {e}")
        return round(raw_qty, 3)

def format_order_price(symbol: str, price: float) -> float:
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    try:
        return float(exchange.price_to_precision(target_sym, price))
    except Exception:
        return round(price, 4)

def place_binance_market_entry(symbol: str, side: str, qty_usd: float, leverage: int = None) -> dict:
    if TRADING_MODE not in ("LIVE", "TESTNET"):
        return {"id": "SIMULATED_MARKET_ENTRY", "filled": 0.0, "price": 0.0}
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    order_side = "buy" if side.upper() == "LONG" else "sell"
    lev = leverage if leverage is not None else DEFAULT_LEVERAGE
    try:
        set_symbol_leverage_and_margin(target_sym, lev)
        # Fetch current price to estimate initial qty
        ticker = exchange.fetch_ticker(target_sym)
        price = float(ticker.get("last") or ticker.get("close") or 1.0)
        formatted_qty = format_order_quantity(target_sym, qty_usd, price)
        order = exchange.create_order(
            symbol=target_sym,
            type="MARKET",
            side=order_side,
            amount=formatted_qty
        )
        order_id = str(order.get("id"))
        filled_qty = float(order.get("filled") or formatted_qty)
        avg_price = float(order.get("average") or order.get("price") or price)
        log_event("INFO", "BINANCE_MARKET_ENTRY", f"Filled MARKET {order_side.upper()} for {filled_qty} {target_sym} at ${avg_price:.6f} (ID: {order_id})")
        return {"id": order_id, "filled": filled_qty, "price": avg_price}
    except Exception as e:
        log_event("ERROR", "BINANCE_MARKET_ENTRY", f"Market entry failed on {target_sym}: {e}")
        return {"id": "", "filled": 0.0, "price": 0.0}

def place_binance_entry_limit(symbol: str, side: str, qty_usd: float, price: float, leverage: int = None) -> str:
    if TRADING_MODE not in ("LIVE", "TESTNET"):
        return "SIMULATED_ENTRY"
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    order_side = "buy" if side.upper() == "LONG" else "sell"
    lev = leverage if leverage is not None else DEFAULT_LEVERAGE
    try:
        set_symbol_leverage_and_margin(target_sym, lev)
        formatted_price = format_order_price(target_sym, price)
        formatted_qty = format_order_quantity(target_sym, qty_usd, price)
        order = exchange.create_limit_order(target_sym, order_side, formatted_qty, formatted_price, {
            "timeInForce": "GTC"
        })
        order_id = str(order.get("id"))
        log_event("INFO", "BINANCE_ORDER", f"Placed {order_side.upper()} Limit for {formatted_qty} {target_sym} at ${formatted_price:.6f} (ID: {order_id})")
        return order_id
    except Exception as e:
        log_event("ERROR", "BINANCE_ORDER", f"Entry limit failed on {target_sym}: {e}")
        return ""

def place_binance_stop_loss(symbol: str, side: str, qty_contracts: float, stop_price: float) -> str:
    if TRADING_MODE not in ("LIVE", "TESTNET"):
        return "SIMULATED_SL"
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    # If position is LONG, exit stop is SELL; if SHORT, exit stop is BUY
    order_side = "sell" if side.upper() == "LONG" else "buy"
    try:
        formatted_stop = format_order_price(target_sym, stop_price)
        formatted_qty = float(exchange.amount_to_precision(target_sym, qty_contracts))
        order = exchange.create_order(
            symbol=target_sym,
            type="STOP_MARKET",
            side=order_side,
            amount=formatted_qty,
            params={
                "stopPrice": formatted_stop,
                "reduceOnly": True,
                "workingType": "MARK_PRICE"
            }
        )
        order_id = str(order.get("id"))
        log_event("INFO", "BINANCE_SL", f"Placed Hard SL for {formatted_qty} {target_sym} at ${formatted_stop:.6f} (ID: {order_id})")
        return order_id
    except Exception as e:
        log_event("ERROR", "BINANCE_SL", f"Stop loss order failed on {target_sym}: {e}")
        return ""

def cancel_binance_order(symbol: str, order_id: str) -> bool:
    if TRADING_MODE not in ("LIVE", "TESTNET") or not order_id or order_id.startswith("SIMULATED"):
        return True
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    try:
        exchange.cancel_order(order_id, target_sym)
        log_event("INFO", "BINANCE_CANCEL", f"Cancelled order {order_id} on {target_sym}")
        return True
    except Exception as e:
        log_event("WARN", "BINANCE_CANCEL", f"Cancel order {order_id} exception on {target_sym}: {e}")
        return False

def emergency_binance_market_close(symbol: str, side: str, qty_contracts: float = 0.0) -> bool:
    if TRADING_MODE not in ("LIVE", "TESTNET"):
        return True
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    order_side = "sell" if side.upper() == "LONG" else "buy"
    try:
        params = {"reduceOnly": True}
        if qty_contracts > 0:
            formatted_qty = float(exchange.amount_to_precision(target_sym, qty_contracts))
            exchange.create_order(symbol=target_sym, type="MARKET", side=order_side, amount=formatted_qty, params=params)
        else:
            # Fetch current open position size on Binance
            positions = exchange.fetch_positions([target_sym])
            for p in positions:
                contracts = abs(float(p.get("contracts") or 0.0))
                if contracts > 0:
                    exchange.create_order(symbol=target_sym, type="MARKET", side=order_side, amount=contracts, params=params)
                    break
        log_event("INFO", "BINANCE_CLOSE", f"Emergency Market Close executed for {target_sym}")
        return True
    except Exception as e:
        log_event("ERROR", "BINANCE_CLOSE", f"Emergency close failed on {target_sym}: {e}")
        return False

def fetch_binance_order_status(symbol: str, order_id: str) -> dict:
    if TRADING_MODE not in ("LIVE", "TESTNET") or not order_id or order_id.startswith("SIMULATED"):
        return {"status": "UNKNOWN", "filled": 0.0, "average": 0.0}
    bridge = bridge_symbol_formats(symbol)
    target_sym = bridge["ccxt"]
    try:
        ord_data = exchange.fetch_order(order_id, target_sym)
        return {
            "status": str(ord_data.get("status", "open")).upper(),
            "filled": float(ord_data.get("filled") or 0.0),
            "average": float(ord_data.get("average") or ord_data.get("price") or 0.0)
        }
    except Exception as e:
        log_event("WARN", "BINANCE_STATUS", f"Fetch order {order_id} error on {target_sym}: {e}")
        return {"status": "ERROR", "filled": 0.0, "average": 0.0}

def trigger_circuit_breaker(hours=4):
    cb_until = time.time() + (hours * 3600)
    with get_db_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES ('circuit_breaker_until', ?)", (str(cb_until),))
    log_event("WARN", "CIRCUIT_BREAKER", f"Drawdown floor hit ({MAX_24H_LOSS_THRESHOLD}%). Paused for {hours} hours.")

def prune_old_logs():
    try:
        with get_db_connection() as conn:
            conn.execute("DELETE FROM system_logs WHERE timestamp < datetime('now', '-2 days')")
            conn.execute("DELETE FROM symbol_cooldowns WHERE cooldown_until <= datetime('now')")
            conn.execute("DELETE FROM ai_audit_logs WHERE timestamp < datetime('now', '-2 days')")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    except Exception:
        pass

def get_contextual_trade_lessons() -> str:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT symbol, action, exit_type, holding_duration_mins, market_regime, diagnostic_reason 
            FROM loss_diagnostics ORDER BY id DESC LIMIT 5
        """)
        rows = c.fetchall()
        if not rows:
            return "No recent liquidation penalties recorded."
        return "\n".join([
            f"- {r['symbol']} ({r['action']}): Exited via {r['exit_type']} after {r['holding_duration_mins']:.0f}m. Cause: {r['diagnostic_reason']}"
            for r in rows
        ])

def get_cooldown_symbols() -> list:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT symbol FROM symbol_cooldowns WHERE cooldown_until > CURRENT_TIMESTAMP")
        return [r["symbol"] for r in c.fetchall()]

def upsert_watched_signal(
    symbol, action, entry, sl, t1, t2, t3, atr_val, pos_size, trade_speed, coin_trend, 
    strategic_thesis="", confidence=0.75, stage_init=0, binance_entry_order_id="", executed_qty=0.0,
    leverage=1, rsi_1h=50.0, funding_rate=0.0, book_ratio=1.0, whale_ls_ratio=1.0, btc_24h_pct=0.0, profit_index=0.0,
    binance_sl_order_id=""
):
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT stage, status, action, confidence, entry_limit, binance_entry_order_id, binance_sl_order_id FROM watched_signals WHERE symbol = ?", (symbol,))
        existing = c.fetchone()

        if existing and existing["status"] == 'WATCHING' and existing["stage"] >= 1:
            log_event("INFO", "WATCHLIST", f"Targets locked for live trade {symbol}. Modification blocked.")
            return False

        movement_tag = "🆕 NEW SETUP"
        if existing and existing["status"] == 'WATCHING':
            if existing["action"] != action:
                movement_tag = f"🔄 BIAS FLIP: {existing['action']} ➔ {action}"
            elif existing["stage"] == 0:
                price_delta = abs(entry - float(existing["entry_limit"] or entry))
                movement_tag = f"📈 RE-ANCHORED (Δ ${price_delta:.4f})" if price_delta > 0 else "⚖️ RE-CONFIRMED"

        conn.execute("""
            INSERT INTO watched_signals 
            (symbol, action, entry_limit, initial_sl, current_sl, target_1, target_2, target_3, atr_val, position_size_usd, stage, highest_favorable_price, status, realized_pnl, confidence, trade_speed, coin_trend, strategic_thesis, movement_tag, binance_entry_order_id, binance_sl_order_id, executed_qty, trading_mode, leverage, rsi_1h, funding_rate, book_ratio, whale_ls_ratio, btc_24h_pct, profit_index, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WATCHING', 0.0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                action=excluded.action,
                entry_limit=excluded.entry_limit,
                initial_sl=excluded.initial_sl,
                current_sl=excluded.current_sl,
                target_1=excluded.target_1,
                target_2=excluded.target_2,
                target_3=excluded.target_3,
                atr_val=excluded.atr_val,
                position_size_usd=excluded.position_size_usd,
                stage=excluded.stage,
                highest_favorable_price=excluded.entry_limit,
                status='WATCHING',
                realized_pnl=0.0,
                confidence=excluded.confidence,
                trade_speed=excluded.trade_speed,
                coin_trend=excluded.coin_trend,
                strategic_thesis=excluded.strategic_thesis,
                movement_tag=excluded.movement_tag,
                binance_entry_order_id=excluded.binance_entry_order_id,
                binance_sl_order_id=excluded.binance_sl_order_id,
                executed_qty=excluded.executed_qty,
                trading_mode=excluded.trading_mode,
                leverage=excluded.leverage,
                rsi_1h=excluded.rsi_1h,
                funding_rate=excluded.funding_rate,
                book_ratio=excluded.book_ratio,
                whale_ls_ratio=excluded.whale_ls_ratio,
                btc_24h_pct=excluded.btc_24h_pct,
                profit_index=excluded.profit_index,
                exit_reason='',
                last_updated=CURRENT_TIMESTAMP
        """, (
            symbol, action, entry, sl, sl, t1, t2, t3, atr_val, pos_size, stage_init, entry, 
            confidence, trade_speed, coin_trend, strategic_thesis, movement_tag, binance_entry_order_id, 
            binance_sl_order_id, executed_qty, TRADING_MODE, leverage, rsi_1h, funding_rate, book_ratio, 
            whale_ls_ratio, btc_24h_pct, profit_index
        ))

        risk_dist = abs(entry - sl) if entry > 0 else 1.0
        reward_dist = abs(t1 - entry) if entry > 0 else 0.0
        rrr = round(reward_dist / risk_dist, 2) if risk_dist > 0 else 0.0
        conn.execute("""
            INSERT INTO signal_history (symbol, action, confidence, rrr, entry_limit, stop_loss, target_1, target_2, target_3, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (symbol, action, confidence, rrr, entry, sl, t1, t2, t3, strategic_thesis))
    return True

def remove_watched_signal_by_symbol(raw_symbol: str) -> int:
    bridge = bridge_symbol_formats(raw_symbol)
    target_sym = bridge["ccxt"]
    base_sym = bridge["base"]

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            UPDATE watched_signals 
            SET status = 'REMOVED', realized_pnl = 0.0, last_updated = CURRENT_TIMESTAMP 
            WHERE status = 'WATCHING' AND (symbol = ? OR symbol LIKE ?)
        """, (target_sym, f"%{base_sym}%"))
        return c.rowcount

def clear_all_watched_signals() -> int:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE watched_signals SET status = 'REMOVED', realized_pnl = 0.0, last_updated = CURRENT_TIMESTAMP WHERE status = 'WATCHING'")
        return c.rowcount

def get_active_watched_signals() -> list:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM watched_signals WHERE status = 'WATCHING' ORDER BY stage DESC, id DESC")
        return c.fetchall()

def get_active_strategy_rules() -> str:
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT directive FROM strategy_rules WHERE active = 1 ORDER BY id DESC LIMIT 5")
        rows = c.fetchall()
        return "\n".join([f"- {r['directive']}" for r in rows]) if rows else "No custom directives set."

def add_strategy_rule(directive: str):
    clean_directive = re.sub(r"[^\w\s\-\.\,\:\%\>\<\=]", "", directive).strip()
    with get_db_connection() as conn:
        conn.execute("INSERT INTO strategy_rules (bot_id, directive) VALUES (?, ?)", (BOT_NAME, clean_directive))

def clear_strategy_rules():
    with get_db_connection() as conn:
        conn.execute("UPDATE strategy_rules SET active = 0")

def log_ai_interaction(symbol: str, task_type: str, prompt: str, response: str, duration_ms: float):
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO ai_audit_logs (symbol, task_type, prompt_payload, response_payload, execution_time_ms) VALUES (?, ?, ?, ?, ?)",
                (symbol, task_type, prompt, response, duration_ms)
            )
    except Exception:
        pass

def get_recent_system_logs(limit=15, level_filter=None) -> list:
    with get_db_connection() as conn:
        c = conn.cursor()
        if level_filter:
            c.execute(
                "SELECT level, component, message, timestamp FROM system_logs WHERE timestamp >= datetime('now', '-1 day') AND level = ? ORDER BY id DESC LIMIT ?",
                (level_filter.upper(), limit)
            )
        else:
            c.execute(
                "SELECT level, component, message, timestamp FROM system_logs WHERE timestamp >= datetime('now', '-1 day') ORDER BY id DESC LIMIT ?",
                (limit,)
            )
        return c.fetchall()

def generate_monthly_performance_report() -> dict:
    start_of_month = utc_now().strftime("%Y-%m-01 00:00:00")
    
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) as scan_count 
            FROM system_logs 
            WHERE component IN ('AI_SCANNER', 'WHALE_SCANNER') 
            AND message LIKE '%complete%' 
            AND timestamp >= ?
        """, (start_of_month,))
        total_scans = c.fetchone()["scan_count"] or 0
        
        c.execute("SELECT COUNT(*) as signal_count FROM signal_history WHERE timestamp >= ?", (start_of_month,))
        total_signals = c.fetchone()["signal_count"] or 0
        
        c.execute("""
            SELECT realized_pnl, status 
            FROM watched_signals 
            WHERE status IN ('COMPLETED', 'INVALIDATED', 'STOPPED_OUT') 
            AND stage >= 1 
            AND last_updated >= ?
        """, (start_of_month,))
        closed_trades = c.fetchall()
        
        wins = [t for t in closed_trades if float(t["realized_pnl"] or 0) > 0]
        losses = [t for t in closed_trades if float(t["realized_pnl"] or 0) <= 0]
        
        win_count = len(wins)
        loss_count = len(losses)
        total_closed = win_count + loss_count
        
        win_rate = (win_count / total_closed * 100.0) if total_closed > 0 else 0.0
        net_pnl = sum(float(t["realized_pnl"] or 0) for t in closed_trades)
        
        c.execute("SELECT COUNT(*) as active_cnt FROM watched_signals WHERE status = 'WATCHING' AND stage >= 1")
        active_positions = c.fetchone()["active_cnt"] or 0

        c.execute("SELECT COUNT(*) as pending_cnt FROM watched_signals WHERE status = 'WATCHING' AND stage = 0")
        pending_orders = c.fetchone()["pending_cnt"] or 0

        cap_info = fetch_current_account_capital()
        avg_win = (sum(float(t["realized_pnl"] or 0) for t in wins) / win_count) if win_count > 0 else 0.0
        avg_loss = (sum(float(t["realized_pnl"] or 0) for t in losses) / loss_count) if loss_count > 0 else 0.0

        return {
            "month": utc_now().strftime("%B %Y"),
            "start_date": start_of_month.split(" ")[0],
            "total_scans": total_scans,
            "total_signals": total_signals,
            "total_closed": total_closed,
            "wins": win_count,
            "losses": loss_count,
            "win_rate": round(win_rate, 1),
            "net_pnl": round(net_pnl, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "active_positions": active_positions,
            "pending_orders": pending_orders,
            "capital_usd": cap_info.get("total", 0.0),
            "free_margin_usd": cap_info.get("free", 0.0),
            "mode": cap_info.get("mode", TRADING_MODE),
            "regime": "WEEKEND (Mean-Reversion Only)" if is_weekend_regime() else "WEEKDAY (Institutional Momentum)"
        }

def export_monthly_report_csv() -> tuple:
    start_of_month = utc_now().strftime("%Y-%m-01 00:00:00")
    filename = f"Trade_Report_{utc_now().strftime('%Y_%m')}.csv"
    
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, symbol, action, entry_limit, current_sl, target_1, target_2, target_3, 
                   realized_pnl, status, confidence, trade_speed, coin_trend, strategic_thesis, 
                   trading_mode, leverage, rsi_1h, funding_rate, book_ratio, whale_ls_ratio, btc_24h_pct, profit_index, exit_reason,
                   timestamp, last_updated
            FROM watched_signals
            WHERE last_updated >= ?
            ORDER BY id DESC
        """, (start_of_month,))
        rows = c.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "ID", "Symbol", "Direction", "Entry Price", "Exit SL", "TP1", "TP2", "TP3",
        "Realized PnL %", "Final Status", "Confidence", "Mode", "Leverage",
        "RSI 1H", "Funding Rate %", "Book Ratio", "Whale L/S Ratio", "BTC 24h %", "Profit Index EV",
        "Speed", "Trend", "Exit Reason", "Thesis",
        "Signal Timestamp (UTC)", "Closed Timestamp (UTC)"
    ])

    for r in rows:
        r_dict = dict(r)
        lev = r_dict.get("leverage") or (SCALP_LEVERAGE if r_dict.get("trade_speed") == "SCALP TRADE" else DEFAULT_LEVERAGE)
        rsi = round(float(r_dict.get("rsi_1h") or 50.0), 1)
        fr = f"{float(r_dict.get('funding_rate') or 0.0):+.4f}%"
        br = round(float(r_dict.get("book_ratio") or 1.0), 2)
        wls = round(float(r_dict.get("whale_ls_ratio") or 1.0), 2)
        btc_chg = f"{float(r_dict.get('btc_24h_pct') or 0.0):+.2f}%"
        p_idx = round(float(r_dict.get("profit_index") or 0.0), 2)
        ex_reason = r_dict.get("exit_reason") or r_dict.get("movement_tag") or ""

        writer.writerow([
            r["id"], r["symbol"], r["action"], r["entry_limit"], r["current_sl"],
            r["target_1"], r["target_2"], r["target_3"], f"{float(r['realized_pnl'] or 0):+.2f}%",
            r["status"], f"{float(r['confidence'] or 0)*100:.0f}%", r_dict.get("trading_mode", "LIVE"), f"{lev}x",
            rsi, fr, br, wls, btc_chg, p_idx,
            r["trade_speed"], r["coin_trend"], ex_reason, r["strategic_thesis"],
            r["timestamp"], r["last_updated"]
        ])

    csv_data = output.getvalue().encode("utf-8")
    return csv_data, filename
