# 📡 BABI_NGEPET_02: Autonomous Crypto Perpetual Market Intelligence, Whale Order Flow Radar & Risk Defense Engine

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?logo=fastapi)](https://fastapi.tiangolo.com/)
[![DeepSeek AI](https://img.shields.io/badge/AI-DeepSeek--V3-purple.svg)](https://www.deepseek.com/)
[![Binance Futures](https://img.shields.io/badge/Exchange-Binance%20Futures-F0B90B.svg?logo=binance)](https://www.binance.com/)

**`babi_ngepet_02`** is an institutional-grade, multi-process quant intelligence and risk-managed execution system for the **Binance USDT-M and USDC-M Perpetual Futures** market. Upgraded from previous iterations, version 02 introduces a **dual AI discovery pipeline** (Macro Regime Engine + Binance Native Derivatives Whale Radar), **dynamic Expected Value (+EV) profit prioritization**, multi-tier execution modes (**SIMULATION**, **TESTNET**, and **LIVE** via CCXT), and a **high-frequency 8-second Sentinel** equipped with peak giveback harvesting, sudden reversal defense, and BTC flash-shock protection.

---

## 📑 Table of Contents

- [Architectural Overview](#-architectural-overview)
- [Key Features & Upgrades in v02](#-key-features--upgrades-in-v02)
- [System Architecture Flowchart](#-system-architecture-flowchart)
- [Execution Modes: Simulation, Testnet & Live](#-execution-modes-simulation-testnet--live)
- [Quantitative Strategy & AI Radar](#-quantitative-strategy--ai-radar)
- [Risk Defense Sentinel & Order Lifecycle](#-risk-defense-sentinel--order-lifecycle)
- [Telegram Command Directory](#-telegram-command-directory)
- [Environment Configuration & Parameter Guide](#-environment-configuration--parameter-guide)
  - [Environment Variables (.env)](#environment-variables-env)
  - [Environment Parameter Reference Table](#environment-parameter-reference-table)
  - [Detailed Parameter Explanations](#detailed-parameter-explanations)
- [Quantitative & Algorithmic Parameter Specifications](#-quantitative--algorithmic-parameter-specifications)
  - [1. Dynamic Risk Bracket & Target Formulation](#1-dynamic-risk-bracket--target-formulation)
  - [2. Objective Quant Scoring Weights & Penalties](#2-objective-quant-scoring-weights--penalties)
  - [3. Dynamic Capital Allocation & Position Sizing](#3-dynamic-capital-allocation--position-sizing)
  - [4. Expected Profit Index (EV) & Confluence Bonuses](#4-expected-profit-index-ev--confluence-bonuses)
  - [5. Sentinel Risk Defense Trigger Thresholds](#5-sentinel-risk-defense-trigger-thresholds)
  - [6. Weekend Regime Schedule](#6-weekend-regime-schedule)
- [Database & Performance Audit Ledger](#-database--performance-audit-ledger)
- [Deployment Guide](#-deployment-guide)
  - [Docker & Docker Compose](#option-1-docker-compose-recommended)
  - [Cloud Platforms (Railway, K3s, Render)](#option-2-cloud-container-platforms-railway-k3s-render)
  - [Direct Linux VPS (Systemd)](#option-3-direct-linux-vps-systemd)
- [Safety & Risk Disclaimer](#-safety--risk-disclaimer)
- [License](#-license)

---

## 🏗️ Architectural Overview

The system runs as an integrated ASGI supervisor service (`main.py`) exposing health check endpoints for container orchestrators while managing two autonomous trading processes:

```
                                 ┌────────────────────────────────────────────────────────┐
                                 │                 FASTAPI ASGI SUPERVISOR                │
                                 │       • main.py: HTTP Health Check (:8000/health)      │
                                 │       • Child Process Life-Cycle Management & Respawn  │
                                 └───────────────────────────┬────────────────────────────┘
                                                             │
                              ┌──────────────────────────────┴──────────────────────────────┐
                              ▼                                                             ▼
  ┌───────────────────────────────────────────────────────┐     ┌───────────────────────────────────────────────────────┐
  │                 ai_scanner.py (WORKER 1)              │     │                  sentinel.py (WORKER 2)               │
  │ • Hourly Unified Discovery Pipeline (Cron/Manual)     │     │ • Silent 8-Second High-Frequency Market Radar         │
  │ • DeepSeek Macro Universe Analysis                    │     │ • BTC 5-Minute Flash Shock Dump/Pump Protection       │
  │ • Binance Microstructure Whale Radar                  │     │ • Stage 0 Limit Order Fill Detection & 3h TTL Expiry  │
  │ • Order Book Depth, Open Interest & Top Trader Ratios │     │ • Auto Break-Even Lock (+0.6R -> Entry +0.15% Fees)   │
  │ • Expected Profit Index (+EV) & Capital Allocator     │     │ • Peak Giveback & Sudden Reversal Profit Harvest      │
  │ • Interactive Telegram Bot & Monthly CSV Audit Export │     │ • Multi-Stage Partial TP Execution & Dynamic Trailing │
  └───────────────────────────┬───────────────────────────┘     └───────────────────────────┬───────────────────────────┘
                              │                                                             │
                              └──────────────────────────────┬──────────────────────────────┘
                                                             ▼
                                 ┌────────────────────────────────────────────────────────┐
                                 │            SHARED WAL-MODE SQLITE MEMORY & STATE       │
                                 │       • scanner_memory.db (PRAGMA journal_mode = WAL)  │
                                 │       • Active Signals, Diagnostic Ledger, Cooldowns   │
                                 └────────────────────────────────────────────────────────┘
```

---

## 🚀 Key Features & Upgrades in v02

### 1. Triple AI Discovery Engines & Global Financial News Ingestion
- **Macro Quant Strategist (`DeepSeek-V3`)**: Analyzes the top 35 liquid perpetual pairs, factoring in real account margin capacity, 1H/4H technical indicators (RSI, ATR, 21 EMA), BTC trend regime (ADX), weekend volatility shifts, and live **multi-stream global financial news**:
  - **Global Financial Markets & Policy**: Yahoo Finance World/Business News RSS & Google News Central Bank / Inflation / FOMC Search Feeds.
  - **Crypto Native Feeds**: CoinTelegraph & CoinDesk Major News.
  - **Macro Sentiment**: Real-time Crypto Fear & Greed Index.
  - **High-Impact Catalyst Detection**: Scans and flags upcoming/active central bank decisions, CPI/NFP data prints, and geopolitical shocks.
- **Institutional Whale Microstructure Radar**: Ingests real-time Binance derivatives microstructure:
  - **Open Interest (OI)** via `/fapi/v1/openInterest`
  - **Order Book Bid/Ask Depth Ratio** via `/fapi/v1/depth` (20-level order book analysis)
  - **Top Trader Long/Short Ratio** via `/futures/data/topLongShortAccountRatio`
  - **Funding Rate Divergence** via `/fapi/v1/premiumIndex`
  - Identifies **Crowded Traps**, **Bid Wall Spoofing**, and **Institutional Distribution Traps**.
- **High-Speed Scalp Sniper Engine**: Specialized rapid-fire AI engine focused on high-velocity 5m-15m setups:
  - Targets high-volume tokens ($> \$10\text{M}$ 24h volume) with severe order book depth imbalances (`book_ratio` $\ge 1.30$ or $\le 0.70$) and liquidity sweeps.
  - Generates tight near-market entries (0.15% limit offset) with compressed 0.8% - 1.3% risk distance.
  - Delivers **2.0x to 2.5x larger position sizing** for the exact same dollar risk, directly magnifying cash returns on rapid micro-moves!

### 2. Triple Execution Modes (`TRADING_MODE`)
- **`SIMULATION`**: 100% risk-free paper trading. Emulates limit fills and dynamically tracks account balance based on closed trade realized PnL.
- **`TESTNET`**: Automatically redirects CCXT to Binance Futures Testnet (`set_sandbox_mode(True)`) for end-to-end exchange validation.
- **`LIVE`**: Direct automated execution on Binance USDT-M Perpetual Futures via CCXT (limit entries, hard stop-losses, partial TP limit/market reductions, and emergency closures).

### 3. Expected Profit Index (+EV) & Correlation Guard
- Ranks candidate setups by mathematical expected value:
  $$\text{EV} = (\text{Confidence} \times \text{Gain}_{\text{TP1}}\%) - ((1 - \text{Confidence}) \times \text{Risk}\%)$$
- Awards confluence bonuses (+0.50 EV) when multiple AI engines (Whale Flow, Macro AI, Scalp Sniper) agree on an asset.
- **Correlation Limit**: Strictly caps active trades to **maximum 2 positions per side** (e.g., max 2 LONGs or 2 SHORTs) to eliminate directional cluster risk.

### 4. Dynamic Regime-Adaptive Risk Sentinel (8s Radar)
- **High-Speed Scalp Machine**:
  - **Stage 0 Fast Expiration**: Scalp pending limit orders expire in **45 minutes** (vs 3 hours for swing) to prevent stale fills and keep margin liquid.
  - **50% Cash Banking at +0.5R Milestone**: Once a scalp reaches $+0.5\text{R}$, banks **50% of the position immediately into realized cash** and moves the remaining 50% to Break-Even (`entry * 1.0015` / `0.9985`).
  - **Micro-Trailing Profit Floors**:
    - Peak $\ge +0.8\% \implies$ Trail stop to lock $\ge +0.35\%$ floor.
    - Peak $\ge +1.2\% \implies$ Trail stop to lock $\ge +0.70\%$ floor.
    - Peak $\ge +1.8\% \implies$ Trail stop to lock $\ge +1.20\%$ floor.
    - Peak $\ge +2.5\% \implies$ Trail stop to lock $\ge +1.80\%$ floor.
  - **Scalp Fast Stagnation Kill**: If a scalp stalls for $\ge 35$ minutes with PnL $\le +0.20\%$, Sentinel automatically closes it at market to recycle capital into active movers.
  - **Fast TP1 Exit**: At TP1 (+1.0R), locks **60% of remaining position** (leaving 40% runner for TP2/TP3).
- **Regime-Adaptive Auto Break-Even & 33% Milestone Scale-Out (Standard Swings)**:
  - **Trending (Weekdays)**: At `+1.0R`, banks **33% into realized cash profit** immediately and moves SL on remaining 67% to BE (`entry * 1.0015` / `0.9985`), guaranteeing cash in hand so winning trades never return to scratch.
  - **Sideways / Chop**: Tightens Auto-BE milestone to `+0.6R` with 33% banked to lock fast range scalps.
  - **Weekend Anomaly**: Ultra-fast `+0.5R` trigger with 33% banked to defend against low-liquidity wicks.
- **Dynamic Tiered Trailing Profit Floors (Standard Swings)**:
  - Automatically ratchets `current_sl` upwards as unrealized peak gains expand:
    - Peak gain $\ge +1.5\% \implies$ Stop-Loss trailed to lock in at least **$+0.6\%$ profit floor**.
    - Peak gain $\ge +2.2\% \implies$ Stop-Loss trailed to lock in at least **$+1.2\%$ profit floor**.
    - Peak gain $\ge +3.0\% \implies$ Stop-Loss trailed to lock in at least **$+2.0\%$ profit floor**.
- **Regime-Adaptive Reversal Harvest**:
  - **Trending**: Lets trades breathe; only exits on $\ge 1.0\%$ pullback after $\ge +2.2\%$ gain, allowing full $+3.3\%$ to $+5.5\%$ TP1/TP2 targets.
  - **Sideways / Weekend**: Secures profits quickly on any $0.4\% - 0.5\%$ giveback after a $+1.0\%$ run.
- **Accurate Simulation Execution**: In `SIMULATION` mode, stop hits execute at the exact resting order price (`current_sl`), eliminating artificial 8-second polling wick latency.
- **BTC Macro Shock Protocol**: If BTC experiences a sudden $\ge 2.0\%$ price shock within 5 minutes, the bot instantly cancels pending counter-trend limit orders and moves active counter-trend stops to Break-Even.
- **Stagnation Trap Exit**: Adaptive timeout (180m in trend, 90m in chop, 60m in weekend) flags dead trades.
- **Symbol Cooldown**: Positions stopped out receive a mandatory 4-hour cooldown lock to prevent revenge trading.

### 5. AI Watchlist Applicability & Active Position Reversal Audit
- **Pending Order Review Engine**: During every hourly AI scan, the system passes all pending Stage 0 limit orders (symbol, planned entry, current mark, distance %, elapsed pending time, original thesis) directly into the DeepSeek AI prompt.
- **Dynamic Invalidation (`CANCEL_INVALID`)**: If the technical thesis has deteriorated, price has moved too far away without filling, order book depth flipped, or macro sentiment invalidated the setup, DeepSeek AI commands `CANCEL_INVALID` and cancels the Binance order.
- **Active Position Reversal Audit**: All active positions (Stage 1+) are evaluated hourly by both Macro and Whale AI (`[ACTIVE POSITIONS - REVERSAL AUDIT]`). If sudden distribution or macro trend breakdown is detected, DeepSeek issues `HARVEST_NOW` (immediate profit preservation exit) or `TIGHTEN_SL` (defensive stop adjustment).
- **Anti-Chase Pullback Limit Pricing**: For extended tokens ($> +3.5\%$ 24h change for LONGs or $< -4.0\%$ for SHORTs), entry levels require deeper discount pullbacks ($0.5\% - 1.0\%$) into EMA21 support/resistance rather than chasing shallow tops.

### 6. Automated Audit Ledger & CSV Export
- `/report` command generates a comprehensive monthly performance audit (win rate, net realized PnL, active/pending counts) and immediately generates and uploads a downloadable `.csv` trade ledger directly to Telegram.

---

## 🔄 System Architecture Flowchart

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               MARKET DATA INGESTION                                    │
│  • Binance Perpetual Tickers (> $1M 24h Vol)                                          │
│  • Microstructure: 20-Level Depth Ratio, Open Interest, Top Trader Long/Short Ratio    │
│  • BTC 1H ADX Regime & 24h Delta                                                       │
│  • Real-Time Macro RSS (Yahoo Finance, Google News, CoinTelegraph) & Fear/Greed Index  │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              DUAL AI REASONING PIPELINE                                │
│                                                                                        │
│   ┌─────────────────────────────────────┐   ┌──────────────────────────────────────┐   │
│   │     DEEPSEEK MACRO STRATEGIST       │   │    WHALE ORDER FLOW RADAR            │   │
│   │  • Trend Momentum & Sweep Reversion │   │  • Liquidation Flush Shorts          │   │
│   │  • Bearish Filter: If BTC < -0.8%   │   │  • Trapped Long / Whale Dump Filters │   │
│   │    --> STRICTLY DISQUALIFY LONGS    │   │  • Bid-Wall Spoofing Elimination     │   │
│   │  • Active Position Reversal Audit   │   │  • Active Position Microstructure    │   │
│   │  • Watchlist Invalidation Audit     │   │  • Watchlist Order Flow Audit        │   │
│   └──────────────────┬──────────────────┘   └───────────────────┬──────────────────┘   │
└──────────────────────┼──────────────────────────────────────────┼──────────────────────┘
                       │                                          │
                       └────────────────────┬─────────────────────┘
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                      EXPECTED PROFIT INDEX & PORTFOLIO GUARD                           │
│  • Compute EV = (Prob × Gain_TP1) - ((1 - Prob) × Risk)                                │
│  • Anti-Chase Entry Discount for Extended Assets (>3.5% Chg)                           │
│  • Confluence Bonus (+0.50 EV if Macro & Whale agree)                                  │
│  • Dynamic Position Sizing (0.5% - 1.0% Risk of Account Balance, capped at 40% Margin) │
│  • Correlation Filter (Max 2 Correlated Positions Per Side)                            │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        SENTINEL RISK & ORDER LIFECYCLE (8s LOOP)                       │
│                                                                                        │
│  [STAGE 0: PENDING LIMIT]                                                              │
│  ├── Pending > 3 Hours TTL               ──► Cancel limit order & free watchlist slot │
│  ├── SL Blown Before Fill                ──► Invalidate & cancel limit order          │
│  └── Price Touches Entry / Binance Fill  ──► Transition to Stage 1 & submit Hard SL   │
│                                                                                        │
│  [STAGE 1+: ACTIVE TRADE DEFENSE]                                                      │
│  ├── BTC Shock Move (>= 2.0% in <5m)     ──► Emergency Move SL to Break-Even           │
│  ├── Gain Reaches +0.6R - +1.0R          ──► Bank 33% Cash + Auto Break-Even on 67%   │
│  ├── Peak Gain >= 1.5% / 2.2% / 3.0%     ──► Ratchet Trailing Floor (+0.6% / 1.2% / 2%)│
│  ├── Sudden Reversal (>=40% drop from PK)──► Reversal Profit Harvest (Market Close)   │
│  ├── Stagnation (> 180m and <= -1.5%)    ──► Diagnostic Alert                          │
│  ├── TP 1 Reached (+1.2R)                ──► Close 50% of Remaining, SL -> BE         │
│  ├── TP 2 Reached (+2.0R)                ──► Close 60% of Remaining, SL -> TP 1 Price  │
│  ├── TP 3 Reached (+3.2R)                ──► Close Remaining Runner, COMPLETED         │
│  └── Stop-Loss Hit                       ──► Fill at SL price, 4h symbol cooldown      │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Execution Modes: Simulation, Testnet & Live

The system execution is configured via the `TRADING_MODE` environment variable in `.env`:

| Parameter Value | Mode Name | Behavior |
| :--- | :--- | :--- |
| `SIMULATION` *(Default)* | **Paper Tracking** | Real-time market monitoring with zero financial risk. Fills, stop-outs, and partial take-profits are tracked virtually in SQLite. Dynamic account balance updates with realized PnL. |
| `TESTNET` | **Binance Testnet** | Connects to the Binance USDT-M Futures Testnet. Submits real testnet limit orders, `STOP_MARKET` risk stops, and partial market take-profits. |
| `LIVE` | **Production Trading** | Connects to live Binance USDT-M Futures. Automatically sets leverage (`DEFAULT_LEVERAGE`), margin mode (`ISOLATED`), places GTC limit orders, attaches hard stop-loss brackets, and executes partial exits. |

---

## 📊 Quantitative Strategy & AI Radar

### Deterministic Risk & Target Formulation
All trade setups calculate dynamic brackets anchored to 1-hour ATR ($\text{ATR}_{14}$):
- **Risk Distance:** $\max(1.5 \times \text{ATR}_{1h}, \text{Price} \times 1.5\%)$, capped at $\text{Price} \times 2.8\%$.
- **TP 1:** $+1.2\text{R}$ (50% position scale-out, triggers Break-Even SL).
- **TP 2:** $+2.0\text{R}$ (30% position scale-out, trails SL to TP 1).
- **TP 3:** $+3.2\text{R}$ (final 20% position scale-out).

### Smart Entry Placement
- **Trend Momentum:** Long entries place limit orders at pullbacks to the 21 EMA or discount zones ($\approx 0.2\% - 0.4\%$ below market) rather than chasing green candles. Shorts place limit orders at bounce zones.
- **Sweep Reversion:** Used during low-volume or weekend regimes to fade range extremes.

### Bearish Macro Overrule
When BTC 24-hour change drops below **$-0.8\%$**:
- All altcoin LONG proposals are **automatically disqualified**.
- The bot switches exclusively to high-beta SHORT breakdowns (assets with weak book ratios or underperforming BTC).

---

## 🛡️ Risk Defense Sentinel & Order Lifecycle

The Sentinel runs every **8 seconds** independently of the hourly scanner:

```
           STAGE 0                    STAGE 1                   STAGE 2               STAGE 3
       [Pending Limit]            [Entry Filled]               [TP 1 Hit]            [TP 2 Hit]
              │                          │                          │                     │
    • TTL Expiry (3h)          • Hard SL Active           • Close 50% Qty       • Close 30% Qty
    • Pre-fill SL Check        • Auto BE @ +0.6R          • SL -> Break-Even    • SL -> TP 1 Price
    • Macro Shock Cancel       • Peak Giveback Exit                 │                     │
              │                • Stagnation Check                   ▼                     ▼
              ▼                          │                      [TP 2 Hit]            [TP 3 Hit]
     [Order Triggered] ──────────► [Active Defense]           (Trails SL)         (Close Final 20%)
```

---

## 📱 Telegram Command Directory

| Command | Syntax / Example | Description |
| :--- | :--- | :--- |
| `/start` | `/start` | Activates bot state, resumes automated hourly scans, and displays system health. |
| `/stop` | `/stop` | Pauses automated discovery scans. Sentinel continues monitoring open positions. |
| `/scan` | `/scan` | Triggers an on-demand unified CEX technical and DeepSeek AI macro discovery scan. |
| `/whale` | `/whale` | Triggers an on-demand Binance derivatives Whale & Institutional Order Flow radar scan. |
| `/scalp` | `/scalp` | Triggers an on-demand High-Speed Scalp Sniper scan on Binance high-volume perps. |
| `/report` | `/report` | Displays the monthly performance audit and attaches a downloadable `.csv` trade ledger. |
| `/watchlist` | `/watchlist` | Displays all tracked pending orders and active positions with speed tags and SL/TP levels. |
| `/addwatch` | `/addwatch SOL LONG`<br>`/addwatch ETH SHORT 2500 2560 2420 SLOW` | Manually tracks a coin. If prices are omitted, ATR brackets are calculated automatically. |
| `/enter` | `/enter BTC` | Promotes a pending limit setup to an active position at current market price. |
| `/unwatch` | `/unwatch PEPE` | Removes a coin from active tracking and cancels any pending exchange orders. |
| `/clearwatch` | `/clearwatch` | Flushes all active watchlist entries to free capacity. |
| `/logs` | `/logs` | Displays the last 10 system log entries directly in Telegram. |

---

## 🛠️ Environment Configuration & Parameter Guide

### Environment Variables (.env)

```env
# ==============================================================================
# REQUIRED CREDENTIALS
# ==============================================================================
DEEPSEEK_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789

# ==============================================================================
# TRADING EXECUTION MODE
# Options: SIMULATION | TESTNET | LIVE
# ==============================================================================
TRADING_MODE=SIMULATION

# ==============================================================================
# BINANCE API CREDENTIALS (Required for TESTNET and LIVE)
# ==============================================================================
BINANCE_API_KEY=
BINANCE_SECRET_KEY=

# ==============================================================================
# PORTFOLIO RISK & ENGINE CONFIGURATION
# ==============================================================================
DEFAULT_LEVERAGE=2
MAX_CORRELATED_POSITIONS_PER_SIDE=2
MAX_CONCURRENT_POSITIONS=30
MAX_DAILY_TRADES=999999
MIN_RRR=1.8
CONFIDENCE_THRESHOLD=0.75
MAX_24H_LOSS_THRESHOLD=-25.0
ACCOUNT_BALANCE_SIM=10000.0

# ==============================================================================
# OPERATIONAL CADENCE & DEFENSE THRESHOLDS
# ==============================================================================
SCAN_INTERVAL_SECONDS=3600
WATCH_INTERVAL_SECONDS=8
PENDING_TTL_HOURS=3
STAGNATION_MINS=180.0
STAGNATION_PNL_FLOOR=-1.5
WHALE_MIN_PROBABILITY=75.0

# ==============================================================================
# STORAGE & SYSTEM
# ==============================================================================
DATA_DIR=/app/data
PORT=8000
```

---

### Environment Parameter Reference Table

| Parameter | Type | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `DEEPSEEK_API_KEY` | `string` | *(Required)* | DeepSeek API key (`https://api.deepseek.com`) powering macro and whale AI analysis. |
| `TELEGRAM_BOT_TOKEN` | `string` | *(Required)* | Telegram Bot token from [@BotFather](https://t.me/BotFather). |
| `TELEGRAM_CHAT_ID` | `string` | *(Required)* | Authorized Telegram User or Group Chat ID. Rejects unauthorized users. |
| `BINANCE_API_KEY` | `string` | `""` | Binance API key with Futures trading permission (Required for `TESTNET` and `LIVE`). |
| `BINANCE_SECRET_KEY` | `string` | `""` | Binance API secret key (Required for `TESTNET` and `LIVE`). |
| `TRADING_MODE` | `string` | `"SIMULATION"` | Execution mode: `SIMULATION`, `TESTNET`, or `LIVE`. |
| `DEFAULT_LEVERAGE` | `integer` | `2` | Default leverage set on the exchange (`ISOLATED` margin) for macro/swing setups. |
| `SCALP_LEVERAGE` | `integer` | `5` | Dedicated aggressive leverage multiplier for High-Speed Scalp Sniper trades. |
| `SCALP_ENTRY_TYPE` | `string` | `"MARKET"` | Entry style for scalps: `MARKET` (immediate fill at current price) or `LIMIT` (0.02% touch). |
| `MAX_CORRELATED_POSITIONS_PER_SIDE` | `integer` | `3` | Max concurrent open swing trades on one side (max 3 LONGs or max 3 SHORTs). |
| `MAX_CORRELATED_SCALPS_PER_SIDE` | `integer` | `4` | Max concurrent open scalp trades on one side for rapid turnover. |
| `MAX_CONCURRENT_POSITIONS` | `integer` | `30` | Maximum simultaneous watchlist capacity across pending and live orders. |
| `MAX_DAILY_TRADES` | `integer` | `999999` | Daily trade execution cap within a rolling 24-hour window. |
| `MIN_RRR` | `float` | `1.8` | Minimum Reward-to-Risk ratio filter for signal validation. |
| `CONFIDENCE_THRESHOLD` | `float` | `0.75` | Minimum quant score ($75\%$) required for candidate setup eligibility. |
| `SCALP_MIN_PROBABILITY` | `float` | `70.0` | Minimum probability % required for High-Speed Scalp Sniper setups. |
| `MAX_24H_LOSS_THRESHOLD` | `float` | `-25.0` | 24-hour drawdown circuit breaker floor in % (triggers 4-hour scan pause). |
| `ACCOUNT_BALANCE_SIM` | `float` | `10000.0` | Simulated baseline balance in USD for paper trading and position scaling. |
| `SCAN_INTERVAL_SECONDS` | `integer` | `3600` | Automated discovery scan interval in seconds (default: 1 hour). |
| `WATCH_INTERVAL_SECONDS` | `integer` | `8` | Sentinel high-frequency monitoring loop in seconds (default: 8s). |
| `PENDING_TTL_HOURS` | `integer` | `3` | Time-to-Live in hours for unfilled Stage 0 limit orders before cancellation. |
| `STAGNATION_MINS` | `float` | `180.0` | Duration in minutes before flagging a position as stagnant (3 hours). |
| `STAGNATION_PNL_FLOOR` | `float` | `-1.5` | PnL threshold below which an aged position triggers stagnation warnings. |
| `WHALE_MIN_PROBABILITY` | `float` | `75.0` | Minimum probability % required for whale order-flow setups. |
| `DATA_DIR` | `string` | `"."` or `"/app/data"` | Directory where SQLite database and file exports are stored. |
| `PORT` | `integer` | `8000` | HTTP port for FastAPI ASGI supervisor health checks. |

---

### Detailed Parameter Explanations

#### Credentials & Authentication
- **`DEEPSEEK_API_KEY`**: Authenticates requests to DeepSeek's OpenAI-compatible endpoint. The system uses model `deepseek-chat` with low temperatures ($0.10 - 0.15$) for deterministic structured JSON generation.
- **`TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHAT_ID`**: Forms an authentication wall. Any message or command from an ID that does not match `TELEGRAM_CHAT_ID` is silently dropped, protecting against unauthorized access.
- **`BINANCE_API_KEY` & `BINANCE_SECRET_KEY`**: Used by CCXT (`ccxt.binanceusdm`). In `SIMULATION` mode, public endpoints are accessed without keys. In `TESTNET` or `LIVE` modes, valid keys with Futures permissions are required.

#### Execution & Leverage Control
- **`TRADING_MODE`**:
  - `SIMULATION`: 100% paper tracking. Orders are tracked in SQLite; balance updates virtually.
  - `TESTNET`: CCXT runs in sandbox mode (`set_sandbox_mode(True)`), placing live orders on Binance Futures Testnet.
  - `LIVE`: Submits real orders, hard stop-losses, and partial take-profits on live Binance USDT-M Perpetual Futures.
- **`DEFAULT_LEVERAGE`**: Configured on each traded symbol via CCXT before order placement. Defaults to conservative `2` (2x leverage) in `ISOLATED` margin mode.

#### Portfolio Risk & Gate Parameters
- **`MAX_CORRELATED_POSITIONS_PER_SIDE`**: Guards against directional correlation risk. If the bot already holds 2 active LONGs, any new LONG setup is bypassed regardless of its score, preserving capital for uncorrelated opportunities.
- **`MAX_CONCURRENT_POSITIONS`**: Hard ceiling on watchlist capacity (default `30`). When reached, new setups are ignored until older positions complete or expire.
- **`CONFIDENCE_THRESHOLD`**: Filters out noisy setups. The objective quant scoring algorithm must yield at least `0.75` ($75\%$) for a setup to be admitted to candidate ranking.
- **`MAX_24H_LOSS_THRESHOLD`**: Rolling 24-hour circuit breaker. If closed trades over the past 24 hours produce a net loss equal to or worse than `-25.0%`, the system automatically pauses new scans for 4 hours.
- **`ACCOUNT_BALANCE_SIM`**: Starting paper capital baseline ($10,000 USD). In `SIMULATION` mode, this baseline is continuously multiplied by $(1.0 + \frac{\text{realized\_pnl}}{100.0})$ to simulate compound equity growth.

#### Operational Timing & Defense Durations
- **`SCAN_INTERVAL_SECONDS`**: Interval for the scheduled job queue that runs `execute_unified_hourly_discovery_pipeline`. Default `3600` seconds (1 hour).
- **`WATCH_INTERVAL_SECONDS`**: The heart of the Sentinel. A tight asynchronous loop that checks all open positions and pending orders every `8` seconds.
- **`PENDING_TTL_HOURS`**: Prevents stale limit orders. Unfilled limit orders older than `3` hours are cancelled on Binance and pruned from the watchlist.
- **`STAGNATION_MINS` & `STAGNATION_PNL_FLOOR`**: Identifies capital traps. If an open trade lasts longer than `180` minutes and is sitting at or below `-1.5%` unrealized PnL, the bot sends an advisory alert to Telegram and logs a diagnostic entry.
- **`WHALE_MIN_PROBABILITY`**: Minimum movement probability required from the AI Whale Microstructure Radar (default `75.0%`).

---

## 🔬 Quantitative & Algorithmic Parameter Specifications

Beyond environment variables, the quantitative and risk defense algorithms in `ai_scanner.py` and `sentinel.py` utilize the following deterministic mathematical parameters:

### 1. Dynamic Risk Bracket & Target Formulation
Calculated via `calculate_deterministic_trade_levels()`:
- **ATR Window:** 14 periods on 1-hour candles ($\text{ATR}_{14, 1h}$).
- **Raw Risk Distance:** $\text{Raw Risk} = 1.5 \times \text{ATR}_{1h}$.
- **Risk Floor & Ceiling:**
  $$\text{Risk Distance} = \min(\max(\text{Raw Risk}, \text{Price} \times 1.5\%), \text{Price} \times 2.8\%)$$
  *(Constrains stop distance between 1.5% and 2.8% to balance volatility against liquidation risk).*
- **Asymmetric Reward Multipliers:**
  - **TP 1:** $+1.2\text{R}$ (Closes 50% position quantity; moves SL to Break-Even).
  - **TP 2:** $+2.0\text{R}$ (Closes 30% position quantity; trails SL to TP 1 price).
  - **TP 3:** $+3.2\text{R}$ (Closes remaining 20% position quantity; trade completes).
- **Limit Entry Pullback Anchoring (Trend Momentum):**
  - **LONG Limit Entry:** Placed between $0.9960 \times \text{Price}$ and $0.9985 \times \text{Price}$ (or anchored to the 21 EMA) to avoid buying local peaks.
  - **SHORT Limit Entry:** Placed between $1.0015 \times \text{Price}$ and $1.0040 \times \text{Price}$ (or anchored to the 21 EMA) to avoid shorting local bottoms.

---

### 2. Objective Quant Scoring Weights & Penalties
Calculated via `calculate_objective_quant_score()`:
- **Base Score:** Starts at `0.50`.
- **Macro Directional Gate:**
  - If direction is `LONG` and BTC 24h change $< -0.8\%$, score is forced to `0.10` *(Automatic Disqualification)*.
  - If direction is `SHORT` and BTC 24h change $> +2.0\%$, score is forced to `0.10` *(Automatic Disqualification)*.
- **Confluence Bonuses:**
  - BTC alignment (e.g. BTC up $\ge +0.5\%$ for LONGs, down $\le -1.0\%$ for SHORTs): $+0.15$ to $+0.25$.
  - Relative strength/weakness vs BTC: $+0.15$ to $+0.20$.
  - 1H RSI sweet spot ($42.0 \le \text{RSI} \le 62.0$ for LONGs, $36.0 \le \text{RSI} \le 55.0$ for SHORTs): $+0.15$.
  - 4H RSI trend confluence: $+0.15$.
  - Funding rate neutrality: $+0.10$.
  - Order book depth ratio ($> 1.15$ for LONGs, $< 0.85$ for SHORTs): $+0.10$.
  - Volume expansion ($\ge \$35\text{M}$ 24h vol: $+0.10$, $\ge \$10\text{M}$: $+0.05$).
- **Chase & Exhaustion Penalties:**
  - 1H RSI overbought ($> 67.0$) or 24h gain $\ge 10.0\%$ on LONGs: $-0.35$.
  - 1H RSI oversold ($< 33.0$) or 24h drop $\le -9.0\%$ on SHORTs: $-0.35$.

---

### 3. Dynamic Capital Allocation & Position Sizing
Calculated via `calculate_dynamic_position_size()`:
Risk per trade is dynamically scaled according to the quant confidence score:
- **Tier 1 ($\text{Score} < 0.78$):** Allocates $0.50\%$ risk of account equity.
- **Tier 2 ($0.78 \le \text{Score} < 0.85$):** Allocates $0.75\%$ risk of account equity.
- **Tier 3 ($\text{Score} \ge 0.85$):** Allocates $1.00\%$ risk of account equity.
- **Position Size Calculation:**
  $$\text{Position Size USD} = \frac{\text{Account Balance} \times \text{Risk Allocation \%}}{\text{Risk \%}} \times 2.0$$
- **Hard Margin Ceiling:** Capped at a maximum of **$40\%$ of total account balance** to preserve margin and prevent over-leverage.

---

### 4. Expected Profit Index (EV) & Confluence Bonuses
Candidates are sorted in descending order by their Expected Profit Index:
$$\text{EV} = (\text{Quant Score} \times \text{Gain}_{\text{TP1}}\%) - ((1.0 - \text{Quant Score}) \times \text{Risk}\%)$$
- **Confluence Bonus (+0.50 EV):** Awarded if both the DeepSeek Macro AI and the Whale Order Flow radar independently select the same symbol.
- **Order Book Depth Bonus (+0.25 EV):** Awarded if depth ratio confirms direction (book ratio $< 0.70$ for shorts, $> 1.30$ for longs).
- **Whale Alignment Bonus (+0.30 EV):** Awarded if top trader long/short ratio $> 1.8$ while BTC is falling (trapped retail/whale longs ripe for liquidation flush).

---

### 5. Sentinel Dynamic Regime Risk Trigger Thresholds
Executed in real-time every 8 seconds via `sentinel.py`:
- **BTC Flash Shock Trigger:**
  - $\pm 2.0\%$ price shift within $\le 300$ seconds (5 minutes).
  - *Action:* Automatically cancels opposing Stage 0 limit orders; tightens opposing Stage 1+ open positions to Break-Even.
- **Regime-Adaptive Auto Break-Even:**
  - **Trending Regime (Weekdays):** Triggers at $+1.0\text{R}$ gain. Moves Stop-Loss to $\text{Entry} \pm 0.15\%$ (covering taker fees) without choking normal micro-retracements.
  - **Sideways / Chop Regime:** Triggers early at $+0.6\text{R}$ gain.
  - **Weekend Anomaly Regime:** Triggers immediately at $+0.5\text{R}$ gain.
- **Regime-Adaptive Sudden Reversal / Peak Harvest:**
  - **Trending Regime:** Exits if peak gain $\ge +2.2\%$ and pullbacks $\ge 1.0\%$ (or peak $\ge +2.8\%$ with $\ge 1.2\%$ giveback), protecting big runners while letting TP1 (+1.2R, $\approx +3.3\%$) execute cleanly.
  - **Sideways Regime:** Exits if peak gain $\ge +1.4\%$ and pullbacks $\ge 0.5\%$.
  - **Weekend Anomaly:** Exits aggressively if peak gain $\ge +1.0\%$ and pullbacks $\ge 0.4\%$.
- **Adaptive Stagnation Timeout:**
  - 180 minutes in Trending, 90 minutes in Sideways Chop, and 60 minutes in Weekend Anomaly.
- **Symbol Cooldown Duration:**
  - 4 hours timeout applied to any asset that triggers its Stop-Loss, preventing revenge re-entries.
- **Circuit Breaker Timeout:**
  - 4 hours total pause on scanning if rolling 24-hour drawdown reaches `MAX_24H_LOSS_THRESHOLD`.

---

### 6. Weekend Regime Schedule & Global Financial News Ingestion
- **Weekend Anomaly Active Hours:** Friday 22:00 UTC through Sunday 22:00 UTC (aligning with CME Futures weekend closure and reduced institutional liquidity).
- **Enforcement:** Strictly disables `TREND_MOMENTUM` breakout strategies. Forces `SWEEP_REVERSION` (fading range boundaries) and tighter profit preservation stops.
- **Global Financial News Ingestion:**
  - Real-time RSS scraping of Yahoo Finance Top Business News and Google News Macro / Fed Search feeds.
  - Automatic keyword detection for high-impact macroeconomic catalysts (`FOMC`, `FED`, `RATE HIKE/CUT`, `POWELL`, `CPI`, `INFLATION`, `PAYROLLS`, `NFP`, `WAR`, `TARIFFS`).
  - Automatically classifies market sentiment and directs DeepSeek AI to avoid counter-trend exposures during high-volatility events.

---

## 🗄️ Database & Performance Audit Ledger

All runtime states are persisted in **`scanner_memory.db`** (SQLite with WAL mode and 60-second busy timeout):

- **`watched_signals`**: Tracks active orders, stage progression (0 to 3), Binance order IDs, trailing stops, and realized PnL.
- **`signal_history`**: Permanent archive of all signals generated by AI Macro and Whale Flow scanners.
- **`loss_diagnostics`**: Post-mortem audit records for stopped-out and stagnant positions.
- **`symbol_cooldowns`**: Enforces 4-hour cooldown timeouts on stopped-out symbols.
- **`ai_audit_logs`**: Stores raw prompts, completion payloads, and execution latencies for DeepSeek AI auditing.
- **`system_logs`**: Operational events, auto-pruned past 48 hours to prevent database bloat.
- **`bot_state`**: Cross-process coordination flags (`scanning_active`, `circuit_breaker_until`, `eval_running`).

---

## 📦 Deployment Guide

### Option 1: Docker Compose (Recommended)

1. Clone repository and navigate to folder:
   ```bash
   git clone https://github.com/detoir/babi_ngepet_02.git
   cd babi_ngepet_02
   ```

2. Create `.env` file:
   ```bash
   cp .env.example .env
   nano .env
   ```

3. Build and run container:
   ```bash
   docker compose up -d --build
   ```

4. Inspect live logs:
   ```bash
   docker compose logs -f
   ```

---

### Option 2: Cloud Container Platforms (Railway, K3s, Render)

The service includes an ASGI HTTP responder on `http://0.0.0.0:8000/health`:

1. Connect your Git repository to **Railway** or **Render**.
2. Set Environment Variables (`DEEPSEEK_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TRADING_MODE`, etc.).
3. Configure persistent storage:
   - Mount Volume Path: `/app/data`
4. Health Check Path: `/health` (Port: `8000`).

---

### Option 3: Direct Linux VPS (Systemd)

1. Install Python 3.11 and system dependencies:
   ```bash
   sudo apt update && sudo apt install -y python3-pip python3-venv sqlite3 curl
   mkdir -p /opt/babi_ngepet_02 && cd /opt/babi_ngepet_02
   ```

2. Clone repository & configure virtual environment:
   ```bash
   git clone https://github.com/detoir/babi_ngepet_02.git .
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. Create systemd service `/etc/systemd/system/babi_ngepet_02.service`:
   ```ini
   [Unit]
   Description=Babi Ngepet 02 Market Intelligence & Risk Sentinel
   After=network.target

   [Service]
   Type=simple
   User=root
   WorkingDirectory=/opt/babi_ngepet_02
   EnvironmentFile=/opt/babi_ngepet_02/.env
   ExecStart=/opt/babi_ngepet_02/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
   Restart=always
   RestartSec=10
   StandardOutput=journal
   StandardError=journal

   [Install]
   WantedBy=multi-user.target
   ```

4. Enable and start:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable babi_ngepet_02
   sudo systemctl start babi_ngepet_02
   ```

---

## 🔒 Safety & Risk Disclaimer

- **Execution Safety:** When running in `SIMULATION` mode, no live orders or exchange balances are touched. When switching to `LIVE`, ensure you have tested order execution thoroughly on `TESTNET`.
- **Authentication Guard:** Only requests matching `TELEGRAM_CHAT_ID` are processed. All unauthorized interactions are discarded.
- **Financial Disclaimer:** Cryptocurrency perpetual futures trading carries substantial financial risk. This software is provided for quantitative research, market intelligence, and automated risk management. Past performance does not guarantee future results.

---

## 📄 License

Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for complete terms.