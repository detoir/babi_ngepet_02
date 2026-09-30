```python
readme_text = """# 📡 BABI_NGEPET_01: Autonomous Crypto Perpetual Market Intelligence & Movement Scanner

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Railway](https://img.shields.io/badge/deploy-railway-0B0D0E.svg?logo=railway)](https://railway.app/)

**`babi_ngepet_01`** is an autonomous, read-only quant intelligence bot designed to scan the entire Binance USDT-M Perpetual Futures market. Built upon the quantitative core of `babi_ngepet_00`, it filters opportunities with mathematical edge (Win Probability $\\ge 85\\%$, Reward-to-Risk $R:R \\ge 1:1.5$) and tracks the full simulated lifecycle of every signal (Entry $\\rightarrow$ TP 1 $\\rightarrow$ TP 2 $\\rightarrow$ TP 3 $\\rightarrow$ Free TP Runner & Trailed SL) in real time without executing live orders.

---

## 🏗️ Architecture & Strategy Overview


```

```
                  ┌────────────────────────────────────────────────────────┐
                  │              BINANCE FUTURES DATA INGESTION            │
                  │  • Dynamic Volume Screener (Quote Vol > $5M)           │
                  │  • 4H Structural Anchor Gate (Green/Red)               │
                  │  • 1H Timing & TradingView TA Oscillators              │
                  │  • Global Macro Headlines & Crypto Sentiment RSS       │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
                  ┌────────────────────────────────────────────────────────┐
                  │              DEEPSEEK AI REASONING ENGINE              │
                  │  • Confluence Filter: Win Probability >= 85%           │
                  │  • Mathematical Expectancy Check: R:R >= 1.5           │
                  │  • 4-Tier Exit Formulation (TP1 / TP2 / TP3 / Free TP) │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
                  ┌────────────────────────────────────────────────────────┐
                  │           5-MINUTE REAL-TIME MOVEMENT SENTINEL         │
                  │                                                        │
                  │  1. Pending Entry Hit  ──► Alert: "Entry Triggered"    │
                  │  2. TP 1 Hit (33%)     ──► Alert: "TP1 Hit! SL -> B.E" │
                  │  3. TP 2 Hit (33%)     ──► Alert: "TP2 Hit! SL -> TP1" │
                  │  4. TP 3 Hit (34%)     ──► Alert: "TP3 Hit! SL -> TP2" │
                  │  5. Free TP Runner     ──► Alert: "Free TP Milestone"  │
                  │  6. Stop-Loss Trigger  ──► Alert: "Setup Invalidated"  │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
                  ┌────────────────────────────────────────────────────────┐
                  │                 TELEGRAM USER INTERFACE                │
                  │   Interactive Bot Commands, Menu & Instant Alerts      │
                  └────────────────────────────────────────────────────────┘

```

```

---

## 🚀 Key Features

* **Zero Capital Risk (100% Read-Only):** No live exchange order execution. Uses public market data endpoints.
* **Dual-Timeframe Quantitative Alignment:**
  * **4-Hour Anchor:** Requires structural confirmation and a volume expansion surge $\\ge 1.5\\times$ the 20-period 4H SMA.
  * **1-Hour Execution:** Timing validated via RSI(14), MACD, and EMAs (20, 50, 200).
* **Multi-Stage Staged Trailing Architecture:**
  * **TP 1 (33%):** Automatically trails the reference Stop-Loss to **Break-Even + Fees ($+0.12\\%$)**.
  * **TP 2 (33%):** Trails Stop-Loss to the **TP 1 Level**, locking in guaranteed multi-R profits.
  * **TP 3 (34%):** Trails Stop-Loss to the **TP 2 Level**.
  * **Free TP Runner:** Tracks extended volatility runs via $2.0\\times\\text{ ATR}_{14}$ dynamic trailing.
* **Active Watchlist Management:** Add, inspect, remove (`/unwatch`), or clear tracked coins on the fly. Removed coins automatically re-register if discovered in future scans.
* **Rolling 24-Hour Logging:** Auto-pruning SQLite database with direct `.log` file export via `/download_logs`.
* **Telegram Command Menu:** Interactive clickable menu pre-configured on startup.

---

## 📱 Telegram Command Directory

| Command | Description |
| :--- | :--- |
| `/start` | Overview of bot status, tracking rules, and commands. |
| `/help` | Detailed operational guide and command manual. |
| `/scan` | Scans all liquid perpetuals concurrently for setups with $\\ge 85\\%$ win probability and $R:R \\ge 1:1.5$. |
| `/watchlist` | Displays all actively monitored setups with current stage and trailed Stop-Loss. |
| `/unwatch <COIN>` | Removes a coin from active watch (e.g., `/unwatch PEPE` or `/watchlist rm SOL`). |
| `/analyze <COIN>` | Generates an on-demand Trade Card for any target asset (e.g., `/analyze BTC`). |
| `/macro` | Displays the Fear & Greed Index, Federal Reserve headlines, and crypto sentiment. |
| `/start_scan` | Resumes automated hourly AI discovery scans and movement alert dispatch. |
| `/stop_scan` | Pauses automated scanning and background notifications. |
| `/rules` | Views active user-defined strategy directives. |
| `/reset_rules` | Resets custom directives back to quantitative baseline. |
| `/logs [FILTER]` | Displays a quick 24-hour log preview (e.g., `/logs error`, `/logs 20`). |
| `/download_logs` | Downloads the full 24-hour auto-pruned `.log` export file directly in chat. |

---

## 🛠️ Environment Configuration

Create a `.env` file in the project root:

```env
# ==========================================
# REQUIRED CREDENTIALS
# ==========================================
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789
DEEPSEEK_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# ==========================================
# OPTIONAL PARAMETERS
# ==========================================
BINANCE_API_KEY=
BINANCE_SECRET_KEY=
CRYPTOPANIC_API_KEY=
MARKET_POOL_SIZE=40
DATA_DIR=./data

```

---

## 📦 Deployment Guide

### Option 1: Docker Compose (Recommended)

1. Clone the repository:
```bash
git clone [https://github.com/YOUR_USERNAME/babi_ngepet_01.git](https://github.com/YOUR_USERNAME/babi_ngepet_01.git)
cd babi_ngepet_01

```


2. Configure environment variables:
```bash
cp .env.example .env
nano .env

```


3. Build and launch the container in the background:
```bash
docker compose up -d --build

```


4. View real-time logs:
```bash
docker logs -f babi_ngepet_01_scanner

```



---

### Option 2: Deploy to Railway.app

1. Fork or push this repository to your private GitHub account.
2. Log in to [Railway.app](https://www.google.com/search?q=https%3A%2F%2Frailway.app%2F) $\rightarrow$ **+ New Project** $\rightarrow$ **Deploy from GitHub repo**.
3. Select your `babi_ngepet_01` repository.
4. Navigate to **Variables** and add:
* `TELEGRAM_BOT_TOKEN`
* `TELEGRAM_CHAT_ID`
* `DEEPSEEK_API_KEY`
* `DATA_DIR=/app/data`


5. Under service settings, attach a **Persistent Volume** with mount path `/app/data` to preserve database history across deploys.

---

### Option 3: Direct Linux VPS (Systemd Service)

1. Install Python 3.11 and virtual environment:
```bash
sudo apt update && sudo apt install -y python3-pip python3-venv sqlite3
mkdir -p /opt/babi_ngepet_01 && cd /opt/babi_ngepet_01

```


2. Initialize virtual environment and dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

```


3. Create systemd service file:
```bash
sudo nano /etc/systemd/system/babi_ngepet_01.service

```


Paste the following service configuration:
```ini
[Unit]
Description=Babi Ngepet 01 Market Intelligence & Movement Scanner
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/babi_ngepet_01
EnvironmentFile=/opt/babi_ngepet_01/.env
ExecStart=/opt/babi_ngepet_01/venv/bin/python /opt/babi_ngepet_01/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target

```


4. Enable and start service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable babi_ngepet_01
sudo systemctl start babi_ngepet_01

```



---

## 🔒 Security & Privacy

* **Read-Only Scope:** Does not make withdrawal, transfer, or trade execution calls.
* **Authentication Guard:** Only queries originating from the designated `TELEGRAM_CHAT_ID` are processed.
* **Auto-Pruning Ledger:** Eliminates database bloat by pruning logs older than 24 hours automatically.

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
"""

with open("README.md", "w", encoding="utf-8") as f:
f.write(readme_text.strip() + "\n")

print("File generated successfully.")

```

```text?code_stdout&code_event_index=1
File generated successfully.


```

Your **`README.md`** file has been generated and is ready for download:

* **Download Link:** [README.md](https://www.google.com/search?q=sandbox:/README.md)

---

### Included Contents Overview

* **Repository Badges & Header:** Python 3.11+, MIT License, Docker, and Railway deployment status tags.
* **Architecture ASCII Flowchart:** Data ingestion $\to$ DeepSeek reasoning $\to$ 5-minute movement sentinel $\to$ Telegram user interface.
* **Core Strategy Specifications:** 4H structural anchor gating, 1H TradingView indicator confluence, multi-stage TP ladders (TP 1–3 + Free TP runner), and simulated risk-free trailing stops.
* **Telegram Command Directory:** Interactive menu references (`/scan`, `/watchlist`, `/unwatch`, `/analyze`, `/macro`, `/logs`, `/download_logs`, `/help`).
* **Deployment Guides:** Complete configurations for Docker Compose, Railway.app (with persistent volume setup), and Linux systemd services.