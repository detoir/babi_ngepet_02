"""
================================================================================
MODULE: main.py
ROLE  : Container Supervisor + Instant ASGI Health Responder for K3s / Helipod
================================================================================
"""

import sys
import subprocess
import time
import signal
import os
import asyncio
import urllib.request
from contextlib import asynccontextmanager
from fastapi import FastAPI

processes = []
OUTBOUND_IP = "Unknown"

def fetch_public_ip() -> str:
    global OUTBOUND_IP
    services = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]
    for url in services:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/7.68.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                ip = resp.read().decode("utf-8").strip()
                if ip:
                    OUTBOUND_IP = ip
                    return ip
        except Exception:
            continue
    OUTBOUND_IP = "Unable to determine (check network)"
    return OUTBOUND_IP

def terminate_all_children():
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass

def signal_handler(sig, frame):
    print("[SUPERVISOR] Termination signal received. Cleaning up child processes...")
    terminate_all_children()
    sys.exit(0)

async def process_supervisor_loop():
    """Background task that starts and monitors sentinel.py and ai_scanner.py."""
    await asyncio.sleep(2)
    
    python_executable = sys.executable

    # Pre-flight environment check
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not tg_token:
        print("[SUPERVISOR] ⚠️ WARNING: TELEGRAM_BOT_TOKEN is not set in Environment Variables!")
        print("[SUPERVISOR] ⚠️ ai_scanner.py will fail to start without a valid Telegram Bot Token.")

    print("[SUPERVISOR] ==========================================")
    print(f"[SUPERVISOR] Public Outbound IP for Binance Whitelist: {OUTBOUND_IP}")
    print(f"[SUPERVISOR] Starting Trading Workers on Python {sys.version.split()[0]}")
    print("[SUPERVISOR] ==========================================")

    try:
        p_sentinel = subprocess.Popen([python_executable, "sentinel.py"])
        processes.append(p_sentinel)
        print("[SUPERVISOR] Started sentinel.py")
    except Exception as e:
        print(f"[SUPERVISOR] Failed to launch sentinel.py: {e}")

    try:
        p_scanner = subprocess.Popen([python_executable, "ai_scanner.py"])
        processes.append(p_scanner)
        print("[SUPERVISOR] Started ai_scanner.py")
    except Exception as e:
        print(f"[SUPERVISOR] Failed to launch ai_scanner.py: {e}")

    while True:
        await asyncio.sleep(5)
        for i, p in enumerate(processes):
            poll = p.poll()
            if poll is not None:
                script_name = "sentinel.py" if i == 0 else "ai_scanner.py"
                print(f"[SUPERVISOR] ⚠️ Worker {script_name} exited with code {poll}. Respawning in 5s...")
                await asyncio.sleep(5)
                new_p = subprocess.Popen([python_executable, script_name])
                processes[i] = new_p

@asynccontextmanager
async def lifespan(app: FastAPI):
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Detect Outbound IP at boot
    ip = fetch_public_ip()
    print("=" * 60)
    print(f"🚀 HELIPOD OUTBOUND IP (FOR BINANCE API WHITELIST): {ip}")
    print("=" * 60)

    supervisor_task = asyncio.create_task(process_supervisor_loop())
    yield
    supervisor_task.cancel()
    terminate_all_children()

app = FastAPI(title="babi_ngepet_01 Supervisor", lifespan=lifespan)

@app.get("/")
@app.get("/health")
def health():
    return {
        "status": "online",
        "bot": "babi_ngepet_01",
        "outbound_ip": OUTBOUND_IP,
        "active_workers": len([p for p in processes if p.poll() is None])
    }

@app.get("/ip")
def get_ip():
    return {
        "outbound_ip": OUTBOUND_IP,
        "message": "Use this IP address in your Binance API Whitelist"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
