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
from contextlib import asynccontextmanager
from fastapi import FastAPI

processes = []

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

    print("[SUPERVISOR] ==========================================")
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
        "active_workers": len([p for p in processes if p.poll() is None])
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
