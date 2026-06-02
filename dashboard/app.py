import asyncio
import aiohttp
import json
import os
import sys
import time
import traceback
import xml.etree.ElementTree as ET
import yaml
import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from redis import asyncio as aioredis
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Vortex Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    from fastapi.responses import Response
    content = Path(__file__).parent.joinpath("static", "index.html").read_bytes()
    return Response(content=content, media_type="text/html",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})

BASE = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE / "config" / "config.yaml"
ENV_PATH = BASE / ".env"
WATCHLIST_PATH = BASE / "config" / "watchlist.yaml"
LOG_PATH = BASE / "data" / "vortex.log"

config_cache = {}
db_conn = None
redis_conn = None


def load_env():
    if not ENV_PATH.exists():
        return {}
    env = {}
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def load_config():
    global config_cache
    if not CONFIG_PATH.exists():
        return {"error": "config.yaml not found"}
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    env = load_env()
    profile = env.get("ACTIVE_PROFILE", "standard")
    if profile in cfg.get("profiles", {}):
        p = cfg["profiles"][profile]
        if "grid" in p:
            cfg["grid"].update(p["grid"])
        if "strategy" in p:
            for k, v in p["strategy"].items():
                if k in cfg["strategy"] and isinstance(v, dict):
                    cfg["strategy"][k].update(v)
                else:
                    cfg["strategy"][k] = v
        if "risk" in p:
            cfg["risk"].update(p["risk"])
    if "timescaledb" in cfg:
        cfg["timescaledb"]["host"] = os.getenv("TIMESCALE_DB_HOST", env.get("TIMESCALE_DB_HOST", cfg["timescaledb"]["host"]))
        cfg["timescaledb"]["port"] = int(os.getenv("TIMESCALE_DB_PORT", env.get("TIMESCALE_DB_PORT", cfg["timescaledb"]["port"])))
        cfg["timescaledb"]["dbname"] = os.getenv("TIMESCALE_DB_NAME", env.get("TIMESCALE_DB_NAME", cfg["timescaledb"]["dbname"]))
        cfg["timescaledb"]["user"] = os.getenv("TIMESCALE_DB_USER", env.get("TIMESCALE_DB_USER", cfg["timescaledb"]["user"]))
        cfg["timescaledb"]["password"] = os.getenv("TIMESCALE_DB_PASSWORD", env.get("TIMESCALE_DB_PASSWORD", cfg["timescaledb"]["password"]))
    if "redis" in cfg:
        cfg["redis"]["host"] = os.getenv("REDIS_HOST", env.get("REDIS_HOST", cfg["redis"]["host"]))
        cfg["redis"]["port"] = int(os.getenv("REDIS_PORT", env.get("REDIS_PORT", cfg["redis"]["port"])))
        cfg["redis"]["password"] = os.getenv("REDIS_PASSWORD", env.get("REDIS_PASSWORD", cfg["redis"]["password"]))
    cfg["active_profile"] = profile
    trade_pairs = env.get("TRADE_PAIRS", "")
    if trade_pairs:
        wanted = {p.strip().upper() for p in trade_pairs.split(",")}
        configured = {p["name"].split("/")[0] for p in cfg["pairs"]}
        for pair in cfg["pairs"]:
            base = pair["name"].split("/")[0]
            pair["enabled"] = base in wanted
        for ticker in wanted:
            if ticker not in configured:
                cfg["pairs"].append({
                    "name": f"{ticker}/USDT",
                    "enabled": True,
                    "grid": {
                        "width_percent": cfg["grid"]["default_width_percent"],
                        "count": cfg["grid"]["default_count"],
                        "equity_percent_per_level": cfg["grid"]["default_equity_percent_per_level"]
                    }
                })
    config_cache = cfg
    return cfg


def get_db():
    global db_conn
    if db_conn and db_conn.closed == 0:
        return db_conn
    try:
        c = config_cache.get("timescaledb", {})
        db_conn = psycopg2.connect(
            host=c.get("host", "localhost"),
            port=c.get("port", 5432),
            dbname=c.get("dbname", "vortex_trades"),
            user=c.get("user", "vortex"),
            password=c.get("password", ""),
        )
        db_conn.autocommit = True
    except Exception:
        return None
    return db_conn


async def get_redis():
    global redis_conn
    if redis_conn:
        try:
            await redis_conn.ping()
            return redis_conn
        except Exception:
            pass
    try:
        c = config_cache.get("redis", {})
        pw = c.get("password", "")
        if pw:
            redis_conn = await aioredis.from_url(f"redis://:{pw}@{c['host']}:{c['port']}", db=c.get("db", 0), decode_responses=True)
        else:
            redis_conn = await aioredis.from_url(f"redis://{c['host']}:{c['port']}", db=c.get("db", 0), decode_responses=True)
        await redis_conn.ping()
    except Exception:
        return None
    return redis_conn


# ---- REST endpoints ----

@app.get("/api/config")
async def api_config():
    return load_config()


@app.get("/api/status")
async def api_status():
    cfg = load_config()
    pairs = [p["name"] for p in cfg.get("pairs", []) if p.get("enabled", True)]
    db_ok = get_db() is not None
    r = await get_redis()
    slots = {}
    if r:
        try:
            raw = await r.get("vortex:allocator")
            if raw:
                slots = json.loads(raw)
        except Exception:
            pass
    return {
        "online": db_ok,
        "profile": cfg.get("active_profile", "standard"),
        "pairs": pairs,
        "grid_type": cfg.get("grid", {}).get("type", "geometric"),
        "grid_width": cfg.get("grid", {}).get("default_width_percent", 1.5),
        "grid_count": cfg.get("grid", {}).get("default_count", 20),
        "entry_timeframe": cfg.get("strategy", {}).get("entry", {}).get("timeframe", "15m"),
        **slots,
    }


@app.get("/api/balances")
async def api_balances():
    return {"error": "Connect dashboard while bot is running to see live balances"}


@app.get("/api/pnl")
async def api_pnl():
    db = get_db()
    if not db:
        return {"error": "TimescaleDB not available"}
    try:
        with db.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
            total = float(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl IS NOT NULL")
            count = cur.fetchone()[0]
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl > 0")
            wins = float(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl > 0")
            win_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl < 0")
            loss_count = cur.fetchone()[0]
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp >= CURRENT_DATE")
            daily = float(cur.fetchone()[0])
        return {"total": total, "trades": count, "wins": win_count, "losses": loss_count, "win_pnl": wins, "daily": daily}
    except Exception as e:
        return {"error": str(e)}


_cpu_prev = {}

@app.get("/api/system")
async def api_system():
    r = await get_redis()
    enabled = True
    if r:
        v = await r.get("vortex:feature:system_monitor")
        if v == b"0":
            enabled = False
    if not enabled:
        return {"enabled": False}
    try:
        cpu = 0.0
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("cpu "):
                    parts = line.split()
                    idle = int(parts[4])
                    total = sum(int(p) for p in parts[1:])
                    prev = _cpu_prev.get("total")
                    if prev:
                        delta_total = total - _cpu_prev["total"]
                        delta_idle = idle - _cpu_prev["idle"]
                        cpu = round((1 - delta_idle / delta_total) * 100, 1) if delta_total else 0
                    _cpu_prev["total"] = total
                    _cpu_prev["idle"] = idle
                    break
        mem_total = mem_avail = 0
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    mem_total = int(line.split()[1]) * 1024
                elif line.startswith("MemAvailable:"):
                    mem_avail = int(line.split()[1]) * 1024
        mem_used = mem_total - mem_avail
        mem_pct = round(mem_used / mem_total * 100, 1) if mem_total else 0
        s = os.statvfs("/")
        disk_total = s.f_frsize * s.f_blocks
        disk_free = s.f_frsize * s.f_bfree
        disk_used = disk_total - disk_free
        disk_pct = round(disk_used / disk_total * 100, 1) if disk_total else 0
        cpu_temp = None
        try:
            with open("/sys/class/thermal/thermal_zone0/temp") as f:
                cpu_temp = round(int(f.read().strip()) / 1000, 1)
        except Exception:
            pass
        cpu_freq = None
        try:
            with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq") as f:
                cpu_freq = round(int(f.read().strip()) / 1000, 1)
        except Exception:
            pass
        uptime = 0
        try:
            with open("/proc/uptime") as f:
                uptime = float(f.read().split()[0])
        except Exception:
            pass
        load_1 = load_5 = load_15 = 0
        try:
            with open("/proc/loadavg") as f:
                parts = f.read().split()
                load_1 = float(parts[0])
                load_5 = float(parts[1])
                load_15 = float(parts[2])
        except Exception:
            pass
        return {
            "enabled": True,
            "cpu": cpu,
            "cpu_temp": cpu_temp,
            "cpu_freq": cpu_freq,
            "mem": {"total": mem_total, "used": mem_used, "percent": mem_pct},
            "disk": {"total": disk_total, "used": disk_used, "percent": disk_pct},
            "uptime": uptime,
            "load": [load_1, load_5, load_15],
            "cores": os.cpu_count() or 0,
        }
    except Exception as e:
        return {"enabled": True, "error": str(e)}


# ---- WebSocket ----

class WSManager:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, data: dict):
        payload = json.dumps(data)
        for conn in self.connections[:]:
            try:
                await conn.send_text(payload)
            except Exception:
                self.disconnect(conn)

ws_manager = WSManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            if data.get("action") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        ws_manager.disconnect(ws)


async def ticker_poller():
    while True:
        r = await get_redis()
        if r:
            try:
                cfg = load_config()
                pairs = [p["name"] for p in cfg.get("pairs", []) if p.get("enabled", True)]
                for symbol in pairs:
                    key = f"vortex:ticker:{symbol.replace('/', '_')}"
                    data = await r.get(key)
                    if data:
                        await ws_manager.broadcast({"type": "ticker", "symbol": symbol, **json.loads(data)})
            except Exception:
                pass
        await asyncio.sleep(3)


# ── Dashboard Broadcaster (WebSocket push) ───────────────────

class WSManager:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, data: dict):
        payload = json.dumps(data)
        for conn in self.connections[:]:
            try:
                await conn.send_text(payload)
            except Exception:
                self.disconnect(conn)

ws_manager = WSManager()


async def dashboard_broadcaster():
    """Push aggregated dashboard data to WebSocket clients every 2s."""
    await asyncio.sleep(3)  # wait for Redis
    while True:
        try:
            r = await get_redis()
            if r:
                status = {}
                pnl = {}
                for key in ("vortex:balance:current", "vortex:balance:initial",
                            "vortex:balance:usdt_free", "vortex:trading_mode"):
                    val = await r.get(key)
                    if val:
                        try:
                            if key == "vortex:balance:current":
                                pnl["current"] = float(val)
                            elif key == "vortex:balance:initial":
                                pnl["initial"] = float(val)
                        except ValueError:
                            pass
                await ws_manager.broadcast({
                    "type": "dashboard", "pnl": pnl,
                    "ts": datetime.utcnow().isoformat(),
                })
        except Exception:
            pass
        await asyncio.sleep(2)


# ── RSS News ─────────────────────────────────────────────────

RSS_SOURCES = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "decrypt": "https://decrypt.co/feed",
}


async def fetch_rss(source: str, url: str) -> list:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=15) as resp:
                text = await resp.text()
                root = ET.fromstring(text)
                items = []
                for item in root.findall(".//item")[:8]:
                    items.append({
                        "source": source,
                        "title": item.findtext("title", "").strip(),
                        "url": item.findtext("link", "").strip(),
                        "summary": item.findtext("description", "").strip()[:200],
                        "time": item.findtext("pubDate", ""),
                    })
                return items
    except Exception:
        return []


@app.get("/api/news")
async def api_news():
    tasks = [fetch_rss(name, url) for name, url in RSS_SOURCES.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    articles = sorted(
        [a for r in results if isinstance(r, list) for a in r],
        key=lambda x: x.get("time", ""), reverse=True
    )
    return {"articles": articles[:20]}


# ── Startup ──────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    try:
        load_config()
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        asyncio.create_task(ticker_poller())
        asyncio.create_task(dashboard_broadcaster())
        asyncio.create_task(_backtest_scheduler())
        asyncio.create_task(_refresh_cache())
        print("Startup complete")
    except Exception as e:
        print(f"Startup error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
