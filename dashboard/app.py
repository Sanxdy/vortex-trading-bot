import asyncio
import json
import os
import yaml
import ccxt
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from redis import asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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

BASE = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE / "config" / "config.yaml"
ENV_PATH = BASE / ".env"

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
    return {
        "online": db_ok,
        "profile": cfg.get("active_profile", "standard"),
        "pairs": pairs,
        "grid_type": cfg.get("grid", {}).get("type", "geometric"),
        "grid_width": cfg.get("grid", {}).get("default_width_percent", 1.5),
        "grid_count": cfg.get("grid", {}).get("default_count", 20),
        "entry_timeframe": cfg.get("strategy", {}).get("entry", {}).get("timeframe", "15m"),
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
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp > NOW() - INTERVAL '24 hours'")
            daily = float(cur.fetchone()[0])
        return {"total": total, "trades": count, "wins": win_count, "losses": loss_count, "win_pnl": wins, "daily": daily}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/trades")
async def api_trades(limit: int = 20):
    db = get_db()
    if not db:
        return {"error": "TimescaleDB not available"}
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT timestamp, pair, side, price, quantity, realized_pnl
                FROM trades WHERE realized_pnl IS NOT NULL
                ORDER BY timestamp DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return [{"ts": r[0].isoformat(), "pair": r[1], "side": r[2], "price": float(r[3]), "qty": float(r[4]), "pnl": float(r[5]) if r[5] else None} for r in rows]
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/history")
async def api_history(symbol: str = "SOL/USDT", limit: int = 200):
    tf = config_cache.get("strategy", {}).get("entry", {}).get("timeframe", "15m")
    try:
        ex = ccxt.binance()
        raw = await asyncio.to_thread(ex.fetch_ohlcv, symbol, tf, limit=limit)
        candles = [{"t": c[0], "o": c[1], "h": c[2], "l": c[3], "c": c[4], "v": c[5]} for c in raw]
        return {"timeframe": tf, "candles": candles}
    except Exception as e:
        return {"error": str(e), "candles": []}


@app.get("/api/orders/active")
async def api_orders_active():
    r = await get_redis()
    if not r:
        return {"orders": []}
    try:
        raw = await r.get("vortex:grid_state")
        if not raw:
            return {"orders": []}
        data = json.loads(raw)
        orders = []
        for symbol, state in data.items():
            for o in state.get("orders", []):
                orders.append({
                    "symbol": f"{symbol}",
                    "side": o["side"],
                    "price": o["price"],
                    "amount": 0,
                })
        return {"orders": orders}
    except Exception as e:
        return {"orders": [], "error": str(e)}


@app.get("/api/conditions")
async def api_conditions():
    r = await get_redis()
    if not r:
        return {"pairs": {}}
    try:
        raw = await r.get("vortex:conditions")
        if not raw:
            return {"pairs": {}}
        return {"pairs": json.loads(raw)}
    except Exception as e:
        return {"pairs": {}, "error": str(e)}


@app.get("/api/performance")
async def api_performance():
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        initial = await r.get("vortex:balance:initial")
        current = await r.get("vortex:balance:current")
        assets_raw = await r.get("vortex:balance:assets")
        start_time = await r.get("vortex:balance:initial_time")
        if not initial or not current:
            return {"error": "No data yet"}
        initial_val = float(initial)
        current_val = float(current)
        diff = current_val - initial_val
        pct = (diff / initial_val * 100) if initial_val > 0 else 0
        return {
            "initial": initial_val, "current": current_val,
            "diff": round(diff, 2), "pct": round(pct, 2),
            "start_time": start_time or "",
        }
    except Exception as e:
        return {"error": str(e)}


# ---- WebSocket ----

connected = set()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected.add(ws)
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            if data.get("action") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        connected.discard(ws)


async def broadcast(data: dict):
    dead = set()
    for ws in connected:
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    connected -= dead


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
                        await broadcast({"type": "ticker", "symbol": symbol, **json.loads(data)})
            except Exception:
                pass
        await asyncio.sleep(3)


@app.on_event("startup")
async def startup():
    load_config()
    asyncio.create_task(ticker_poller())


@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
