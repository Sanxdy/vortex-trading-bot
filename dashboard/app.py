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
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp > NOW() - INTERVAL '24 hours'")
            daily = float(cur.fetchone()[0])
        return {"total": total, "trades": count, "wins": win_count, "losses": loss_count, "win_pnl": wins, "daily": daily}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/pnl/by-regime")
async def api_pnl_by_regime():
    db = get_db()
    if not db:
        return {"regimes": {}}
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT t.pair, t.timestamp, t.side, t.realized_pnl
                FROM trades t WHERE t.realized_pnl IS NOT NULL
            """)
            all_trades = cur.fetchall()
            cur.execute("""
                SELECT symbol, timestamp, regime, decision FROM trade_decisions
                WHERE decision IN ('ENTER_GRID', 'ENTER_TREND')
            """)
            all_decisions = cur.fetchall()
        regimes = {}
        for t in all_trades:
            paired = [d for d in all_decisions if d[0] == t[0] and abs((t[1] - d[1]).total_seconds()) < 600]
            regime = paired[0][2] if paired else "unknown"
            regimes.setdefault(regime, {"trades": 0, "pnl": 0.0, "wins": 0, "losses": 0})
            regimes[regime]["trades"] += 1
            pnl = float(t[3]) if t[3] else 0
            regimes[regime]["pnl"] += pnl
            if pnl > 0: regimes[regime]["wins"] += 1
            elif pnl < 0: regimes[regime]["losses"] += 1
        for v in regimes.values():
            v["pnl"] = round(v["pnl"], 2)
        return {"regimes": regimes}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/pnl/summary")
async def api_pnl_summary():
    db = get_db()
    r = await get_redis()
    result = {"realized_pnl": 0, "realized_pnl_24h": 0, "portfolio_change": 0, "portfolio_change_pct": 0, "trades": 0, "wins": 0, "losses": 0, "total_fees": 0}
    if db:
        try:
            with db.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(realized_pnl), 0), COUNT(*) FROM trades WHERE realized_pnl IS NOT NULL")
                total_pnl, total_count = cur.fetchone()
                cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl > 0")
                win_pnl = float(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl > 0")
                win_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl < 0")
                loss_count = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp > NOW() - INTERVAL '24 hours'")
                daily = float(cur.fetchone()[0])
                cur.execute("SELECT COALESCE(SUM(fee_cost), 0) FROM trades")
                total_fees = float(cur.fetchone()[0])
                result["realized_pnl"] = round(float(total_pnl), 2) if total_pnl else 0
                result["realized_pnl_24h"] = round(daily, 2)
                result["trades"] = total_count or 0
                result["wins"] = win_count or 0
                result["losses"] = loss_count or 0
                result["total_fees"] = round(total_fees, 2)
        except Exception:
            pass
    if r:
        try:
            initial = await r.get("vortex:balance:initial")
            current = await r.get("vortex:balance:current")
            if initial and current:
                iv = float(initial)
                cv = float(current)
                result["portfolio_change"] = round(cv - iv, 2)
                result["portfolio_change_pct"] = round((cv - iv) / iv * 100, 2) if iv > 0 else 0
        except Exception:
            pass
    return result


@app.get("/api/trades")
async def api_trades(limit: int = 20, offset: int = 0):
    db = get_db()
    if not db:
        return {"error": "TimescaleDB not available"}
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT timestamp, pair, side, price, quantity, realized_pnl
                FROM trades WHERE realized_pnl IS NOT NULL
                ORDER BY timestamp DESC LIMIT %s OFFSET %s
            """, (limit, offset))
            rows = cur.fetchall()
        return [{"ts": r[0].isoformat(), "pair": r[1], "side": r[2], "price": float(r[3]), "qty": float(r[4]), "pnl": float(r[5]) if r[5] is not None else None} for r in rows]
    except Exception as e:
        return {"error": str(e)}


TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w"]

@app.get("/api/history")
async def api_history(symbol: str = "SOL/USDT", timeframe: str = "", limit: int = 200):
    tf = timeframe if timeframe in TIMEFRAMES else config_cache.get("strategy", {}).get("entry", {}).get("timeframe", "15m")
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
            if state.get("trend_active"):
                orders.append({"symbol": symbol, "side": "entry", "price": state["trend_entry"], "amount": state.get("trend_size", 0), "tag": "TREND"})
                orders.append({"symbol": symbol, "side": "stop", "price": state["trend_stop"], "amount": 0, "tag": "TREND"})
                ticker_key = f"vortex:ticker:{symbol.replace('/', '_')}"
                raw = await r.get(ticker_key)
                if raw:
                    t = json.loads(raw)
                    cp = float(t.get("last", 0))
                    ep = float(state.get("trend_entry", 0))
                    sz = float(state.get("trend_size", 0))
                    if ep and sz:
                        orders.append({"symbol": symbol, "side": "pnl", "price": cp, "amount": round((cp - ep) * sz, 2), "tag": "TREND"})
        dyn = {symbol: state.get("dynamic_levels", 0) for symbol, state in data.items()}
        return {"orders": orders, "dynamic": dyn}
    except Exception as e:
        return {"orders": [], "error": str(e)}


@app.get("/api/decisions")
async def api_decisions(limit: int = 30, offset: int = 0):
    db = get_db()
    if not db:
        return {"decisions": []}
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT timestamp, symbol, decision, reason, regime, adx, atr, rsi, price, balance_usdt
                FROM trade_decisions ORDER BY timestamp DESC LIMIT %s OFFSET %s
            """, (limit, offset))
            rows = cur.fetchall()
        return {"decisions": [{
            "ts": r[0].isoformat(), "symbol": r[1], "decision": r[2], "reason": r[3],
            "regime": r[4], "adx": float(r[5]) if r[5] else 0, "atr": float(r[6]) if r[6] else 0,
            "rsi": float(r[7]) if r[7] else 0, "price": float(r[8]) if r[8] else 0,
            "balance": float(r[9]) if r[9] else 0,
        } for r in rows]}
    except Exception as e:
        return {"decisions": [], "error": str(e)}


@app.get("/api/pending-history")
async def api_pending_history(limit: int = 10, offset: int = 0):
    db = get_db()
    if not db:
        return {"entries": []}
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT timestamp, symbol, decision, reason, price
                FROM trade_decisions
                WHERE decision LIKE 'PENDING_%%'
                ORDER BY timestamp DESC LIMIT %s OFFSET %s
            """, (limit, offset))
            rows = cur.fetchall()
        return {"entries": [{
            "ts": r[0].isoformat(), "symbol": r[1], "decision": r[2],
            "reason": r[3], "price": float(r[4]) if r[4] else 0,
        } for r in rows]}
    except Exception as e:
        return {"entries": [], "error": str(e)}


@app.get("/api/kill")
async def api_kill():
    r = await get_redis()
    if not r:
        return {"error": "Redis not available — use Telegram /kill instead"}
    try:
        await r.setex("vortex:kill:signal", 60, "1")
        return {"message": "Kill signal sent to bot. Orders will be cancelled and positions sold."}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/revert")
async def api_revert():
    """Toggle panic_revert_to_safe_mode in config.yaml."""
    config_path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    try:
        content = config_path.read_text()
        if "panic_revert_to_safe_mode: true" in content:
            content = content.replace("panic_revert_to_safe_mode: true", "panic_revert_to_safe_mode: false")
            msg = "Countertrend re-enabled"
        else:
            content = content.replace("panic_revert_to_safe_mode: false", "panic_revert_to_safe_mode: true")
            msg = "Panic revert activated"
        config_path.write_text(content)
        return {"message": msg}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/revert/status")
async def api_revert_status():
    """Check if panic_revert_to_safe_mode is active."""
    config_path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    try:
        content = config_path.read_text()
        reverted = "panic_revert_to_safe_mode: true" in content
        return {"reverted": reverted}
    except Exception as e:
        return {"reverted": False, "error": str(e)}


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


@app.get("/api/notification")
async def api_notification():
    r = await get_redis()
    if not r:
        return {"message": None}
    try:
        msg = await r.get("vortex:notification")
        return {"message": msg}
    except Exception:
        return {"message": None}


@app.get("/api/activity")
async def api_activity(limit: int = 50):
    r = await get_redis()
    if not r:
        return {"entries": []}
    try:
        raw = await r.lrange("vortex:activity", 0, limit - 1)
        entries = [json.loads(e) for e in raw]
        return {"entries": entries}
    except Exception:
        return {"entries": []}


@app.get("/api/performance")
async def api_performance():
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        initial = await r.get("vortex:balance:initial")
        current = await r.get("vortex:balance:current")
        holdings_raw = await r.get("vortex:balance:holdings")
        usdt_free = await r.get("vortex:balance:usdt_free")
        usdt_used = await r.get("vortex:balance:usdt_used")
        start_time = await r.get("vortex:balance:initial_time")
        if not initial or not current:
            return {"error": "No data yet"}
        initial_val = float(initial)
        current_val = float(current)
        diff = current_val - initial_val
        pct = (diff / initial_val * 100) if initial_val > 0 else 0
        holdings = json.loads(holdings_raw) if holdings_raw else []
        coin_value = sum(h.get("value", 0) for h in holdings)
        usdt_free_val = float(usdt_free) if usdt_free else 0
        usdt_used_val = float(usdt_used) if usdt_used else 0
        history = []
        db = get_db()
        if db:
            try:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT EXTRACT(EPOCH FROM timestamp)::bigint * 1000, usdt_balance
                        FROM balance_snapshots ORDER BY timestamp
                    """)
                    for row in cur.fetchall():
                        history.append({"t": row[0], "v": float(row[1])})
            except Exception:
                pass
        return {
            "initial": initial_val, "current": current_val,
            "diff": round(diff, 2), "pct": round(pct, 2),
            "start_time": start_time or "",
            "history": history,
            "breakdown": {
                "usdt_free": usdt_free_val,
                "usdt_in_orders": usdt_used_val,
                "coin_value": round(coin_value, 2),
                "holdings": holdings,
            }
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
    from fastapi.responses import Response
    content = Path(__file__).parent.joinpath("static", "index.html").read_bytes()
    return Response(content=content, media_type="text/html",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
