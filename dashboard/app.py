import asyncio
import aiohttp
import json
import os
import secrets
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
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Vortex Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _get_session_role(request: Request) -> str:
    token = request.cookies.get("vortex_token")
    if not token:
        return "none"
    r = await get_redis()
    if not r:
        return "none"
    try:
        val = await r.get(f"vortex:dash_session:{token}")
        if val:
            return val
    except Exception:
        pass
    return "none"


async def _require_admin(request: Request):
    role = await _get_session_role(request)
    if role != "admin":
        return JSONResponse({"error": "Admin only"}, status_code=403)
    return None


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


# ── Auth ────────────────────────────────────────────────────

@app.get("/api/auth/status")
async def auth_status(request: Request):
    role = await _get_session_role(request)
    return {"role": role}


@app.post("/api/auth/login")
async def auth_login(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid request"}, status_code=400)
    if body.get("role") == "guest":
        token = secrets.token_hex(32)
        r = await get_redis()
        if r:
            await r.setex(f"vortex:dash_session:{token}", 86400, "guest")
        resp = JSONResponse({"role": "guest"})
        resp.set_cookie("vortex_token", token, max_age=86400, httponly=True, samesite="lax")
        return resp
    user = os.getenv("DASHBOARD_USER", "")
    pwd = os.getenv("DASHBOARD_PASS", "")
    if user and pwd and body.get("username") == user and body.get("password") == pwd:
        token = secrets.token_hex(32)
        r = await get_redis()
        if r:
            await r.setex(f"vortex:dash_session:{token}", 86400, "admin")
        resp = JSONResponse({"role": "admin"})
        resp.set_cookie("vortex_token", token, max_age=86400, httponly=True, samesite="lax")
        return resp
    return JSONResponse({"error": "Invalid credentials"}, status_code=401)


@app.get("/api/auth/logout")
async def auth_logout(request: Request):
    token = request.cookies.get("vortex_token")
    if token:
        r = await get_redis()
        if r:
            try:
                await r.delete(f"vortex:dash_session:{token}")
            except Exception:
                pass
    resp = JSONResponse({"role": "none"})
    resp.delete_cookie("vortex_token")
    return resp
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


@app.get("/api/fear-greed")
async def api_fear_greed():
    r = await get_redis()
    if not r:
        return {"value": None, "error": "Redis not available"}
    try:
        raw = await r.get("vortex:fear_greed")
        if raw:
            return json.loads(raw)
        return {"value": None}
    except Exception as e:
        return {"error": str(e)}


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
                SELECT symbol, timestamp, regime, decision, trend_uptrend FROM trade_decisions
                WHERE decision IN ('ENTER_TREND_PLACED')
            """)
            all_decisions = cur.fetchall()
        regimes = {}
        for t in all_trades:
            paired = [d for d in all_decisions if d[0] == t[0] and 0 <= (t[1] - d[1]).total_seconds() < 86400]
            if not paired:
                continue
            dr = paired[0]
            base_regime = dr[2] if dr[2] else "unknown"
            trend_up = dr[4]  # trend_uptrend
            if base_regime == "trending" and trend_up is True:
                regime = "bullish"
            elif base_regime == "trending" and trend_up is False:
                regime = "bearish"
            elif base_regime == "trending":
                regime = "trending"
            else:
                regime = base_regime
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
                cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp >= CURRENT_DATE")
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
    result["server_time"] = datetime.now(timezone.utc).isoformat()
    return result


@app.get("/api/trades")
async def api_trades(limit: int = 20, offset: int = 0, hours: int = 0):
    db = get_db()
    if not db:
        return {"error": "TimescaleDB not available"}
    try:
        with db.cursor() as cur:
            where = "WHERE realized_pnl IS NOT NULL"
            if hours > 0:
                where += f" AND timestamp > NOW() - INTERVAL '{hours} hours'"
            cur.execute(f"""
                SELECT timestamp, pair, side, price, quantity, realized_pnl, fee_cost
                FROM trades {where}
                ORDER BY timestamp DESC LIMIT %s OFFSET %s
            """, (limit, offset))
            rows = cur.fetchall()
        return [{"ts": r[0].isoformat(), "pair": r[1], "side": r[2], "price": float(r[3]), "qty": float(r[4]), "pnl": float(r[5]) if r[5] is not None else None, "fee": float(r[6]) if r[6] else 0} for r in rows]
    except Exception as e:
        return {"error": str(e)}


TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w"]

_exchange_cache = {}

@app.get("/api/history")
async def api_history(symbol: str = "SOL/USDT", timeframe: str = "", limit: int = 200):
    tf = timeframe if timeframe in TIMEFRAMES else config_cache.get("strategy", {}).get("entry", {}).get("timeframe", "15m")
    try:
        key = "binance_spot"
        if key not in _exchange_cache:
            ex = ccxt.binance()
            ex.load_markets()
            _exchange_cache[key] = ex
        else:
            ex = _exchange_cache[key]
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
            if state.get("trend_entry_pending"):
                orders.append({"symbol": symbol, "side": "entry_pending", "price": state.get("trend_entry", 0), "amount": state.get("trend_size", 0), "tag": "TREND"})
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
            cur.execute("SELECT COUNT(*) FROM trade_decisions")
            total = cur.fetchone()[0]
            cur.execute("""
                SELECT timestamp, symbol, decision, reason, regime, adx, atr, rsi, price, balance_usdt, trend_uptrend
                FROM trade_decisions ORDER BY timestamp DESC LIMIT %s OFFSET %s
            """, (limit, offset))
            rows = cur.fetchall()
        return {"total": total, "decisions": [{
            "ts": r[0].isoformat(), "symbol": r[1], "decision": r[2], "reason": r[3],
            "regime": r[4], "adx": float(r[5]) if r[5] else 0, "atr": float(r[6]) if r[6] else 0,
            "rsi": float(r[7]) if r[7] else 0, "price": float(r[8]) if r[8] else 0,
            "balance": float(r[9]) if r[9] else 0, "trend_uptrend": r[10],
        } for r in rows]}
    except Exception as e:
        return {"decisions": [], "error": str(e)}


@app.get("/api/plan/status")
@app.get("/api/strategies/summary")
async def api_strategies_summary():
    """Active strategies with config + live trade counts per strategy."""
    db = get_db()
    ep = config_cache.get("entry_paths", {})
    strategies = {}
    for pair, paths in ep.items():
        for strat, enabled in paths.items():
            if not enabled:
                continue
            if strat not in strategies:
                strategies[strat] = {"pairs": [], "entries": 0, "fills": 0, "pnl": 0.0, "target": 30}
            strategies[strat]["pairs"].append(pair)
    if db:
        try:
            with db.cursor() as cur:
                cur.execute("""
                    SELECT d.reason, COUNT(DISTINCT d.id) as entries,
                           COUNT(t.id) as fills,
                           COALESCE(SUM(t.realized_pnl), 0) as total_pnl
                    FROM trade_decisions d
                    LEFT JOIN trades t ON t.pair = d.symbol AND t.side = 'sell'
                      AND t.timestamp > d.timestamp
                      AND t.timestamp < d.timestamp + INTERVAL '4 hours'
                    WHERE d.decision = 'ENTER_TREND_PLACED'
                      AND d.reason LIKE '%_placed'
                      AND d.timestamp > NOW() - INTERVAL '14 days'
                    GROUP BY d.reason
                """)
                for reason, entries, fills, pnl in cur.fetchall():
                    key = reason.replace("_placed", "")
                    if key in strategies:
                        strategies[key]["entries"] = entries
                        strategies[key]["fills"] = fills
                        strategies[key]["pnl"] = round(float(pnl), 2)
        except Exception:
            pass
    return {"strategies": strategies}

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
async def api_kill(request: Request):
    admin = await _require_admin(request)
    if admin: return admin
    r = await get_redis()
    if not r:
        return {"error": "Redis not available — use Telegram /kill instead"}
    try:
        await r.setex("vortex:kill:signal", 60, "1")
        return {"message": "Kill signal sent to bot. Orders will be cancelled and positions sold."}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/revert")
async def api_revert(request: Request, mode: str = ""):
    admin = await _require_admin(request)
    if admin: return admin
    """Set regime mode: normal, auto, or countertrend."""
    config_path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    import time
    try:
        content = config_path.read_text()
        if mode == "normal":
            content = content.replace('regime_mode: "auto"', 'regime_mode: "normal"')
            if 'regime_mode: "countertrend"' in content:
                content = content.replace('regime_mode: "countertrend"', 'regime_mode: "normal"')
            content = content.replace("panic_revert_to_safe_mode: false", "panic_revert_to_safe_mode: true")
            msg = "🔵 Normal mode"
        elif mode == "countertrend":
            content = content.replace('regime_mode: "auto"', 'regime_mode: "countertrend"')
            if 'regime_mode: "normal"' in content:
                content = content.replace('regime_mode: "normal"', 'regime_mode: "countertrend"')
            content = content.replace("panic_revert_to_safe_mode: true", "panic_revert_to_safe_mode: false")
            msg = "🟠 Countertrend mode"
        else:
            content = content.replace('regime_mode: "normal"', 'regime_mode: "auto"')
            if 'regime_mode: "countertrend"' in content:
                content = content.replace('regime_mode: "countertrend"', 'regime_mode: "auto"')
            msg = "🟢 Auto mode"
        config_path.write_text(content)
        r = await get_redis()
        if r:
            await r.setex("vortex:kill:signal", 60, "1")
            entry = json.dumps({"t": time.time(), "m": msg, "type": "warn"})
            await r.lpush("vortex:activity", entry)
            await r.ltrim("vortex:activity", 0, 499)
        return {"message": msg}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/pair/exit")
async def api_pair_exit(request: Request):
    admin = await _require_admin(request)
    if admin: return admin
    """Queue a per-pair graceful exit signal for the heartbeat to pick up."""
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        body = await request.json()
        symbol = body.get("symbol")
        reason = body.get("reason", "manual_tp")
        if not symbol:
            return {"error": "symbol required"}
        if reason not in ("manual_tp", "manual_sl"):
            return {"error": "reason must be manual_tp or manual_sl"}
        redis_key = f"vortex:exit:signal:{symbol.replace('/', '_')}"
        await r.setex(redis_key, 120, reason)
        entry = json.dumps({"t": time.time(), "m": f"Manual exit queued: {symbol} ({reason})", "type": "warn"})
        await r.lpush("vortex:activity", entry)
        await r.ltrim("vortex:activity", 0, 499)
        return {"message": f"Exit signal queued for {symbol}. Executes within 30s."}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/risk/limit")
async def api_risk_limit_get():
    r = await get_redis()
    if not r:
        return {"absolute": None, "percent": 30}
    try:
        raw = await r.get("vortex:max_daily_loss")
        return {"absolute": float(raw) if raw else None, "percent": 30}
    except Exception:
        return {"absolute": None, "percent": 30}

@app.post("/api/risk/limit")
async def api_risk_limit_set(request: Request):
    admin = await _require_admin(request)
    if admin: return admin
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        body = await request.json()
        val = body.get("absolute")
        if val is None:
            await r.delete("vortex:max_daily_loss")
            return {"message": "Override cleared, using config percentage"}
        amount = float(val)
        if amount <= 0:
            return {"error": "amount must be positive"}
        await r.set("vortex:max_daily_loss", str(amount))
        entry = json.dumps({"t": time.time(), "m": f"Daily loss limit set to ${amount:.0f} absolute", "type": "warn"})
        await r.lpush("vortex:activity", entry)
        await r.ltrim("vortex:activity", 0, 499)
        return {"message": f"Daily loss limit set to ${amount:.0f}"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/revert/status")
async def api_revert_status():
    """Return current regime mode."""
    config_path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    try:
        content = config_path.read_text()
        if 'regime_mode: "normal"' in content:
            mode = "normal"
        elif 'regime_mode: "countertrend"' in content:
            mode = "countertrend"
        else:
            mode = "auto"
        return {"mode": mode}
    except Exception as e:
        return {"mode": "auto", "error": str(e)}


@app.get("/api/breakout")
async def api_breakout_get(enabled: str = ""):
    """Get/set breakout toggle. With ?enabled=true|false sets, without reads."""
    r = await get_redis()
    if not r:
        return {"enabled": None, "error": "redis unavailable"}
    try:
        if enabled:
            val = "true" if enabled == "true" else "false"
            await r.set("vortex:breakout", val)
            msg = "✅ Breakout entries ON" if enabled == "true" else "❌ Breakout entries OFF"
            entry = json.dumps({"t": time.time(), "m": msg, "type": "info"})
            await r.lpush("vortex:activity", entry)
            await r.ltrim("vortex:activity", 0, 499)
            return {"message": msg, "enabled": enabled == "true"}
        val = await r.get("vortex:breakout")
        if val == "true":
            return {"enabled": True}
        elif val == "false":
            return {"enabled": False}
        return {"enabled": None}
    except Exception as e:
        return {"enabled": None, "error": str(e)}


@app.get("/api/conditions")
async def api_conditions():
    r = await get_redis()
    if not r:
        return {"pairs": {}}
    try:
        raw = await r.get("vortex:conditions")
        if not raw:
            return {"pairs": {}}
        data = json.loads(raw)
        meta = data.pop("_meta", None)
        stats = data.pop("_stats", None)
        return {"pairs": data, "mode": (meta or {}).get("regime_mode", "auto"), "trading_mode": (meta or {}).get("trading_mode", "ai_observe_only"), "stats": stats}
    except Exception as e:
        return {"pairs": {}, "error": str(e)}


@app.api_route("/api/trading_mode", methods=["GET", "POST"])
async def api_trading_mode(request: Request = None):
    r = await get_redis()
    if not r:
        return {"mode": "ai_observe_only"}
    try:
        if request:
            params = dict(request.query_params)
            mode = params.get("mode", "")
            if mode:
                valid = ["technical_only", "ai_observe_only", "technical_plus_ai"]
                if mode not in valid:
                    return {"error": f"Invalid mode. Choose: {', '.join(valid)}"}
                await r.setex("vortex:trading_mode", 86400, mode)
                return {"mode": mode}
        current = await r.get("vortex:trading_mode")
        return {"mode": current or "ai_observe_only"}
    except Exception:
        return {"mode": "ai_observe_only"}


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


@app.get("/api/log")
async def api_log(msg: str = "", type: str = "info"):
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        import time
        entry = json.dumps({"t": time.time(), "m": msg, "type": type})
        await r.lpush("vortex:activity", entry)
        await r.ltrim("vortex:activity", 0, 499)
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


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


@app.get("/api/backtest")
async def api_backtest():
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        cfg = load_config()
        raw = await r.get("vortex:backtest:latest")
        data = json.loads(raw) if raw else {"pairs": [], "summary": {}}
        if "summary" not in data:
            data["summary"] = {}
        if "pairs_with_trades" not in data["summary"]:
            data["summary"]["pairs_with_trades"] = sum(1 for p in data.get("pairs", []) if p.get("trades", 0) > 0)
        running_raw = await r.get("vortex:backtest:running")
        next_raw = await r.get("vortex:backtest:next_run")
        refresh_raw = await r.get("vortex:backtest:last_refresh")
        data["active_profile"] = cfg.get("active_profile", "standard")
        data["running"] = running_raw == "1" if running_raw else False
        data["next_run_ts"] = next_raw if next_raw else None
        data["last_refresh_ts"] = refresh_raw if refresh_raw else None
        return data
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/backtest/run")
async def api_backtest_run(request: Request):
    admin = await _require_admin(request)
    if admin: return admin
    r = await get_redis()
    if not r:
        return {"error": "Redis not available"}
    try:
        running = await r.get("vortex:backtest:running")
        if running == b"1":
            return {"status": "already_running"}
        last_raw = await r.get("vortex:backtest:last_run")
        if last_raw:
            last_ts = float(last_raw)
            if time.time() - last_ts < 300:
                return {"status": "debounced", "next_attempt": int(last_ts + 300)}
        asyncio.create_task(_run_backtest_once(r))
        return {"status": "started"}
    except Exception as e:
        return {"error": str(e)}


# ── Backtest Scheduler ────────────────────────────────────────────

async def _run_backtest_once(redis, days: int = 365):
    try:
        await redis.set("vortex:backtest:running", "1", ex=86400)
        cfg = load_config()
        profile = cfg.get("active_profile", "standard")
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backtest"))
        from backtest.run import run_all, PAIRS
        print(f"[backtest] Starting {profile} {days}d across {len(PAIRS)} pairs...")
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: asyncio.run(run_all(days=days, profile=profile))
        )
        results = result.get("results", [])
        summary = result.get("summary", {})
        pairs_data = []
        for r in results:
            pairs_data.append({
                "pair": r["symbol"].split("/")[0],
                "trades": r["trades"],
                "wins": r["wins"],
                "losses": r["losses"],
                "win_rate": r["win_rate"],
                "pnl": r["total_pnl"],
                "dpd": r.get("dpd", 0),
                "max_drawdown": r["max_drawdown"],
                "profile": r["profile"],
                "by_path": r.get("by_path", {}),
            })
        payload = {
            "pairs": pairs_data,
            "summary": summary,
        }
        await redis.set("vortex:backtest:latest", json.dumps(payload, default=str))
        await redis.set("vortex:backtest:last_run", str(time.time()))
        now = datetime.now(timezone.utc)
        tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        await redis.set("vortex:backtest:next_run", str(tomorrow.timestamp()))
        print(f"[backtest] Complete — {summary.get('trades', 0)} trades, ${summary.get('pnl', 0):.2f}")
    except Exception as e:
        print(f"[backtest] Error: {e}")
        traceback.print_exc()
    finally:
        try:
            await redis.delete("vortex:backtest:running")
        except Exception:
            pass


async def _backtest_scheduler():
    r = await get_redis()
    if not r:
        return
    await asyncio.sleep(30)
    while True:
        try:
            now = datetime.now(timezone.utc)
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            next_midnight = midnight + timedelta(days=1)
            secs_to_midnight = (next_midnight - now).total_seconds()
            running = await r.get("vortex:backtest:running")
            if not running and now.hour == 0 and now.minute < 5:
                await _run_backtest_once(r)
                secs_to_midnight = 86400
            elif not running and not await r.exists("vortex:backtest:latest"):
                await _run_backtest_once(r)
                secs_to_midnight = 86400
        except Exception as e:
            print(f"[backtest] Scheduler error: {e}")
        await asyncio.sleep(min(secs_to_midnight if secs_to_midnight > 60 else 60, 3600))


async def _refresh_cache():
    r = await get_redis()
    if not r:
        return
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now.day == 1 and now.hour == 1 and now.minute < 5:
                running = await r.get("vortex:backtest:running")
                if not running:
                    print("[cache] Monthly cache refresh starting...")
                    from backtest.cache import DataCache, ALL_PAIRS
                    cache = DataCache()
                    for pair in ALL_PAIRS:
                        try:
                            print(f"  Fetching {pair}...")
                            cache.fetch_and_cache(pair, "5m")
                            cache.fetch_and_cache(pair, "15m")
                            cache.fetch_and_cache(pair, "1h")
                        except Exception as e:
                            print(f"  {pair} error: {e}")
                    await r.set("vortex:backtest:last_refresh", str(now.timestamp()))
                    print("[cache] Complete")
                days_to_next = (31 - now.day) if now.day < 28 else 7
        except Exception as e:
            days_to_next = 1
            print(f"[cache] Error: {e}")
        await asyncio.sleep(days_to_next * 86400 if days_to_next >= 1 else 86400)


_cpu_prev = {}
try:
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu "):
                parts = line.split()
                _cpu_prev["total"] = sum(int(p) for p in parts[1:])
                _cpu_prev["idle"] = int(parts[4])
                break
except Exception:
    pass

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


# ── Fear & Greed Fetcher (dashboard-side, independent of bot) ──

async def _fear_greed_fetcher():
    r = await get_redis()
    if not r:
        return
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.alternative.me/fng/?limit=1", timeout=10) as resp:
                    data = await resp.json()
                    if data and "data" in data and len(data["data"]) > 0:
                        await r.setex("vortex:fear_greed", 3600, json.dumps({
                            "value": int(data["data"][0]["value"]),
                            "classification": data["data"][0]["value_classification"],
                        }))
        except Exception as e:
            print(f"fear_greed error: {e}")
        await asyncio.sleep(1800)  # every 30 min


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
        asyncio.create_task(_fear_greed_fetcher())
        print("Startup complete")
    except Exception as e:
        print(f"Startup error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
