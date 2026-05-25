import asyncio
import json
import os
import time
import yaml
import ccxt
from datetime import datetime, timezone
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


@app.get("/api/plan/status")
async def api_plan_status():
    r = await get_redis()
    if not r:
        return {"phase2": {"ready": False}, "new_pair": {"ready": False, "target_pair": None}}
    try:
        dt = await r.get("vortex:plan:deploy_time")
        if not dt:
            return {"phase2": {"ready": False}, "new_pair": {"ready": False, "target_pair": None}, "message": "No deploy time set — restart the bot"}
        raw = await r.get("vortex:plan:status")
        if not raw:
            return {"phase2": {"ready": False}, "new_pair": {"ready": False, "target_pair": None}, "message": "Check not yet run — waiting for daily cycle"}
        result = json.loads(raw)
        result["deploy_time"] = dt
        return result
    except Exception as e:
        return {"phase2": {"ready": False, "error": str(e)}, "new_pair": {"ready": False, "target_pair": None}}


@app.get("/api/collection/status")
async def api_collection_status():
    db = get_db()
    if not db:
        return {"ema50_bounce": {"entries": 0, "fills": 0, "pnl": 0.0, "target": 30},
                "lowvol_scalp": {"entries": 0, "fills": 0, "pnl": 0.0, "target": 30}}
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
                  AND d.reason IN ('ema50_bounce_placed', 'lowvol_scalp_placed')
                  AND d.timestamp > NOW() - INTERVAL '14 days'
                GROUP BY d.reason
            """)
            rows = cur.fetchall()
        paths = {}
        for reason, entries, fills, pnl in rows:
            key = reason.replace("_placed", "")
            paths[key] = {"entries": entries, "fills": fills, "pnl": round(float(pnl), 2), "target": 30}
        for p in ("ema50_bounce", "lowvol_scalp"):
            if p not in paths:
                paths[p] = {"entries": 0, "fills": 0, "pnl": 0.0, "target": 30}
        return paths
    except Exception as e:
        return {"error": str(e)}

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
async def api_revert(mode: str = ""):
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


@app.get("/api/logs/download")
async def api_logs_download(request: Request = None):
    if not LOG_PATH.exists():
        return {"error": "No log file found — bot may not be running"}
    params = dict(request.query_params) if request else {}
    start_date = params.get("start_date", "")
    end_date = params.get("end_date", "")
    level_filter = params.get("level", "").upper()
    search = params.get("search", "").lower()
    sources = [s.strip() for s in params.get("source", "file").lower().split(",") if s.strip()]

    # Quick path: file-only with no filters → raw FileResponse (efficient, backward compatible)
    if sources == ["file"] and not any([start_date, end_date, level_filter, search]):
        return FileResponse(
            path=str(LOG_PATH),
            filename="vortex.log",
            media_type="text/plain",
            headers={
                "Content-Disposition": "attachment; filename=vortex.log",
                "Cache-Control": "no-cache",
            },
        )

    all_lines = []

    # ── Source: file log ──
    if "file" in sources:
        allowed_levels = {l.strip().upper() for l in level_filter.split(",")} if level_filter else set()
        for line in LOG_PATH.read_text().splitlines():
            if len(line) < 11:
                all_lines.append(line)
                continue
            if start_date and line[:10] < start_date:
                continue
            if end_date and line[:10] > end_date:
                continue
            if level_filter:
                bracket = line.find("[")
                end_bracket = line.find("]", bracket)
                if bracket != -1 and end_bracket != -1:
                    lvl = line[bracket + 1:end_bracket].upper()
                    if lvl not in allowed_levels:
                        continue
                else:
                    if "OTHER" not in allowed_levels:
                        continue
            if search and search not in line.lower():
                continue
            all_lines.append(line)

    # ── Source: decisions (TimescaleDB) ──
    if "decisions" in sources:
        db = get_db()
        if db:
            try:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT timestamp, symbol, decision, reason, regime, adx, atr, rsi, price
                        FROM trade_decisions ORDER BY timestamp ASC
                    """)
                    rows = cur.fetchall()
                for r in rows:
                    ts_str = r[0].strftime("%Y-%m-%d %H:%M:%S")
                    line = f"{ts_str} [DECISION] {r[1]} {r[2]} {r[3]} | regime={r[4]} adx={r[5]} rsi={r[7] if r[7] else 0}"
                    if start_date and ts_str[:10] < start_date:
                        continue
                    if end_date and ts_str[:10] > end_date:
                        continue
                    if search and search not in line.lower():
                        continue
                    all_lines.append(line)
            except Exception:
                pass

    # ── Source: activity (Redis) ──
    if "activity" in sources:
        r_conn = await get_redis()
        if r_conn:
            try:
                raw = await r_conn.lrange("vortex:activity", 0, 499)
                for entry in raw:
                    try:
                        d = json.loads(entry)
                        ts_str = datetime.fromtimestamp(d["t"]).strftime("%Y-%m-%d %H:%M:%S")
                        line = f"{ts_str} [ACTIVITY] {d['m']}"
                        if start_date and ts_str[:10] < start_date:
                            continue
                        if end_date and ts_str[:10] > end_date:
                            continue
                        if search and search not in line.lower():
                            continue
                        all_lines.append(line)
                    except Exception:
                        pass
            except Exception:
                pass

    # Sort all lines by timestamp prefix YYYY-MM-DD HH:MM:SS
    all_lines.sort(key=lambda x: x[:19])

    content = "\n".join(all_lines)
    from fastapi.responses import Response
    return Response(
        content=content,
        media_type="text/plain",
        headers={
            "Content-Disposition": "attachment; filename=vortex-log-export.txt",
            "Cache-Control": "no-cache",
        },
    )


@app.get("/api/watchlist")
async def get_watchlist():
    r = await get_redis()
    if r:
        try:
            raw = await r.get("vortex:watchlist:status")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    # Fallback: read watchlist.yaml directly when bot is offline
    try:
        with open(WATCHLIST_PATH) as f:
            wl = yaml.safe_load(f) or {}
        pairs = []
        for sym, cfg in wl.get("pairs", {}).items():
            pairs.append({
                "symbol": sym,
                "status": "watching",
                "color": "red",
                "summary": "bot offline",
                "enabled": False,
            })
        return {"pairs": pairs}
    except Exception:
        return {"pairs": []}


@app.post("/api/watchlist/enable")
async def watchlist_enable(request: Request):
    r = await get_redis()
    if not r:
        return {"error": "redis unavailable"}, 503
    body = await request.json()
    symbol = body.get("symbol", "").upper()
    if not symbol:
        return {"error": "symbol required"}, 400
    await r.rpush("vortex:watchlist:cmd", json.dumps({"cmd": "enable", "symbol": symbol}))
    return {"ok": True, "symbol": symbol}


@app.post("/api/watchlist/remove")
async def watchlist_remove(request: Request):
    r = await get_redis()
    if not r:
        return {"error": "redis unavailable"}, 503
    body = await request.json()
    symbol = body.get("symbol", "").upper()
    if not symbol:
        return {"error": "symbol required"}, 400
    await r.rpush("vortex:watchlist:cmd", json.dumps({"cmd": "remove", "symbol": symbol}))
    return {"ok": True, "symbol": symbol}


@app.post("/api/watchlist/add")
async def watchlist_add(request: Request):
    r = await get_redis()
    if not r:
        return {"error": "redis unavailable"}, 503
    body = await request.json()
    symbol = body.get("symbol", "").upper()
    if not symbol:
        return {"error": "symbol required"}, 400
    await r.rpush("vortex:watchlist:cmd", json.dumps({"cmd": "add", "symbol": symbol}))
    return {"ok": True, "symbol": symbol}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
