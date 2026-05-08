import asyncio
import argparse
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import ccxt
import yaml
import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from strategist import Strategist

TOP_COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "DOT", "LINK"]


class MockExchange:
    def __init__(self):
        self.exchange = ccxt.binance()

    async def fetch_ohlcv(self, symbol, timeframe, limit=1000):
        return await asyncio.to_thread(self.exchange.fetch_ohlcv, symbol, timeframe, limit=limit)

    async def close(self):
        pass


def load_config_for_profile(profile: str) -> dict:
    path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)
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
    cfg["active_profile"] = profile
    for pair in cfg["pairs"]:
        pair["enabled"] = True
    return cfg


def simulate_analyst(ec: dict, df_entry: pd.DataFrame) -> tuple:
    """Returns (verdict, safe) — rule-based proxy for DeepSeek."""
    atr = ec.get("atr", 0)
    avg_atr = float(df_entry["atr"].mean()) if "atr" in df_entry.columns else 0
    regime = ec.get("regime", "?")
    last = df_entry.iloc[-1]
    price = last["close"]
    above_ema = False
    if "ema_200" in df_entry.columns:
        above_ema = price > df_entry.iloc[-1]["ema_200"]

    if avg_atr > 0 and atr > avg_atr * 2.0:
        return "HIGH_VOLATILITY", False
    if not above_ema and regime != "trending":
        return "STRONG_DOWNTREND", False
    return "SAFE", True


def simulate_pnl(df: pd.DataFrame, entry_idx: int, entry_price: float, width: float) -> dict:
    """Look ahead up to 12 candles, simulate a grid flip."""
    lookahead = 12
    sell_target = entry_price * (1 + width)
    stop = entry_price * 0.98
    for j in range(1, min(lookahead, len(df) - entry_idx - 1)):
        candle = df.iloc[entry_idx + j]
        high, low = candle["high"], candle["low"]
        if high >= sell_target:
            return {"pnl": round((sell_target - entry_price) / entry_price * 100, 2), "exit_price": sell_target, "bars": j, "result": "win"}
        if low <= stop:
            return {"pnl": round((stop - entry_price) / entry_price * 100, 2), "exit_price": stop, "bars": j, "result": "loss"}
    return {"pnl": 0.0, "exit_price": entry_price, "bars": lookahead, "result": "no_fill"}


async def run_batch(coins: list, days: int, profile: str, write_db: bool):
    config = load_config_for_profile(profile)
    tf = config["strategy"]["entry"]["timeframe"]
    db_conn = None
    if write_db:
        import os
        tc = config.get("timescaledb", {})
        db_host = os.getenv("TIMESCALE_DB_HOST") or tc.get("host", "localhost")
        db_port = int(os.getenv("TIMESCALE_DB_PORT") or tc.get("port", 5432))
        db_name = os.getenv("TIMESCALE_DB_NAME") or tc.get("dbname", "vortex_trades")
        db_user = os.getenv("TIMESCALE_DB_USER") or tc.get("user", "vortex")
        db_pass = os.getenv("TIMESCALE_DB_PASSWORD") or tc.get("password", "")
        try:
            db_conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass)
            db_conn.autocommit = True
            print(f"DB connected at {db_host}:{db_port}")
        except Exception as e:
            print(f"DB connect failed: {e}")
            write_db = False

    total_decisions = 0
    total_written = 0
    ts_counter = 0

    for coin in coins:
        symbol = f"{coin}/USDT"
        print(f"\n--- {symbol} ({days}d) ---")
        config["pairs"] = [p for p in config["pairs"] if p["name"] == symbol]
        if not config["pairs"]:
            config["pairs"].append({"name": symbol, "enabled": True, "grid": {}})

        exchange = MockExchange()
        strat = Strategist(config, exchange)
        await strat.backfill(symbol, tf)
        await strat.backfill(symbol, strat.timeframes["exit_trend"])
        await exchange.close()

        df = strat.data[symbol][tf]
        if df is None or len(df) < 100:
            continue

        width = config["grid"].get("default_width_percent", 1.5) / 100
        coin_decisions = 0
        coin_entries = 0

        for i in range(100, len(df)):
            chunk = df.iloc[:i + 1].copy()
            strat.data[symbol][tf] = chunk
            strat.calculate_indicators(symbol, tf)
            ec = strat.entry_conditions.get(symbol, {})
            regime = ec.get("regime", "?")
            last = chunk.iloc[-1]
            price = float(last["close"])
            adx = ec.get("adx", 0)
            atr_val = ec.get("atr", 0)
            rsi = ec.get("rsi", 0)

            verdict, safe = simulate_analyst(ec, chunk)
            decision = None
            reason = ""

            if regime == "high_vol" or (not safe and verdict == "HIGH_VOLATILITY"):
                decision = "BLOCKED"
                reason = "simulated_high_volatility"
            elif not safe and verdict == "STRONG_DOWNTREND":
                decision = "BLOCKED"
                reason = "simulated_downtrend"
            elif regime == "trending" and strat.should_enter_trend(symbol):
                decision = "ENTER_TREND"
                reason = "simulated_trend_pullback"
            elif regime == "sideways" and strat.should_enter(symbol):
                decision = "ENTER_GRID"
                reason = "simulated_grid_entry"

            if decision:
                coin_decisions += 1
                total_decisions += 1
                if write_db and db_conn:
                    ts_counter += 1
                    ts = datetime(2026, 5, 6, 0, 0, 0, tzinfo=timezone.utc).timestamp() + ts_counter
                    row_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                    try:
                        with db_conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO trade_decisions (timestamp, symbol, decision, reason, regime, adx, atr, rsi, price, balance_usdt)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (row_ts, symbol, decision, reason, regime,
                                  round(adx, 2), round(atr_val, 2), round(rsi, 1),
                                  round(price, 2), 10000.0))
                    except Exception:
                        pass

                if decision.startswith("ENTER"):
                    coin_entries += 1
                    pnl_info = simulate_pnl(df, i, price, width)
                    if write_db and db_conn and pnl_info["result"] != "no_fill":
                        realized = round(pnl_info["pnl"], 2)
                        ts_counter += 1
                        ts = datetime(2026, 5, 6, 0, 0, 0, tzinfo=timezone.utc).timestamp() + ts_counter
                        row_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                        try:
                            with db_conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO trades (timestamp, pair, side, price, quantity, order_id, status, grid_level, realized_pnl)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (row_ts, symbol, "sell", round(pnl_info["exit_price"], 2), 0.001,
                                      f"sim_{coin_decisions}", "closed", None, realized))
                        except Exception:
                            pass
                        total_written += 1

        print(f"  {coin_decisions} decisions, {coin_entries} entries")

    if db_conn:
        db_conn.close()
    print(f"\n{'='*40}")
    print(f"Total: {total_decisions} decisions written, {total_written} simulated PnL trades")
    print(f"Run /report on Telegram to analyze patterns")


class Backtest:
    def __init__(self, symbol: str, days: int = 30, profile: str = "scalper"):
        self.symbol = symbol
        self.days = days
        self.profile = profile
        self.result = {}

    async def run(self):
        config = load_config_for_profile(self.profile)
        config["pairs"] = [p for p in config["pairs"] if p["name"] == self.symbol]
        if not config["pairs"]:
            config["pairs"].append({"name": self.symbol, "enabled": True, "grid": {}})

        exchange = MockExchange()
        strat = Strategist(config, exchange)
        tf = config["strategy"]["entry"]["timeframe"]
        await strat.backfill(self.symbol, tf)
        await strat.backfill(self.symbol, strat.timeframes["exit_trend"])
        await exchange.close()

        df = strat.data[self.symbol][tf]
        if df is None or len(df) < 50:
            self.result = {"error": f"Not enough data"}
            return self.result

        total_candles = len(df)
        grid_signals = trend_signals = 0
        grid_entries = trend_entries = []

        for i in range(50, total_candles):
            chunk = df.iloc[:i + 1].copy()
            strat.data[self.symbol][tf] = chunk
            strat.calculate_indicators(self.symbol, tf)
            ec = strat.entry_conditions.get(self.symbol, {})
            regime = ec.get("regime", "?")
            if strat.should_enter(self.symbol) and regime == "sideways":
                grid_signals += 1
                grid_entries.append({"time": chunk.iloc[-1]["timestamp"], "price": chunk.iloc[-1]["close"]})
            if strat.should_enter_trend(self.symbol) and regime == "trending":
                trend_signals += 1
                trend_entries.append({"time": chunk.iloc[-1]["timestamp"], "price": chunk.iloc[-1]["close"]})

        total = grid_signals + trend_signals
        self.result = {
            "symbol": self.symbol, "profile": self.profile, "timeframe": tf,
            "candles": total_candles, "days": self.days,
            "grid_signals": grid_signals, "trend_signals": trend_signals,
            "total_signals": total,
            "signal_density_pct": round(total / max(total_candles, 1) * 100, 1),
        }
        return self.result


async def compare_profiles(symbol: str, days: int = 14) -> dict:
    standard = await Backtest(symbol, days, "standard").run()
    scalper = await Backtest(symbol, days, "scalper").run()
    return {"standard": standard, "scalper": scalper}


def main():
    parser = argparse.ArgumentParser(description="Vortex Strategy Backtest")
    parser.add_argument("symbol", nargs="?", default="BTC/USDT")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--profile", default="scalper", choices=["standard", "scalper"])
    parser.add_argument("--write-db", action="store_true", help="Write decisions + PnL to TimescaleDB")
    parser.add_argument("--coins", default="", help="Comma-separated coins (default: top 10)")
    args = parser.parse_args()

    if args.write_db:
        coins = [c.strip().upper() for c in args.coins.split(",")] if args.coins else TOP_COINS
        asyncio.run(run_batch(coins, args.days, args.profile, True))
    elif args.profile == "both":
        results = asyncio.run(compare_profiles(args.symbol.upper(), args.days))
        for prof, r in results.items():
            print(f"\n=== {prof.upper()} ===")
            print(f"Grid: {r.get('grid_signals',0)} | Trend: {r.get('trend_signals',0)} | Density: {r.get('signal_density_pct','?')}%")
    else:
        bt = Backtest(args.symbol.upper(), args.days, args.profile)
        r = asyncio.run(bt.run())
        print(f"\n=== {args.profile.upper()} ===")
        print(f"Grid: {r.get('grid_signals',0)} | Trend: {r.get('trend_signals',0)} | Total: {r.get('total_signals',0)}/{r.get('candles',0)} candles ({r.get('signal_density_pct','?')}%)")


if __name__ == "__main__":
    main()
