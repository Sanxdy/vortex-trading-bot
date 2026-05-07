import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime

import ccxt
import yaml
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from strategist import Strategist


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


def load_base_config() -> dict:
    return load_config_for_profile("scalper")


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
            self.result = {"error": f"Not enough data ({len(df) if df is not None else 0} candles)"}
            return self.result

        total_candles = len(df)
        grid_signals = 0
        trend_signals = 0
        grid_entries = []
        trend_entries = []

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
            "symbol": self.symbol,
            "profile": self.profile,
            "timeframe": tf,
            "candles": total_candles,
            "days": self.days,
            "grid_signals": grid_signals,
            "trend_signals": trend_signals,
            "total_signals": total,
            "signal_density_pct": round(total / max(total_candles, 1) * 100, 1),
            "grid_pct": round(grid_signals / max(total, 1) * 100, 0),
            "trend_pct": round(trend_signals / max(total, 1) * 100, 0),
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
    args = parser.parse_args()

    if args.profile == "both":
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
