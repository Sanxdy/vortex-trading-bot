import asyncio
import argparse
import sys
import csv
import time
from pathlib import Path
from datetime import datetime, timezone

import ccxt
import yaml
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from strategist import Strategist
from backtest.cache import DataCache

TOP_COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "DOT", "LINK"]
PROFILES = ["scalper", "standard", "trend_only", "conservative", "sideway"]
PAIRS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "ADA/USDT"]
DEFAULT_DAYS = 60


class MockExchange:
    def __init__(self):
        self.exchange = ccxt.binance()

    async def fetch_ohlcv(self, symbol, timeframe, limit=1000):
        target_candles = {"5m": 6000, "15m": 5000, "1h": 2000}.get(timeframe, 1000)
        all_data = []
        end_time = None
        for _ in range(30):
            params = {}
            if end_time:
                params["endTime"] = end_time
            chunk = await asyncio.to_thread(
                self.exchange.fetch_ohlcv, symbol, timeframe, limit=1000, params=params
            )
            if not chunk or len(chunk) < 2:
                break
            all_data = chunk + all_data
            end_time = all_data[0][0] - 1
            if len(all_data) >= target_candles:
                break
        return all_data

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
    cfg["anti_churn"] = {"continuation": {"max_consecutive_losses": 2, "cooldown_minutes": 45}}
    cfg["execution"]["post_only_trend"] = False
    cfg["backtest"] = {"max_candles": 6000}
    return cfg


def choose_entry_path(ec: dict, strat: Strategist, symbol: str) -> tuple:
    regime = ec.get("regime", "")
    ct_score = strat.evaluate_countertrend_scalp(symbol)
    if regime == "trending":
        if strat.should_enter(symbol):
            bo = ec.get("trend_breakout", False)
            pb = ec.get("trend_pullback", False)
            if bo:
                _rsi_caps = {"BTC/USDT": 65, "ETH/USDT": 0}
                rsi_cap = _rsi_caps.get(symbol, 62)
                rsi = ec.get("rsi", 50)
                adx = ec.get("adx", 0)
                if rsi_cap == 0:
                    return ("skip", "breakout_disabled")
                if rsi > rsi_cap and adx < 40:
                    return ("skip", f"breakout_rsi{rsi:.0f}_cap{rsi_cap}")
                return ("breakout", "trend_breakout")
            if pb:
                return ("pullback", "trend_pullback")
            adx = ec.get("adx", 0)
            rsi = ec.get("rsi", 50)
            price_above_50 = ec.get("price_above_50_ema", False)
            price_above_200 = ec.get("price_above_200_ema", False)
            if adx > 25 and 40 <= rsi <= 55 and price_above_50:
                return ("continuation", "trend_continuation")
            if adx > 35 and 35 <= rsi <= 65 and price_above_200:
                return ("continuation", "trend_continuation")
            if rsi < ec.get("rsi_oversold", 35) and price_above_200:
                return ("continuation", "trend_continuation")
        return ("skip", "no_setup")
    if regime == "sideways" and ct_score >= 65:
        return ("countertrend", f"countertrend_score_{ct_score}")
    return ("skip", f"no_entry_{regime}")

def choose_sideway_path(ec: dict, strat: Strategist, df, i: int) -> tuple:
    """Multi-hypothesis entry for sideways regime."""
    regime = ec.get("regime", "")
    if regime != "sideways":
        return ("skip", "not_sideways")
    rvol = ec.get("rvol", 1.0)
    rsi = ec.get("rsi", 50)
    adx = ec.get("adx", 0)
    atr_pct = ec.get("atr_pct", 0)
    price_above_50 = ec.get("price_above_50_ema", False)
    price_at_lower_bb = ec.get("price_at_lower_bb", False)
    candle_eff = ec.get("candle_eff", 0.5)
    close = float(df.iloc[-1]["close"])
    bb_lower = ec.get("bb_lower", 0)
    bb_upper = float(df.iloc[-1].get("bb_upper", 0)) if "bb_upper" in df.columns else 0
    previous_close = float(df.iloc[-2]["close"]) if len(df) >= 2 else close

    # S1: Low-volatility scalp (atr_pct < 0.3%, above 50 EMA, green candle)
    if atr_pct and atr_pct < 0.3 and price_above_50 and close > previous_close:
        return ("lowvol_scalp", "lowvol_s1")
    # S2: BB overshoot with volume (RSI < 30, price below lower BB, volume spike)
    if rsi < 30 and bb_lower > 0 and close < bb_lower and rvol > 1.5:
        return ("bb_overshoot", "bb_os_s2")
    # S3: Strong momentum candle (efficiency > 0.7, volume > 1.5x, above 50 EMA)
    if candle_eff > 0.7 and rvol > 1.5 and price_above_50:
        return ("momentum_scalp", "momentum_s3")
    # S4: EMA50 support bounce (price at or below EMA50, RSI between 40-60, RVOL > 1.0)
    ema50 = float(df.iloc[-1].get("ema_50", 0)) if "ema_50" in df.columns else 0
    if ema50 > 0 and close >= ema50 * 0.998 and close <= ema50 * 1.005 and 40 <= rsi <= 60 and rvol > 1.0:
        return ("ema50_bounce", "ema50_s4")
    # S5: Oversold bounce (RSI < 35, price > 50 EMA AND above 200 EMA)
    price_above_200 = ec.get("price_above_200_ema", False)
    if rsi < 35 and price_above_50 and price_above_200:
        return ("oversold_bounce", "ob_s5")
    return ("skip", "no_sideway_setup")


def choose_entry_path_clean(ec: dict, strat: Strategist, symbol: str, df=None, i=0) -> tuple:
    """Trend entry + sideway hypotheses. Returns (path, reason) or ('skip', why)."""
    regime = ec.get("regime", "")
    if regime == "trending":
        if strat.should_enter(symbol):
            bo = ec.get("trend_breakout", False)
            pb = ec.get("trend_pullback", False)
            if bo:
                _rsi_caps = {"BTC/USDT": 65, "ETH/USDT": 0}
                rsi_cap = _rsi_caps.get(symbol, 62)
                rsi = ec.get("rsi", 50)
                adx = ec.get("adx", 0)
                if rsi_cap == 0:
                    return ("skip", "breakout_disabled")
                if rsi > rsi_cap and adx < 40:
                    return ("skip", f"breakout_rsi{rsi:.0f}_cap{rsi_cap}")
                return ("breakout", "trend_breakout")
            if pb:
                return ("pullback", "trend_pullback")
            adx = ec.get("adx", 0)
            rsi = ec.get("rsi", 50)
            price_above_50 = ec.get("price_above_50_ema", False)
            price_above_200 = ec.get("price_above_200_ema", False)
            if adx > 25 and 40 <= rsi <= 55 and price_above_50:
                return ("continuation", "trend_continuation")
            if adx > 35 and 35 <= rsi <= 65 and price_above_200:
                return ("continuation", "trend_continuation")
            if rsi < ec.get("rsi_oversold", 35) and price_above_200:
                return ("continuation", "trend_continuation")
        return ("skip", "no_setup")
    if regime == "sideways":
        return choose_sideway_path(ec, strat, df, i)
    return ("skip", f"no_entry_{regime}")


class SimulatedPosition:
    def __init__(self, symbol: str, entry_price: float, entry_type: str, 
                 atr: float, trail_mult: float, entry_time: int,
                 balance: float, fee_rate: float = 0.001):
        self.symbol = symbol
        self.entry_price = entry_price
        self.entry_type = entry_type
        self.atr = atr
        self.trail_mult = trail_mult
        self.entry_time = entry_time
        self.balance = balance
        self.fee_rate = fee_rate
        self.highest_price = entry_price
        self.lowest_price = entry_price
        self.stop_level = entry_price - (atr * trail_mult) if entry_type != "range_short" else entry_price + (atr * trail_mult)
        self.breakeven_activated = False
        self.exit_price = None
        self.exit_reason = None
        self.exit_time = None
        self.closed = False
        self.fixed_tp = None
        self.fixed_sl = None

    def update(self, candle_high: float, candle_low: float, current_time: int):
        if self.closed:
            return
        # Fixed TP/SL for sideway strategies
        if self.fixed_tp is not None:
            if self.fixed_tp >= self.entry_price:  # Long: TP above entry
                if candle_high >= self.fixed_tp:
                    self.exit_price = self.fixed_tp
                    self.exit_reason = "tp"
                    self.exit_time = current_time
                    self.closed = True
                    return
                if self.fixed_sl and candle_low <= self.fixed_sl:
                    self.exit_price = self.fixed_sl
                    self.exit_reason = "sl"
                    self.exit_time = current_time
                    self.closed = True
                    return
            else:  # Short: TP below entry
                if candle_low <= self.fixed_tp:
                    self.exit_price = self.fixed_tp
                    self.exit_reason = "tp"
                    self.exit_time = current_time
                    self.closed = True
                    return
                if self.fixed_sl and candle_high >= self.fixed_sl:
                    self.exit_price = self.fixed_sl
                    self.exit_reason = "sl"
                    self.exit_time = current_time
                    self.closed = True
                    return
            return
        # Standard trailing stop for trend trades
        if candle_high > self.highest_price:
            self.highest_price = candle_high
            new_stop = candle_high - (self.atr * self.trail_mult)
            self.stop_level = max(self.stop_level, new_stop)
        if not self.breakeven_activated and candle_high >= self.entry_price * 1.002:
            if self.entry_type in ("continuation", "breakout"):
                self.breakeven_activated = True
                be_stop = self.entry_price * 1.001
                self.stop_level = max(self.stop_level, be_stop)
        emergency_stop = self.entry_price * 0.97
        if candle_low <= emergency_stop:
            self.exit_price = min(candle_low, self.stop_level)
            self.exit_reason = "emergency"
            self.exit_time = current_time
            self.closed = True
            return
        if candle_low <= self.stop_level:
            self.exit_price = self.stop_level
            self.exit_reason = "trail"
            self.exit_time = current_time
            self.closed = True

    def get_pnl(self) -> float:
        if not self.closed or self.exit_price is None:
            return 0.0
        gross = (self.exit_price - self.entry_price) / self.entry_price
        notional = self.balance * 0.95
        gross_usd = gross * notional
        entry_fee = notional * self.fee_rate
        exit_fee = (notional + gross_usd) * self.fee_rate
        return round(gross_usd - entry_fee - exit_fee, 2)


class SimulatedExecutor:
    def __init__(self, config: dict, strat: Strategist, symbol: str):
        self.config = config
        self.strat = strat
        self.symbol = symbol
        self.tf = config["strategy"]["entry"]["timeframe"]
        self.df = strat.data[symbol][self.tf]
        self.trend_cfg = config.get("strategy", {}).get("trend", {})
        self.fee = config.get("fees", {}).get("taker", 0.001)
        self.trail_mult = self.trend_cfg.get("trail_atr", 1.5)
        self.balance = 150.0
        self.trades = []
        self.open_position = None
        self.continuation_losses = 0
        self.continuation_cooldown_until = 0

    def check_entry(self, i: int):
        if self.open_position is not None:
            return
        candle = self.df.iloc[i]
        ec = self.strat.entry_conditions.get(self.symbol, {})
        regime = ec.get("regime", "")
        if regime not in ("trending", "sideways"):
            return
        path, reason = choose_entry_path_clean(ec, self.strat, self.symbol, df=self.df, i=i)
        if path == "skip":
            return
        if path == "continuation":
            cooldown = self.config.get("anti_churn", {}).get("continuation", {}).get("cooldown_minutes", 45) * 60
            if self.continuation_cooldown_until > i:
                return
        atr = ec.get("atr", 0)
        if atr <= 0:
            return
        entry_price = float(candle["close"])
        # Sideway strategies use fixed TP/SL
        sideway_tp_sl = {
            "lowvol_scalp": (0.004, 0.002),     # 0.4% TP, 0.2% SL
            "bb_overshoot": (0.008, 0.004),      # 0.8% TP, 0.4% SL
            "momentum_scalp": (0.006, 0.003),    # 0.6% TP, 0.3% SL
            "ema50_bounce": (0.005, 0.003),      # 0.5% TP, 0.3% SL
            "oversold_bounce": (0.008, 0.004),   # 0.8% TP, 0.4% SL
        }
        if path in sideway_tp_sl:
            tp_pct, sl_pct = sideway_tp_sl[path]
            sl = entry_price * (1 - sl_pct)
            tp = entry_price * (1 + tp_pct)
            pos = SimulatedPosition(
                symbol=self.symbol, entry_price=entry_price, entry_type=path,
                atr=atr, trail_mult=1.0, entry_time=i, balance=self.balance, fee_rate=self.fee,
            )
            pos.fixed_tp = tp
            pos.fixed_sl = sl
            self.open_position = pos
            return
        position = SimulatedPosition(
            symbol=self.symbol,
            entry_price=entry_price,
            entry_type=path,
            atr=atr,
            trail_mult=self.trail_mult,
            entry_time=i,
            balance=self.balance,
            fee_rate=self.fee,
        )
        self.open_position = position

    def update_position(self, i: int, candle=None):
        if self.open_position is None:
            return
        if candle is None:
            candle = self.df.iloc[i]
        self.open_position.update(
            candle_high=float(candle["high"]),
            candle_low=float(candle["low"]),
            current_time=i,
        )
        if self.open_position.closed:
            pnl = self.open_position.get_pnl()
            if self.open_position.entry_type == "continuation":
                if pnl < 0:
                    self.continuation_losses += 1
                    max_losses = self.config.get("anti_churn", {}).get("continuation", {}).get("max_consecutive_losses", 2)
                    if self.continuation_losses >= max_losses:
                        cooldown = self.config.get("anti_churn", {}).get("continuation", {}).get("cooldown_minutes", 45) * 60
                        self.continuation_cooldown_until = i + cooldown
                else:
                    self.continuation_losses = 0
            self.balance += pnl
            self.trades.append({
                "symbol": self.symbol,
                "entry_time": self.open_position.entry_time,
                "exit_time": i,
                "entry_price": self.open_position.entry_price,
                "exit_price": self.open_position.exit_price,
                "entry_type": self.open_position.entry_type,
                "exit_reason": self.open_position.exit_reason,
                "pnl": pnl,
                "atr": self.open_position.atr,
                "balance_after": round(self.balance, 2),
            })
            self.open_position = None

    def run(self):
        total = len(self.df)
        for i in range(100, total):
            chunk = self.df.iloc[:i + 1].copy()
            self.strat.data[self.symbol][self.tf] = chunk
            self.strat.calculate_indicators(self.symbol, self.tf)
            candle = self.df.iloc[i]
            self.update_position(i, candle)
            self.check_entry(i)
        if self.open_position is not None:
            self.open_position.exit_price = float(self.df.iloc[-1]["close"])
            self.open_position.exit_reason = "end_of_data"
            self.open_position.closed = True
            pnl = self.open_position.get_pnl()
            self.trades.append({
                "symbol": self.symbol,
                "entry_time": self.open_position.entry_time,
                "exit_time": total - 1,
                "entry_price": self.open_position.entry_price,
                "exit_price": self.open_position.exit_price,
                "entry_type": self.open_position.entry_type,
                "exit_reason": "end_of_data",
                "pnl": pnl,
                "atr": self.open_position.atr,
                "balance_after": round(self.balance, 2),
            })
        if self.trades:
            self.balance = self.trades[-1]["balance_after"]
        return self.trades


async def run_single(symbol: str, profile: str, days: int = DEFAULT_DAYS) -> dict:
    config = load_config_for_profile(profile)
    config["pairs"] = [p for p in config["pairs"] if p["name"] == symbol]
    if not config["pairs"]:
        config["pairs"].append({"name": symbol, "enabled": True, "grid": {}})
    cache = DataCache()
    tf = config["strategy"]["entry"]["timeframe"]
    tf_exit = config["strategy"]["exit"]["trend_inversion"]["timeframe"]
    df_entry = cache.load(symbol, tf)
    df_exit = cache.load(symbol, tf_exit)
    if df_entry.empty or len(df_entry) < 200:
        return None
    exchange = MockExchange()
    strat = Strategist(config, exchange)
    strat.data[symbol][tf] = df_entry
    strat.data[symbol][tf_exit] = df_exit
    strat.calculate_indicators(symbol, tf)
    strat.calculate_indicators(symbol, tf_exit)
    df = df_entry
    total_candles = len(df)
    sim = SimulatedExecutor(config, strat, symbol)
    trades = sim.run()
    total_pnl = round(sim.balance - 150.0, 2)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    scratches = [t for t in trades if t["pnl"] == 0]
    by_path = {}
    for t in trades:
        path = t["entry_type"]
        if path not in by_path:
            by_path[path] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        by_path[path]["trades"] += 1
        by_path[path]["pnl"] = round(by_path[path]["pnl"] + t["pnl"], 2)
        if t["pnl"] > 0:
            by_path[path]["wins"] += 1
        elif t["pnl"] < 0:
            by_path[path]["losses"] += 1
    max_drawdown = 0.0
    peak = 150.0
    for t in trades:
        bal = t["balance_after"]
        if bal > peak:
            peak = bal
        dd = round(peak - bal, 2)
        if dd > max_drawdown:
            max_drawdown = dd
    return {
        "profile": profile,
        "timeframe": tf,
        "symbol": symbol,
        "candles": total_candles,
        "approx_days": round(total_candles * 5 / 60 / 24, 1) if "5m" in tf else round(total_candles * 15 / 60 / 24, 1),
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "scratches": len(scratches),
        "win_rate": round(len(wins) / max(len(trades), 1) * 100, 1),
        "total_pnl": total_pnl,
        "avg_pnl": round(total_pnl / max(len(trades), 1), 4),
        "avg_win": round(sum(t["pnl"] for t in wins) / max(len(wins), 1), 4) if wins else 0,
        "avg_loss": round(sum(t["pnl"] for t in losses) / max(len(losses), 1), 4) if losses else 0,
        "max_drawdown": max_drawdown,
        "by_path": by_path,
        "exit_reasons": {},
    }


def print_summary(results: list):
    print(f"\n{'='*110}")
    print(f"{'Profile':<14} {'TF':<6} {'Pair':<10} {'Trades':<8} {'Win%':<7} {'Avg$':<8} {'Total$':<8} {'MaxDD$':<8} {'AvgWin$':<9} {'AvgLoss$':<9}")
    print(f"{'-'*14} {'-'*6} {'-'*10} {'-'*8} {'-'*7} {'-'*8} {'-'*8} {'-'*8} {'-'*9} {'-'*9}")
    best = max(results, key=lambda r: r["total_pnl"]) if results else None
    worst = min(results, key=lambda r: r["total_pnl"]) if results else None
    for r in sorted(results, key=lambda x: x["total_pnl"], reverse=True):
        marker = ""
        if r is best:
            marker = " 🏆"
        elif r is worst:
            marker = " 💀"
        print(f"{r['profile']:<14} {r['timeframe']:<6} {r['symbol']:<10} {r['trades']:<8} {r['win_rate']:<7} ${r['avg_pnl']:<+7.4f} ${r['total_pnl']:<+7.2f} ${r['max_drawdown']:<7.2f} ${r['avg_win']:<+8.4f} ${r['avg_loss']:<+8.4f}{marker}")
    print(f"{'='*110}")

    r_best = results[0] if results else None
    for profile in PROFILES:
        pr = [r for r in results if r["profile"] == profile]
        if not pr:
            continue
        total_pnl = sum(r["total_pnl"] for r in pr)
        total_trades = sum(r["trades"] for r in pr)
        wins = sum(r["wins"] for r in pr)
        print(f"\n--- {profile.upper()} (${total_pnl:+.2f} across {total_trades} trades, {wins} wins) ---")
        for r in sorted(pr, key=lambda x: x["total_pnl"], reverse=True):
            paths = []
            for p, v in r.get("by_path", {}).items():
                paths.append(f"{p}: {v['trades']}t ${v['pnl']:+.2f}")
            print(f"  {r['symbol']:<10} {r['trades']:>3} trades  {r['win_rate']:>5.1f}%  ${r['total_pnl']:>+7.2f}  {' | '.join(paths)}")

    print(f"\n{'='*110}")
    for profile in PROFILES:
        pr = [r for r in results if r["profile"] == profile]
        if not pr:
            continue
        total_pnl = sum(r["total_pnl"] for r in pr)
        print(f"  {profile:<14} ${total_pnl:>+7.2f}")
    print(f"{'='*110}")


async def run_all():
    results = []
    all_trades = []
    for profile in PROFILES:
        for symbol in PAIRS:
            print(f"  Running {profile} / {symbol} ...", end=" ")
            sys.stdout.flush()
            try:
                r = await run_single(symbol, profile, DEFAULT_DAYS)
                if r is None:
                    print("no data")
                    continue
                results.append(r)
                print(f"{r['trades']} trades, ${r['total_pnl']:+.2f}")
            except Exception as e:
                print(f"error: {e}")
    print_summary(results)


def main():
    parser = argparse.ArgumentParser(description="Vortex Strategy Backtest v2")
    parser.add_argument("--symbol", default="", help="Single pair (e.g. BTC/USDT)")
    parser.add_argument("--profile", default="", help="Single profile (e.g. scalper)")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    args = parser.parse_args()
    if args.symbol and args.profile:
        r = asyncio.run(run_single(args.symbol.upper(), args.profile, args.days))
        if r:
            print_summary([r])
        else:
            print(f"No data for {args.symbol} / {args.profile}")
    else:
        asyncio.run(run_all())


class Backtest:
    """Legacy stub — kept for notifier import compatibility."""
    def __init__(self, symbol: str = "", days: int = 30, profile: str = "scalper"):
        self.symbol = symbol
        self.days = days
        self.profile = profile

    async def run(self):
        r = await run_single(self.symbol, self.profile, self.days)
        if r:
            return {k: v for k, v in r.items() if k != "by_path"}
        return {"error": "no data"}

if __name__ == "__main__":
    main()
