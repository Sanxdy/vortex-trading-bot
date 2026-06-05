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
try:
    import pandas_ta as pd_ta
except ImportError:
    import sys
    # Install shim into sys.modules so strategist can also find it
    from backtest.pandas_ta_shim import bbands, ema, atr, rsi, adx, supertrend
    pd_ta = type(sys)("pandas_ta")
    pd_ta.bbands = staticmethod(bbands)
    pd_ta.ema = staticmethod(ema)
    pd_ta.atr = staticmethod(atr)
    pd_ta.rsi = staticmethod(rsi)
    pd_ta.adx = staticmethod(adx)
    pd_ta.supertrend = staticmethod(supertrend)
    sys.modules["pandas_ta"] = pd_ta
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from strategist import Strategist
from backtest.cache import DataCache, ALL_PAIRS as CACHE_PAIRS

TOP_COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "DOT", "LINK"]
PROFILES = ["scalper", "standard", "trend_only", "conservative", "sideway"]
PAIRS = CACHE_PAIRS
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
    if "backtest" not in cfg:
        cfg["backtest"] = {}
    cfg["backtest"] = {"max_candles": 6000}
    for pair in cfg.get("pairs", []):
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
            # Supertrend ATR-based trend flip (after breakout/pullback checks)
            if df is not None and "supertrend" in df.columns:
                st_val = float(df["supertrend"].iloc[-1])
                st_prev = float(df["supertrend"].iloc[-2]) if len(df) >= 2 else st_val
                if st_val == 1 and st_prev == -1 and price_above_200:
                    return ("supertrend", "supertrend_flip")
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

def choose_sideway_path(ec: dict, strat: Strategist, df, i: int, symbol: str = "", ep_cfg: dict = None) -> tuple:
    """Mirrors live executor._check_sideway_entry() exactly."""
    ep = ep_cfg if ep_cfg else {}
    rsi = ec.get("rsi", 50)
    rvol = ec.get("rvol", 0)
    atr_pct = ec.get("atr_pct", 0)
    adx = ec.get("adx", 0)
    last_close = ec.get("close", 0)
    previous_close = ec.get("close_prev", 0) if ec.get("close_prev") else last_close
    if not last_close:
        return ("skip", "no_close")
    if df is None or len(df) < 25:
        return ("skip", "short_df")

    # BB squeeze + confluence (mirrors live)
    has_bb = all(c in df.columns for c in ("bb_upper", "bb_lower", "bb_middle"))
    if ep.get("bb_squeeze", False) and has_bb:
        bb_w = (df["bb_upper"] - df["bb_lower"]) / df["bb_middle"].clip(lower=1)
        cur_w = float(bb_w.iloc[-1])
        min_w = float(bb_w.iloc[-20:-1].min())
        expanding = cur_w > min_w * 1.12
        bb_u = float(df["bb_upper"].iloc[-1])
        bb_l = float(df["bb_lower"].iloc[-1])
        near_upper = last_close >= bb_u * 0.99
        near_lower = last_close <= bb_l * 1.01
        rsi_rising = rsi > float(df["rsi"].iloc[-3]) if "rsi" in df.columns and len(df) >= 3 else False
        price_falling = last_close < float(df["close"].iloc[-3]) if len(df) >= 3 else False
        bearish_1h = False

        if expanding and rvol > 0.8:
            if near_upper:
                return ("bb_squeeze", "bb_squeeze")
            if near_lower or not bearish_1h:
                return ("bb_squeeze", "bb_squeeze")
        if expanding and rvol > 0.6 and adx > 25 and not bearish_1h:
            return ("bb_squeeze", "bb_squeeze")

    # trend_bounce: buy pullback to lower BB within uptrend
    if ep.get("trend_bounce", False):
        bb_l = float(df["bb_lower"].iloc[-1]) if "bb_lower" in df.columns else 0
        above_200 = ec.get("price_above_200_ema", False)
        if above_200 and bb_l > 0:
            near_lower = last_close <= bb_l * 1.02 and last_close >= bb_l * 0.97
            if near_lower and rsi < 45 and rvol > 0.3:
                return ("trend_bounce", "trend_bounce")

    # scalping_5m: quick oversold bounces on 5m timeframe
    if ep.get("scalping_5m", False):
        df_5m = strat.data.get(symbol, {}).get("5m")
        if df_5m is not None and len(df_5m) >= 50 and "bb_lower" in df_5m.columns and "rsi" in df_5m.columns:
            c5 = float(df_5m["close"].iloc[-1])
            bl = float(df_5m["bb_lower"].iloc[-1])
            rsi5 = float(df_5m["rsi"].iloc[-1])
            rsi5_prev = float(df_5m["rsi"].iloc[-2]) if len(df_5m) >= 2 else rsi5
            near_bb = c5 <= bl * 1.02
            oversold = rsi5 < 55
            recovering = rsi5 > rsi5_prev
            if ep.get("scalp_original", False):
                if c5 <= bl * 1.005 and rsi5 < 35 and recovering:
                    return ("scalp_original", "scalp_original")
            if near_bb and oversold and recovering:
                return ("scalping_5m", "scalping_5m")

    # lowvol_scalp: ATR < 0.5%, near EMA50, RSI 25-65
    if ep.get("lowvol_scalp", False):
        ema50 = float(df.iloc[-1].get("ema_50", 0)) if "ema_50" in df.columns else 0
        if ema50 > 0:
            near_ema = abs(last_close - ema50) / ema50 * 100 < 1.0
            if near_ema and atr_pct and atr_pct < 0.5 and 25 <= rsi <= 65 and rvol > 0.1:
                return ("lowvol_scalp", "lowvol_scalp")

    # lowvol_momentum: low volatility + uptrend + green candle
    if ep.get("lowvol_momentum", False):
        ema50 = float(df.iloc[-1].get("ema_50", 0)) if "ema_50" in df.columns else 0
        if ema50 > 0:
            above_50 = last_close > ema50
            low_vol = atr_pct and atr_pct < 0.3
            green = last_close > previous_close
            if above_50 and low_vol and green:
                return ("lowvol_momentum", "lowvol_momentum")

    # supertrend: ATR-based trend flip in bull market
    if ep.get("supertrend", False):
        if "supertrend" in df.columns:
            st_val = float(df["supertrend"].iloc[-1])
            st_prev = float(df["supertrend"].iloc[-2]) if len(df) >= 2 else st_val
            above_200 = ec.get("price_above_200_ema", False)
            if st_val == 1 and st_prev == -1 and above_200:
                return ("supertrend", "supertrend")

    # vwap_revert: buy when price below VWAP + RSI oversold
    if ep.get("vwap_revert", False):
        if "vwap" in df.columns and "vwap_lower" in df.columns:
            vwap = float(df["vwap"].iloc[-1])
            if vwap > 0 and last_close <= vwap and rsi < 40:
                return ("vwap_revert", "vwap_revert")

    return ("skip", "no_sideway")


def choose_entry_path_clean(ec: dict, strat: Strategist, symbol: str, df=None, i=0, ep_cfg: dict = None) -> tuple:
    """Trend entry + sideway hypotheses. Returns (path, reason) or ('skip', why)."""
    regime = ec.get("regime", "")
    if regime == "trending":
        if df is not None and "supertrend" in df.columns:
            price_above_200 = ec.get("price_above_200_ema", False)
            st_val = float(df["supertrend"].iloc[-1])
            st_prev = float(df["supertrend"].iloc[-2]) if len(df) >= 2 else st_val
            if st_val == 1 and st_prev == -1 and price_above_200:
                return ("supertrend", "supertrend_flip")
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
            if df is not None and "supertrend" in df.columns:
                st_val = float(df["supertrend"].iloc[-1])
                st_prev = float(df["supertrend"].iloc[-2]) if len(df) >= 2 else st_val
                if st_val == 1 and st_prev == -1 and price_above_200:
                    return ("supertrend", "supertrend_flip")
            if adx > 25 and 40 <= rsi <= 55 and price_above_50:
                return ("continuation", "trend_continuation")
            if adx > 35 and 35 <= rsi <= 65 and price_above_200:
                return ("continuation", "trend_continuation")
            if rsi < ec.get("rsi_oversold", 35) and price_above_200:
                return ("continuation", "trend_continuation")
        # Live also tries sideway strategies in trending regime
        sw = choose_sideway_path(ec, strat, df, i, symbol, ep_cfg)
        if sw[0] != "skip":
            return sw
        return ("skip", "no_setup")
    if regime == "sideways":
        return choose_sideway_path(ec, strat, df, i, symbol, ep_cfg)
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
        self.scalp_cooldown_until = 0

    def check_entry(self, i: int):
        if self.open_position is not None:
            return
        candle = self.df.iloc[i]
        ec = self.strat.entry_conditions.get(self.symbol, {})
        ep_cfg = self.config.get("entry_paths", {}).get(self.symbol, {})
        path, reason = choose_entry_path_clean(ec, self.strat, self.symbol, df=self.df, i=i, ep_cfg=ep_cfg)
        if path == "skip":
            return
        if path == "continuation":
            cooldown = self.config.get("anti_churn", {}).get("continuation", {}).get("cooldown_minutes", 45) * 60
            if self.continuation_cooldown_until > i:
                return
        if path in ("scalping_5m", "scalp_original", "lowvol_momentum") and self.scalp_cooldown_until > i:
            return
        atr = ec.get("atr", 0)
        if atr <= 0:
            return
        entry_price = float(candle["close"])
        # Sideway strategies use fixed or ATR-based TP/SL
        sideway_tp_sl = {
            "bb_squeeze": (0.009, 0.004),
            "trend_bounce": (0.006, 0.004),
            "scalping_5m": (0.007, 0.004),
            "scalp_original": (0.007, 0.004),
            "lowvol_scalp": (0.005, 0.002),
            "lowvol_momentum": (0.005, 0.002),
            "supertrend": (0.013, 0.006),
            "vwap_revert": (0.009, 0.003),
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
            if self.open_position.entry_type in ("scalp_recover", "scalp_norecover", "cross_adx", "cross_ema50") and pnl < 0:
                self.scalp_cooldown_until = i + 3600  # 1-hour cooldown after scalp loss
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
        # Target ~50k iteration steps regardless of total candles
        step = max(1, total // 50000)
        last_pct = 0
        for idx, i in enumerate(range(100, total, step)):
            if idx % 500 == 0:
                pct = (i - 100) / (total - 100) * 100
                if pct - last_pct >= 5:
                    last_pct = pct
                    print(f"    {self.symbol} {self.tf}: {pct:.0f}% ({idx}/{total//step} steps)")
            chunk = self.df.iloc[:i + 1]
            self.strat.data[self.symbol][self.tf] = chunk
            if len(chunk) >= 25:
                self.strat.check_conditions(self.symbol)
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
    df_5m = cache.load(symbol, "5m")
    if df_entry.empty or len(df_entry) < 200:
        return None
    exchange = MockExchange()
    strat = Strategist(config, exchange)
    strat.data[symbol][tf] = df_entry
    strat.data[symbol][tf_exit] = df_exit
    strat.data[symbol]["5m"] = df_5m
    strat.calculate_indicators(symbol, tf)
    strat.calculate_indicators(symbol, tf_exit)
    if not df_5m.empty:
        strat.calculate_indicators(symbol, "5m")
    df = df_entry
    total_candles = len(df)

    # Pre-compute indicators on the full dataframe (one-time cost)
    intervals_per_day = {"5m": 288, "15m": 96, "30m": 48, "1h": 24, "2h": 12, "4h": 6, "1d": 1}
    candles_per_day = intervals_per_day.get(tf, 24)
    max_candles = min(total_candles, days * candles_per_day + 100)
    if max_candles < total_candles:
        df = df.iloc[-max_candles:].reset_index(drop=True)

    # Pre-compute indicators once on the full working dataframe
    bb_period = config["strategy"]["entry"]["bollinger"]["period"]
    bb_std = config["strategy"]["entry"]["bollinger"]["std_dev"]
    ema_period = config["strategy"]["entry"]["ema_period"]
    bb_short = pd_ta.bbands(df["close"], length=bb_period, std=bb_std)
    df["bb_lower"] = bb_short.iloc[:, 0]
    df["bb_middle"] = bb_short.iloc[:, 1]
    df["bb_upper"] = bb_short.iloc[:, 2]
    df["ema_200"] = pd_ta.ema(df["close"], length=ema_period)
    df["atr"] = pd_ta.atr(df["high"], df["low"], df["close"], length=14)
    df["rsi"] = pd_ta.rsi(df["close"], length=14)
    trend_cfg = config.get("strategy", {}).get("trend", {})
    ema_fast = trend_cfg.get("ema_fast", 20)
    ema_slow = trend_cfg.get("ema_slow", 50)
    df["ema_20"] = pd_ta.ema(df["close"], length=ema_fast)
    df["ema_50"] = pd_ta.ema(df["close"], length=ema_slow)
    adx_period = config["strategy"]["regime"].get("adx_period", 14)
    adx_df = pd_ta.adx(df["high"], df["low"], df["close"], length=adx_period)
    df["adx"] = adx_df.iloc[:, 0]

    # SuperTrend (ATR-based trend filter)
    st = pd_ta.supertrend(df["high"], df["low"], df["close"], length=7, multiplier=3)
    df["supertrend"] = st.iloc[:, 1]  # direction: 1=up, -1=down
    df["supertrend_line"] = st.iloc[:, 2]  # the band value

    # VWAP and bands (volume-weighted average price)
    cum_vwap = (df["close"] * df["volume"]).cumsum() / df["volume"].cumsum()
    df["vwap"] = cum_vwap
    sq_diff = df["volume"] * (df["close"] - cum_vwap) ** 2
    vwap_std = np.sqrt(sq_diff.cumsum() / df["volume"].cumsum())
    df["vwap_upper"] = cum_vwap + 2 * vwap_std
    df["vwap_lower"] = cum_vwap - 2 * vwap_std

    strat.data[symbol][tf] = df

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
        "dpd": round(total_pnl / (total_candles / candles_per_day), 2) if total_candles > 0 else 0,
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
    for p in PROFILES:
        pr = [r for r in results if r["profile"] == p]
        if not pr:
            continue
        total_pnl = sum(r["total_pnl"] for r in pr)
        total_trades = sum(r["trades"] for r in pr)
        wins = sum(r["wins"] for r in pr)
        print(f"\n--- {p.upper()} (${total_pnl:+.2f} across {total_trades} trades, {wins} wins) ---")
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


async def run_all(days: int = DEFAULT_DAYS, profile: str = ""):
    results = []
    all_trades = []
    profiles = [profile] if profile else PROFILES
    total = len(profiles) * len(PAIRS)
    done = 0
    for p in profiles:
        for symbol in PAIRS:
            done += 1
            print(f"\n[{done}/{total}] {p} / {symbol} ({done/total*100:.0f}%) ...")
            sys.stdout.flush()
            try:
                r = await run_single(symbol, p, days)
                if r is None:
                    print("no data")
                    continue
                results.append(r)
                print(f"{r['trades']} trades, ${r['total_pnl']:+.2f}")
            except Exception as e:
                print(f"error: {e}")
    print_summary(results)
    summary = aggregate_summary(results, profiles)
    return {"results": results, "summary": summary}


def aggregate_summary(results: list, profiles: list = None) -> dict:
    if not results:
        return {"pairs_with_trades": 0, "total_pairs": 0, "trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "total_pnl": 0, "dpd": 0.0}
    by_profile = {}
    for r in results:
        p = r["profile"]
        if p not in by_profile:
            by_profile[p] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "pairs": set()}
        by_profile[p]["trades"] += r["trades"]
        by_profile[p]["wins"] += r["wins"]
        by_profile[p]["losses"] += r["losses"]
        by_profile[p]["pnl"] += r["total_pnl"]
        by_profile[p]["pairs"].add(r["symbol"])
    pairs_with_trades = sum(1 for r in results if r["trades"] > 0)
    total_trades = sum(r["trades"] for r in results)
    total_wins = sum(r["wins"] for r in results)
    total_pnl = sum(r["total_pnl"] for r in results)
    total_candles = sum(r.get("candles", 0) for r in results)
    daily_candles = total_candles / max(len(results), 1)
    approx_days = max((r.get("approx_days", 0) for r in results), default=365)
    dpd = round(total_pnl / approx_days, 2) if approx_days > 0 else 0
    summary = {
        "pairs_with_trades": pairs_with_trades,
        "total_pairs": len(results),
        "trades": total_trades,
        "wins": total_wins,
        "losses": sum(r["losses"] for r in results),
        "win_rate": round(total_wins / max(total_trades, 1) * 100, 1),
        "pnl": round(total_pnl, 2),
        "dpd": dpd,
        "approx_days": approx_days,
        "by_profile": {p: {k: v for k, v in d.items() if k != "pairs"} for p, d in by_profile.items()},
    }
    return summary


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
    elif args.profile:
        asyncio.run(run_all(args.days, args.profile))
    else:
        asyncio.run(run_all(args.days))


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
