import asyncio
from typing import Dict, List, Optional, Tuple

import ccxt
import numpy as np
import pandas as pd
import pandas_ta as ta

from exchange_wrapper import ExchangeWrapper
from activity import push_activity

class Strategist:
    def __init__(self, config: dict, exchange: ExchangeWrapper):
        self.config = config
        self.exchange = exchange
        try:
            self.data_exchange = ccxt.binance()
        except Exception:
            self.data_exchange = None
        self.pairs = [p["name"] for p in config["pairs"] if p.get("enabled", True)]
        self.timeframes = {
            "entry": config["strategy"]["entry"]["timeframe"],
            "exit_trend": config["strategy"]["exit"]["trend_inversion"]["timeframe"]
        }
        self.data: dict = {}
        self.entry_conditions: dict = {}
        self.exit_conditions: dict = {}
        self._prev_entry_conditions: dict = {}
        self.allow_breakout_override: Optional[bool] = None
        self._last_closed_ts: dict = {}
        for pair in self.pairs:
            self.data[pair] = {
                self.timeframes["entry"]: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
                self.timeframes["exit_trend"]: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
                "5m": pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
            }
            self.entry_conditions[pair] = {"price_at_lower_bb": False, "price_above_200_ema": False}
            self.exit_conditions[pair] = {"price_at_upper_bb": False, "price_below_200_ema_1h": False}

    async def backfill(self, symbol: str, timeframe: str):
        try:
            src = self.data_exchange if self.data_exchange else self.exchange
            candles = await asyncio.to_thread(src.fetch_ohlcv, symbol, timeframe, limit=1000)
            if candles:
                rows = [{"timestamp": pd.to_datetime(c[0], unit='ms'),
                         "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": float(c[5])} for c in candles]
                max_candles = self.config.get("backtest", {}).get("max_candles", 800)
                df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).tail(max_candles)
                self.data[symbol][timeframe] = df
                need = self.config["strategy"]["entry"]["bollinger"]["period"]
                enough = len(df) >= need
                self.calculate_indicators(symbol, timeframe)
                print(f"Backfilled {len(df)} {timeframe} candles for {symbol} {'✅' if enough else f'❌ needs {need}'}")
        except Exception as e:
            print(f"Backfill error ({symbol}/{timeframe}): {e}")
            await push_activity(f"Backfill error ({symbol}/{timeframe}): {e}", "error")

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        key = f"{symbol}:{timeframe}"
        while True:
            try:
                src = self.data_exchange if self.data_exchange else self.exchange
                candles = await asyncio.to_thread(src.fetch_ohlcv, symbol, timeframe, limit=6)
                if not candles or len(candles) < 2:
                    await asyncio.sleep(60)
                    continue
                # Use the most recently COMPLETED candle (second-to-last).
                # The last candle is the current forming one (volume may be 0).
                completed = candles[-2]
                ts = completed[0]
                prev_ts = self._last_closed_ts.get(key)
                if prev_ts == ts:
                    await asyncio.sleep(60)
                    continue
                # New candle closed — update data
                rows = []
                for c in candles[:-1]:  # exclude current forming candle
                    rows.append({"timestamp": pd.to_datetime(c[0], unit='ms'),
                                 "open": float(c[1]), "high": float(c[2]),
                                 "low": float(c[3]), "close": float(c[4]),
                                 "volume": float(c[5])})
                df = self.data[symbol][timeframe]
                max_candles = self.config.get("backtest", {}).get("max_candles", 800)
                df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True).drop_duplicates(subset=["timestamp"]).tail(max_candles)
                self.data[symbol][timeframe] = df
                self.calculate_indicators(symbol, timeframe)
                self._last_closed_ts[key] = ts
                print(f"watch_ohlcv updated {symbol}/{timeframe} — new candle @ {pd.to_datetime(ts, unit='ms')}")
            except Exception as e:
                print(f"watch_ohlcv error ({symbol}/{timeframe}): {e}")
                await push_activity(f"watch_ohlcv error ({symbol}/{timeframe}): {e}", "error")
            await asyncio.sleep(60)

    def calculate_indicators(self, symbol: str, timeframe: str):
        df = self.data[symbol][timeframe].copy()
        bb_period = self.config["strategy"]["entry"]["bollinger"]["period"]
        bb_std = self.config["strategy"]["entry"]["bollinger"]["std_dev"]
        ema_period = self.config["strategy"]["entry"]["ema_period"] if timeframe == self.timeframes["entry"] else self.config["strategy"]["exit"]["trend_inversion"]["ema_period"]
        if len(df) >= bb_period:
            bb = ta.bbands(df["close"], length=bb_period, std=bb_std)
            df["bb_lower"] = bb.iloc[:, 0]
            df["bb_middle"] = bb.iloc[:, 1]
            df["bb_upper"] = bb.iloc[:, 2]
        if len(df) >= ema_period:
            df["ema_200"] = ta.ema(df["close"], length=ema_period)
        if len(df) >= 14:
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
            df["rsi"] = ta.rsi(df["close"], length=14)
        if timeframe == self.timeframes["entry"]:
            trend_cfg = self.config["strategy"].get("trend", {})
            ema_fast = trend_cfg.get("ema_fast", 20)
            ema_slow = trend_cfg.get("ema_slow", 50)
            if len(df) >= ema_slow:
                df["ema_20"] = ta.ema(df["close"], length=ema_fast)
                df["ema_50"] = ta.ema(df["close"], length=ema_slow)
        adx_period = self.config["strategy"]["regime"].get("adx_period", 14)
        if len(df) >= adx_period + 1:
            adx_df = ta.adx(df["high"], df["low"], df["close"], length=adx_period)
            df["adx"] = adx_df.iloc[:, 0]
            df["adx_pos"] = adx_df.iloc[:, 1] if adx_df.shape[1] > 1 else 0
            df["adx_neg"] = adx_df.iloc[:, 2] if adx_df.shape[1] > 2 else 0
        if len(df) >= 14:
            df["supertrend"] = ta.supertrend(df["high"], df["low"], df["close"], length=7, multiplier=3).iloc[:, 1]
            cum_vwap = (df["close"] * df["volume"]).cumsum() / df["volume"].cumsum()
            df["vwap"] = cum_vwap
            sq_diff = df["volume"] * (df["close"] - cum_vwap) ** 2
            vwap_std = np.sqrt(sq_diff.cumsum() / df["volume"].cumsum())
            df["vwap_lower"] = cum_vwap - 2 * vwap_std
            df["vwap_upper"] = cum_vwap + 2 * vwap_std
        self.data[symbol][timeframe] = df
        if len(df) >= bb_period:
            self.check_conditions(symbol)
 
    def check_conditions(self, symbol: str):
        self._prev_entry_conditions[symbol] = self.entry_conditions.get(symbol, {}).copy()
        tf_entry = self.timeframes["entry"]
        df_entry = self.data[symbol][tf_entry]
        bb_period = self.config["strategy"]["entry"]["bollinger"]["period"]
        ema_period = self.config["strategy"]["entry"]["ema_period"]
        if len(df_entry) < bb_period or "bb_lower" not in df_entry.columns:
            return
        last_close = df_entry.iloc[-1]["close"]
        last_lower_bb = df_entry.iloc[-1]["bb_lower"]
        bb_threshold = self.config["strategy"]["entry"].get("bb_threshold", 0.001)
        self.entry_conditions[symbol]["price_at_lower_bb"] = abs(last_close - last_lower_bb) / last_lower_bb < bb_threshold
        self.entry_conditions[symbol]["bb_lower"] = last_lower_bb
        self.entry_conditions[symbol]["price_above_200_ema"] = "ema_200" in df_entry.columns and last_close > df_entry.iloc[-1]["ema_200"]
        self.entry_conditions[symbol]["atr"] = float(df_entry.iloc[-1]["atr"]) if "atr" in df_entry.columns else 0
        self.entry_conditions[symbol]["close"] = last_close
        self.entry_conditions[symbol]["close_prev"] = float(df_entry.iloc[-2]["close"]) if len(df_entry) >= 2 else last_close
        self.entry_conditions[symbol]["atr_pct"] = round(self.entry_conditions[symbol]["atr"] / last_close, 4) if last_close > 0 else 0
        adx = float(df_entry.iloc[-1]["adx"]) if "adx" in df_entry.columns else 0
        atr_val = self.entry_conditions[symbol]["atr"]
        avg_atr = float(df_entry["atr"].mean()) if "atr" in df_entry.columns else 0
        adx_threshold = self.config["strategy"]["regime"].get("adx_trend_threshold", 20)
        vol_spike = self.config["strategy"]["regime"].get("atr_vol_spike", 2.0)
        if adx > adx_threshold:
            self.entry_conditions[symbol]["regime"] = "trending"
        elif avg_atr > 0 and atr_val > avg_atr * vol_spike:
            self.entry_conditions[symbol]["regime"] = "high_vol"
        else:
            self.entry_conditions[symbol]["regime"] = "sideways"
        self.entry_conditions[symbol]["adx"] = adx
        # ADX slope (change over 3 bars)
        adx_series = df_entry["adx"] if "adx" in df_entry.columns else None
        if adx_series is not None and len(adx_series) >= 4:
            adx_slope = float(adx_series.iloc[-1] - adx_series.iloc[-4])
        else:
            adx_slope = 0
        self.entry_conditions[symbol]["adx_slope"] = adx_slope
        # RVOL (last volume / 20-bar avg volume)
        if "volume" in df_entry.columns and len(df_entry) >= 21:
            vols = df_entry["volume"].values[-21:-1]
            avg_vol = float(vols.mean()) if len(vols) > 0 else 1
            last_vol = float(df_entry["volume"].iloc[-1])
            # If last candle is current forming (partial volume), use previous completed candle's volume
            if len(df_entry) >= 3:
                prev_vol = float(df_entry["volume"].iloc[-2])
                if prev_vol > last_vol:
                    last_vol = prev_vol
            self.entry_conditions[symbol]["rvol"] = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0
        else:
            self.entry_conditions[symbol]["rvol"] = 1.0
        # Candle efficiency (avg abs(close-open)/(high-low) over last 20)
        if len(df_entry) >= 21:
            effs = []
            for i in range(-20, 0):
                rng = float(df_entry["high"].iloc[i] - df_entry["low"].iloc[i])
                if rng > 0:
                    effs.append(abs(float(df_entry["close"].iloc[i] - df_entry["open"].iloc[i])) / rng)
            self.entry_conditions[symbol]["candle_eff"] = round(sum(effs) / len(effs), 2) if effs else 0.5
        else:
            self.entry_conditions[symbol]["candle_eff"] = 0.5
        self.entry_conditions[symbol]["rsi_oversold"] = self.config["strategy"]["entry"].get("rsi_oversold", 35)
        rsi_val = float(df_entry.iloc[-1]["rsi"]) if "rsi" in df_entry.columns else 50
        ema20_val = float(df_entry.iloc[-1]["ema_20"]) if "ema_20" in df_entry.columns else 0
        ema50_val = float(df_entry.iloc[-1]["ema_50"]) if "ema_50" in df_entry.columns else 0
        self.entry_conditions[symbol]["price_above_50_ema"] = ema50_val > 0 and last_close > ema50_val
        trend_uptrend = ema50_val > 0 and ema20_val > ema50_val and "ema_200" in df_entry.columns and last_close > df_entry.iloc[-1]["ema_200"]
        near_ema20 = ema20_val > 0 and abs(last_close - ema20_val) / ema20_val < 0.01
        self.entry_conditions[symbol]["trend_uptrend"] = trend_uptrend
        self.entry_conditions[symbol]["trend_pullback"] = trend_uptrend and near_ema20 and rsi_val < 65
        self.entry_conditions[symbol]["trend_pullback_price"] = ema20_val if near_ema20 else 0
        self.entry_conditions[symbol]["last_price"] = last_close
        bb_upper = float(df_entry.iloc[-1]["bb_upper"]) if "bb_upper" in df_entry.columns else 0
        prev_close = float(df_entry.iloc[-2]["close"]) if len(df_entry) >= 2 else 0
        prev_bb_upper = float(df_entry.iloc[-2]["bb_upper"]) if len(df_entry) >= 2 and "bb_upper" in df_entry.columns else 0
        self.entry_conditions[symbol]["trend_breakout"] = (
            adx > 35 and rsi_val > 70 and last_close > bb_upper
            and prev_close < prev_bb_upper
        )
        # LTF (5m) indicators for multi-timeframe confirmation
        tf_5m = "5m"
        if tf_5m in self.data.get(symbol, {}) and len(self.data[symbol][tf_5m]) >= 20:
            df_5m = self.data[symbol][tf_5m]
            self.entry_conditions[symbol]["ltf_rsi"] = float(df_5m["rsi"].iloc[-1]) if "rsi" in df_5m.columns else 50
            self.entry_conditions[symbol]["ltf_close"] = float(df_5m["close"].iloc[-1])
            if "ema_20" in df_5m.columns:
                self.entry_conditions[symbol]["ltf_ema_20"] = float(df_5m["ema_20"].iloc[-1])
            else:
                df_5m["ema_20"] = ta.ema(df_5m["close"], length=20)
                self.entry_conditions[symbol]["ltf_ema_20"] = float(df_5m["ema_20"].iloc[-1])
        allow_breakout = self.allow_breakout_override if self.allow_breakout_override is not None else self.config.get("strategy", {}).get("trend", {}).get("allow_breakout", False)
        if allow_breakout:
            self.entry_conditions[symbol]["trend_breakout"] = (
                self.entry_conditions[symbol]["trend_breakout"]
                or (adx > 30 and rsi_val > 50 and trend_uptrend)
            )
        self.entry_conditions[symbol]["rsi"] = rsi_val
        self.entry_conditions[symbol]["ema_20"] = ema20_val
        self.entry_conditions[symbol]["ema_50"] = ema50_val
        tf_exit = self.timeframes["exit_trend"]
        df_exit = self.data[symbol][tf_exit]
        if "ema_200" in df_exit.columns:
            self.exit_conditions[symbol]["price_below_200_ema_1h"] = df_exit.iloc[-1]["close"] < df_exit.iloc[-1]["ema_200"]
        if "bb_upper" in df_entry.columns:
            self.exit_conditions[symbol]["price_at_upper_bb"] = abs(last_close - df_entry.iloc[-1]["bb_upper"]) / df_entry.iloc[-1]["bb_upper"] < 0.001

    def get_regime(self, symbol: str) -> str:
        return self.entry_conditions.get(symbol, {}).get("regime", "unknown")

    def should_enter_trend(self, symbol: str) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        return ec.get("trend_pullback", False) or ec.get("trend_breakout", False)

    def get_trend_price(self, symbol: str) -> float:
        cond = self.entry_conditions.get(symbol, {})
        if cond.get("regime") != "trending":
            return 0
        if cond.get("trend_breakout"):
            return cond.get("last_price", 0)
        return cond.get("trend_pullback_price", 0)

    def evaluate_countertrend_scalp(self, symbol: str, analyst_signal: str = "NEUTRAL") -> int:
        ec = self.entry_conditions.get(symbol, {})
        score = 50

        rsi = ec.get("rsi", 50)
        close = ec.get("close", 0)
        bb_lower = ec.get("bb_lower", 0)
        adx = ec.get("adx", 0)
        adx_slope = ec.get("adx_slope", 0)
        atr_pct = ec.get("atr_pct", 0)

        if rsi < 25:
            score += 30
        elif rsi < 35:
            score += 20

        if close > 0 and bb_lower > 0 and close <= bb_lower:
            score += 20

        if adx > 25 and adx_slope < 0:
            score += 15

        if atr_pct > 0.03:
            score -= 15

        return max(0, min(100, score))

    # ═══════════════════════════════════════════════════════════
    # Adaptive countertrend entry (replaces hard 1h EMA200 block)
    # ═══════════════════════════════════════════════════════════

    PILOT_PAIRS = ["SOL/USDT", "BTC/USDT", "ETH/USDT"]

    def _adx_slope(self, symbol: str, window: int = 5) -> float:
        ec = self.entry_conditions.get(symbol, {})
        return ec.get("adx_slope", 0)

    def _bear_candle_eff(self, symbol: str, window: int = 5) -> float:
        df = self.data.get(symbol, {}).get(self.timeframes["entry"])
        if df is None or len(df) < window:
            return 0.5
        effs = []
        for i in range(-window, 0):
            h = float(df.iloc[i]["high"])
            l = float(df.iloc[i]["low"])
            o = float(df.iloc[i]["open"])
            if h == l:
                effs.append(0.5)
            else:
                effs.append((o - l) / (h - l))
        return float(np.mean(effs)) if effs else 0.5

    def _market_panic(self) -> bool:
        btc_df = self.data.get("BTC/USDT", {}).get(self.timeframes["entry"])
        if btc_df is None or len(btc_df) < 4:
            return False
        lookback = min(12, len(btc_df))

        for i in range(1, lookback):
            row = btc_df.iloc[-i]
            drop = (float(row["open"]) - float(row["close"])) / float(row["open"]) * 100
            if drop > 3.0:
                print(f"  Panic: {drop:.1f}% single-candle drop in BTC -{i}")
                return True

        if lookback >= 4:
            for i in range(1, lookback - 2):
                period_open = float(btc_df.iloc[-(i+2)]["open"])
                period_close = float(btc_df.iloc[-i]["close"])
                if period_open > period_close:
                    cumulative = (period_open - period_close) / period_open * 100
                    if cumulative > 4.5:
                        print(f"  Panic: 4.5%+ cumulative bleed over 3 candles")
                        return True
        return False

    def evaluate_countertrend_entry(self, symbol: str, ct_score: int) -> Tuple[bool, Optional[Dict]]:
        if self.config.get("safety", {}).get("panic_revert_to_safe_mode", False):
            return False, None
        if symbol not in self.PILOT_PAIRS:
            return False, None
        if self._market_panic():
            print(f"  Panic active — countertrend blocked for {symbol}")
            return False, None
        if ct_score < 55:
            return False, None
        adx_slope = self._adx_slope(symbol)
        bear_eff = self._bear_candle_eff(symbol)
        rvol = self.entry_conditions.get(symbol, {}).get("rvol", 1)
        if adx_slope > 0.2 and bear_eff > 0.6 and rvol > 2.0:
            print(f"  Accelerating bear trend — countertrend blocked for {symbol}")
            return False, None
        # Two-tier sizing
        if ct_score >= 70:
            size_mult = 0.15; stop_mult = 0.8; time_limit = 25
        else:
            size_mult = 0.07; stop_mult = 0.75; time_limit = 15
        return True, {
            "size_multiplier": size_mult,
            "stop_atr_multiplier": stop_mult,
            "time_limit_minutes": time_limit,
            "force_exit_on_timeout": True,
        }

    def get_profile_params(self, symbol: str) -> dict:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        adx = ec.get("adx", 0)
        above_200 = ec.get("price_above_200_ema", False)

        if regime == "sideways" or adx < 20:
            return {"tp_atr": 2.0, "sl_atr": 2.0, "thesis_add": True}
        elif regime == "trending" and adx <= 35 and above_200:
            return {"tp_atr": 2.0, "sl_atr": 1.5, "thesis_add": True}
        elif regime == "trending" and adx <= 35 and not above_200:
            return {"tp_atr": 1.0, "sl_atr": 0.8, "thesis_add": False}
        elif adx > 35:
            return {"tp_atr": 2.5, "sl_atr": 2.0, "thesis_add": False}
            return {"tp_atr": 1.5, "sl_atr": 2.0, "thesis_add": True}

    def get_breakeven_pct(self, default: float = 0.2) -> float:
        active_profile = self.config.get("active_profile", "standard")
        profiles = self.config.get("profiles", {})
        prof = profiles.get(active_profile, {})
        strategy = prof.get("strategy", {})
        try:
            return float(strategy.get("breakeven_pct", default)) / 100.0
        except Exception:
            return default / 100.0

    def evaluate_thesis_add(self, symbol: str, pos_state: dict) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        if regime == "sideways":
            return False
        elapsed = asyncio.get_event_loop().time() - pos_state.get("last_entry_attempt", 0)
        if elapsed < 300:
            return False
        avg_entry = pos_state.get("avg_entry_price", 0)
        if avg_entry <= 0:
            return False
        profit_pct = (ec.get("close", 0) - avg_entry) / avg_entry
        if profit_pct < 0.003:
            return False
        if self._market_panic():
            return False
        return True

    def should_enter(self, symbol: str) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        if regime == "trending":
            adx = ec.get("adx", 0)
            rsi = ec.get("rsi", 50)
            if adx > 35:
                if 35 <= rsi <= 65 and ec.get("price_above_200_ema", False):
                    return True
            if adx > 25 and 40 <= rsi <= 55 and ec.get("price_above_50_ema", False):
                return True
            return rsi < ec.get("rsi_oversold", 35) and ec.get("price_above_200_ema", False)
        elif regime == "sideways":
            return True
        return False

    def should_exit_take_profit(self, symbol: str) -> bool:
        return self.exit_conditions[symbol]["price_at_upper_bb"]

    def should_exit_trend_inversion(self, symbol: str) -> bool:
        return self.exit_conditions[symbol]["price_below_200_ema_1h"]

    async def run(self):
        for pair in self.pairs:
            await self.backfill(pair, self.timeframes["entry"])
            await self.backfill(pair, self.timeframes["exit_trend"])
            await self.backfill(pair, "5m")
        tasks = []
        for pair in self.pairs:
            tasks.append(self.watch_ohlcv(pair, self.timeframes["entry"]))
            tasks.append(self.watch_ohlcv(pair, self.timeframes["exit_trend"]))
            tasks.append(self.watch_ohlcv(pair, "5m"))
        await asyncio.gather(*tasks)
