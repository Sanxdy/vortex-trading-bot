import asyncio
import pandas as pd
import pandas_ta as ta
from exchange_wrapper import ExchangeWrapper

class Strategist:
    def __init__(self, config: dict, exchange: ExchangeWrapper):
        self.config = config
        self.exchange = exchange
        self.pairs = [p["name"] for p in config["pairs"] if p.get("enabled", True)]
        self.timeframes = {
            "entry": config["strategy"]["entry"]["timeframe"],
            "exit_trend": config["strategy"]["exit"]["trend_inversion"]["timeframe"]
        }
        self.data: dict = {}
        self.entry_conditions: dict = {}
        self.exit_conditions: dict = {}
        self._prev_entry_conditions: dict = {}
        for pair in self.pairs:
            self.data[pair] = {
                self.timeframes["entry"]: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
                self.timeframes["exit_trend"]: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
            }
            self.entry_conditions[pair] = {"price_at_lower_bb": False, "price_above_200_ema": False}
            self.exit_conditions[pair] = {"price_at_upper_bb": False, "price_below_200_ema_1h": False}

    async def backfill(self, symbol: str, timeframe: str):
        try:
            candles = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=1000)
            if candles:
                rows = [{"timestamp": pd.to_datetime(c[0], unit='ms'),
                         "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": float(c[5])} for c in candles]
                df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).tail(800)
                self.data[symbol][timeframe] = df
                need = self.config["strategy"]["entry"]["bollinger"]["period"]
                enough = len(df) >= need
                self.calculate_indicators(symbol, timeframe)
                print(f"Backfilled {len(df)} {timeframe} candles for {symbol} {'✅' if enough else f'❌ needs {need}'}")
        except Exception as e:
            print(f"Backfill error ({symbol}/{timeframe}): {e}")

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        while True:
            try:
                candles = await self.exchange.watch_ohlcv(symbol, timeframe)
                if not candles:
                    await asyncio.sleep(1)
                    continue
                rows = []
                for c in candles:
                    rows.append({"timestamp": pd.to_datetime(c[0], unit='ms'),
                                 "open": float(c[1]), "high": float(c[2]),
                                 "low": float(c[3]), "close": float(c[4]),
                                 "volume": float(c[5])})
                df = self.data[symbol][timeframe]
                df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True).drop_duplicates(subset=["timestamp"]).tail(800)
                self.data[symbol][timeframe] = df
                self.calculate_indicators(symbol, timeframe)
            except Exception as e:
                print(f"Strategist ({symbol}/{timeframe}): {e}")
                await asyncio.sleep(1)

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
        trend_uptrend = ema50_val > 0 and ema20_val > ema50_val and "ema_200" in df_entry.columns and last_close > df_entry.iloc[-1]["ema_200"]
        near_ema20 = ema20_val > 0 and abs(last_close - ema20_val) / ema20_val < 0.01
        self.entry_conditions[symbol]["trend_uptrend"] = trend_uptrend
        self.entry_conditions[symbol]["trend_pullback"] = trend_uptrend and near_ema20 and rsi_val < 60
        self.entry_conditions[symbol]["trend_pullback_price"] = ema20_val if near_ema20 else 0
        self.entry_conditions[symbol]["last_price"] = last_close
        bb_upper = float(df_entry.iloc[-1]["bb_upper"]) if "bb_upper" in df_entry.columns else 0
        prev_close = float(df_entry.iloc[-2]["close"]) if len(df_entry) >= 2 else 0
        prev_bb_upper = float(df_entry.iloc[-2]["bb_upper"]) if len(df_entry) >= 2 and "bb_upper" in df_entry.columns else 0
        self.entry_conditions[symbol]["trend_breakout"] = (
            adx > 35 and rsi_val > 70 and last_close > bb_upper
            and prev_close < prev_bb_upper
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

    def all_regime_is(self, regime: str) -> bool:
        return all(
            ec.get("regime") == regime
            for ec in self.entry_conditions.values()
            if ec.get("regime")
        )

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

        if analyst_signal == "STRONG_DOWNTREND":
            score -= 35
        elif analyst_signal in ("STRONG_UPTREND",):
            score -= 20

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

    def evaluate_thesis_add(self, symbol: str, position_state) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        prev_ec = self._prev_entry_conditions.get(symbol, {})
        current_price = ec.get("close", 0)
        entry_price = position_state.get("avg_entry_price", 0)
        if not entry_price:
            return False

        entry_time = position_state.get("last_entry_attempt", 0)
        now = __import__("time").time()
        minutes_since_entry = (now - entry_time) / 60
        if minutes_since_entry < 15:
            return False

        atr = ec.get("atr", 0)
        if atr <= 0:
            return False
        drawdown = entry_price - current_price
        if drawdown < atr * 1.5:
            return False

        analyst = ec.get("analyst_signal", "NEUTRAL")
        if analyst == "STRONG_DOWNTREND":
            return False

        current_adx = ec.get("adx", 0)
        prev_adx = prev_ec.get("adx", 0)
        current_rsi = ec.get("rsi", 50)
        prev_rsi = prev_ec.get("rsi", 50)

        momentum_flattening = current_adx <= prev_adx
        rsi_hooking_up = current_rsi > prev_rsi and current_rsi < 45

        return bool(momentum_flattening and rsi_hooking_up)

    def get_profile_params(self, symbol: str) -> dict:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        adx = ec.get("adx", 0)
        above_200 = ec.get("price_above_200_ema", False)

        if regime == "sideways" or adx < 20:
            return {"tp_atr": 2.0, "sl_atr": 1.5, "thesis_add": True}
        elif regime == "trending" and adx <= 35 and above_200:
            return {"tp_atr": 2.0, "sl_atr": 1.5, "thesis_add": True}
        elif regime == "trending" and adx <= 35 and not above_200:
            return {"tp_atr": 1.0, "sl_atr": 0.8, "thesis_add": False}
        elif adx > 35:
            return {"tp_atr": 2.5, "sl_atr": 2.0, "thesis_add": False}
        return {"tp_atr": 1.5, "sl_atr": 2.0, "thesis_add": True}

    def should_enter(self, symbol: str) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        if regime == "trending":
            adx = ec.get("adx", 0)
            if adx > 30:
                return ec.get("rsi", 50) > 60 and ec.get("price_above_200_ema", False)
            return ec.get("rsi", 50) < ec.get("rsi_oversold", 35) and ec.get("price_above_200_ema", False)
        elif regime == "sideways":
            return ec.get("price_at_lower_bb", False)
        return False

    def should_exit_take_profit(self, symbol: str) -> bool:
        return self.exit_conditions[symbol]["price_at_upper_bb"]

    def should_exit_trend_inversion(self, symbol: str) -> bool:
        return self.exit_conditions[symbol]["price_below_200_ema_1h"]

    async def run(self):
        for pair in self.pairs:
            await self.backfill(pair, self.timeframes["entry"])
            await self.backfill(pair, self.timeframes["exit_trend"])
        tasks = []
        for pair in self.pairs:
            tasks.append(self.watch_ohlcv(pair, self.timeframes["entry"]))
            tasks.append(self.watch_ohlcv(pair, self.timeframes["exit_trend"]))
        await asyncio.gather(*tasks)
