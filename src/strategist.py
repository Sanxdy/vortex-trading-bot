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
        self.entry_conditions[symbol]["price_above_200_ema"] = "ema_200" in df_entry.columns and last_close > df_entry.iloc[-1]["ema_200"]
        self.entry_conditions[symbol]["atr"] = float(df_entry.iloc[-1]["atr"]) if "atr" in df_entry.columns else 0
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
        self.entry_conditions[symbol]["rsi_oversold"] = self.config["strategy"]["entry"].get("rsi_oversold", 35)
        rsi_val = float(df_entry.iloc[-1]["rsi"]) if "rsi" in df_entry.columns else 50
        ema20_val = float(df_entry.iloc[-1]["ema_20"]) if "ema_20" in df_entry.columns else 0
        ema50_val = float(df_entry.iloc[-1]["ema_50"]) if "ema_50" in df_entry.columns else 0
        trend_uptrend = ema50_val > 0 and ema20_val > ema50_val and "ema_200" in df_entry.columns and last_close > df_entry.iloc[-1]["ema_200"]
        near_ema20 = ema20_val > 0 and abs(last_close - ema20_val) / ema20_val < 0.01
        self.entry_conditions[symbol]["trend_uptrend"] = trend_uptrend
        self.entry_conditions[symbol]["trend_pullback"] = trend_uptrend and near_ema20 and rsi_val < 60
        self.entry_conditions[symbol]["trend_pullback_price"] = ema20_val if near_ema20 else 0
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
        return self.entry_conditions.get(symbol, {}).get("trend_pullback", False)

    def get_trend_price(self, symbol: str) -> float:
        cond = self.entry_conditions.get(symbol, {})
        if cond.get("regime") != "trending":
            return 0
        return cond.get("trend_pullback_price", 0)

    def should_enter(self, symbol: str) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        if regime == "trending":
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
