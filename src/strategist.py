import asyncio
import pandas as pd
import pandas_ta as ta
from exchange_wrapper import ExchangeWrapper

class Strategist:
    def __init__(self, config: dict, exchange: ExchangeWrapper):
        self.config = config
        self.exchange = exchange
        self.symbol = config["grid"]["pair"]
        self.timeframes = {
            "entry": config["strategy"]["entry"]["timeframe"],
            "exit_trend": config["strategy"]["exit"]["trend_inversion"]["timeframe"]
        }
        self.ohlcv_data = {
            self.timeframes["entry"]: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
            self.timeframes["exit_trend"]: pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        }
        self.entry_conditions = {
            "price_at_lower_bb": False,
            "price_above_200_ema": False
        }
        self.exit_conditions = {
            "price_at_upper_bb": False,
            "price_below_200_ema_1h": False
        }

    async def watch_ohlcv(self, timeframe: str):
        while True:
            try:
                ohlcv = await self.exchange.watch_ohlcv(self.symbol, timeframe)
                new_row = pd.DataFrame([{
                    "timestamp": pd.to_datetime(ohlcv[0], unit='ms'),
                    "open": float(ohlcv[1]),
                    "high": float(ohlcv[2]),
                    "low": float(ohlcv[3]),
                    "close": float(ohlcv[4]),
                    "volume": float(ohlcv[5])
                }])
                df = self.ohlcv_data[timeframe]
                df = pd.concat([df, new_row], ignore_index=True).drop_duplicates(subset=["timestamp"]).tail(200)
                self.ohlcv_data[timeframe] = df
                self.calculate_indicators(timeframe)
            except Exception as e:
                print(f"Strategist OHLCV error ({timeframe}): {e}")
                await asyncio.sleep(1)

    def calculate_indicators(self, timeframe: str):
        df = self.ohlcv_data[timeframe].copy()
        if len(df) < 200:
            return
        bb_period = self.config["strategy"]["entry"]["bollinger"]["period"]
        bb_std = self.config["strategy"]["entry"]["bollinger"]["std_dev"]
        bb = ta.bbands(df["close"], length=bb_period, std=bb_std)
        df["bb_lower"] = bb.iloc[:, 0]
        df["bb_middle"] = bb.iloc[:, 1]
        df["bb_upper"] = bb.iloc[:, 2]
        ema_period = self.config["strategy"]["entry"]["ema_period"] if timeframe == self.timeframes["entry"] else self.config["strategy"]["exit"]["trend_inversion"]["ema_period"]
        df["ema_200"] = ta.ema(df["close"], length=ema_period)
        self.ohlcv_data[timeframe] = df
        self.check_conditions()

    def check_conditions(self):
        tf_entry = self.timeframes["entry"]
        df_entry = self.ohlcv_data[tf_entry]
        if len(df_entry) < 200:
            return
        last_close = df_entry.iloc[-1]["close"]
        last_lower_bb = df_entry.iloc[-1]["bb_lower"]
        last_ema_200 = df_entry.iloc[-1]["ema_200"]
        self.entry_conditions["price_at_lower_bb"] = abs(last_close - last_lower_bb) / last_lower_bb < 0.001
        self.entry_conditions["price_above_200_ema"] = last_close > last_ema_200
        tf_exit = self.timeframes["exit_trend"]
        df_exit = self.ohlcv_data[tf_exit]
        if len(df_exit) >= 200:
            last_close_1h = df_exit.iloc[-1]["close"]
            last_ema_200_1h = df_exit.iloc[-1]["ema_200"]
            self.exit_conditions["price_below_200_ema_1h"] = last_close_1h < last_ema_200_1h
        if len(df_entry) >= 20:
            last_upper_bb = df_entry.iloc[-1]["bb_upper"]
            self.exit_conditions["price_at_upper_bb"] = abs(last_close - last_upper_bb) / last_upper_bb < 0.001

    def should_enter(self) -> bool:
        return self.entry_conditions["price_at_lower_bb"] and self.entry_conditions["price_above_200_ema"]

    def should_exit_take_profit(self) -> bool:
        return self.exit_conditions["price_at_upper_bb"]

    def should_exit_trend_inversion(self) -> bool:
        return self.exit_conditions["price_below_200_ema_1h"]

    async def run(self):
        await asyncio.gather(
            self.watch_ohlcv(self.timeframes["entry"]),
            self.watch_ohlcv(self.timeframes["exit_trend"])
        )
