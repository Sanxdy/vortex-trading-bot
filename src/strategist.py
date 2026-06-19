import asyncio
from typing import Dict, List, Optional, Tuple
import ccxt
import numpy as np
import pandas as pd
import pandas_ta as ta
from exchange_wrapper import ExchangeWrapper
from activity import push_activity

NFI_TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]
BTC_TIMEFRAMES = ["1h", "4h", "1d"]

class Strategist:
    def __init__(self, config: dict, exchange: ExchangeWrapper):
        self.config = config
        self.exchange = exchange
        self._is_futures = "binanceusdm" in config.get("exchange", {}).get("name", "")
        try:
            self.data_exchange = ccxt.binance({"options": {"defaultType": "spot", "fetchMarkets": ["spot"]}})
        except Exception:
            self.data_exchange = None
        self.pairs = [p["name"] for p in config["pairs"] if p.get("enabled", True)]
        self.base_tf = config.get("strategy", {}).get("entry", {}).get("timeframe", "5m")
        self.data: dict = {}
        self.entry_conditions: dict = {}
        self.exit_conditions: dict = {}
        self._prev_entry_conditions: dict = {}
        self.allow_breakout_override: Optional[bool] = None
        self._last_closed_ts: dict = {}

        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        for pair in self.pairs:
            self.data[pair] = {}
            for tf in NFI_TIMEFRAMES:
                self.data[pair][tf] = pd.DataFrame(columns=cols)
            for tf in BTC_TIMEFRAMES:
                self.data[pair][f"BTC_{tf}"] = pd.DataFrame(columns=cols)
            self.entry_conditions[pair] = {"price_at_lower_bb": False, "price_above_200_ema": False}
            self.exit_conditions[pair] = {"price_at_upper_bb": False, "price_below_200_ema_1h": False}
        self.timeframes = {"entry": self.base_tf, "exit_trend": "1h"}

    def _data_symbol(self, symbol: str) -> str:
        return symbol.split(":")[0]

    async def backfill(self, symbol: str, timeframe: str):
        try:
            src = self.data_exchange if self.data_exchange else self.exchange
            data_sym = self._data_symbol(symbol) if self.data_exchange else symbol
            candles = await asyncio.to_thread(src.fetch_ohlcv, data_sym, timeframe, limit=1000)
            if candles:
                rows = [{"timestamp": pd.to_datetime(c[0], unit='ms'),
                         "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": float(c[5])} for c in candles]
                max_candles = self.config.get("backtest", {}).get("max_candles", 800)
                df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).tail(max_candles)
                self.data[symbol][timeframe] = df
                self._calc_nfi_indicators(symbol, timeframe)
                print(f"Backfilled {len(df)} {timeframe} candles for {symbol}")
        except Exception as e:
            print(f"Backfill error ({symbol}/{timeframe}): {e}")
            await push_activity(f"Backfill error ({symbol}/{timeframe}): {e}", "error")

    async def backfill_btc(self, pair: str, timeframe: str):
        btc_key = f"BTC_{timeframe}"
        try:
            src = self.data_exchange if self.data_exchange else self.exchange
            data_sym = "BTC/USDT"
            candles = await asyncio.to_thread(src.fetch_ohlcv, data_sym, timeframe, limit=1000)
            if candles:
                rows = [{"timestamp": pd.to_datetime(c[0], unit='ms'),
                         "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": float(c[5])} for c in candles]
                max_candles = self.config.get("backtest", {}).get("max_candles", 800)
                df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).tail(max_candles)
                for p in self.pairs:
                    self.data[p][btc_key] = df.copy()
                    self._calc_btc_indicators(p, timeframe)
                print(f"Backfilled {len(df)} BTC {timeframe} candles")
        except Exception as e:
            print(f"Backfill BTC error ({timeframe}): {e}")

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        key = f"{symbol}:{timeframe}"
        data_sym = self._data_symbol(symbol) if self.data_exchange else symbol
        while True:
            try:
                src = self.data_exchange if self.data_exchange else self.exchange
                candles = await asyncio.to_thread(src.fetch_ohlcv, data_sym, timeframe, limit=6)
                if not candles or len(candles) < 2:
                    await asyncio.sleep(60); continue
                completed = candles[-2]
                ts = completed[0]
                prev_ts = self._last_closed_ts.get(key)
                if prev_ts == ts:
                    await asyncio.sleep(60); continue
                rows = [{"timestamp": pd.to_datetime(c[0], unit='ms'),
                         "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": float(c[5])} for c in candles[:-1]]
                df = self.data[symbol][timeframe]
                max_candles = self.config.get("backtest", {}).get("max_candles", 800)
                df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True).drop_duplicates(subset=["timestamp"]).tail(max_candles)
                self.data[symbol][timeframe] = df
                self._calc_nfi_indicators(symbol, timeframe)
                self._last_closed_ts[key] = ts
                if timeframe == self.base_tf:
                    self.get_ec(symbol)
            except Exception as e:
                print(f"watch_ohlcv error ({symbol}/{timeframe}): {e}")
                await push_activity(f"watch_ohlcv error ({symbol}/{timeframe}): {e}", "error")
            await asyncio.sleep(60)

    async def watch_btc_ohlcv(self, pair: str, timeframe: str):
        btc_key = f"BTC_{timeframe}"
        key = f"BTC:{timeframe}"
        while True:
            try:
                src = self.data_exchange if self.data_exchange else self.exchange
                candles = await asyncio.to_thread(src.fetch_ohlcv, "BTC/USDT", timeframe, limit=6)
                if not candles or len(candles) < 2:
                    await asyncio.sleep(60); continue
                completed = candles[-2]
                ts = completed[0]
                prev_ts = self._last_closed_ts.get(key)
                if prev_ts == ts:
                    await asyncio.sleep(60); continue
                rows = [{"timestamp": pd.to_datetime(c[0], unit='ms'),
                         "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": float(c[5])} for c in candles[:-1]]
                max_candles = self.config.get("backtest", {}).get("max_candles", 800)
                df_btc = pd.DataFrame(rows)
                for p in self.pairs:
                    df = self.data[p][btc_key]
                    df = pd.concat([df, df_btc], ignore_index=True).drop_duplicates(subset=["timestamp"]).tail(max_candles)
                    self.data[p][btc_key] = df
                    self._calc_btc_indicators(p, timeframe)
                self._last_closed_ts[key] = ts
            except Exception as e:
                print(f"watch_btc error ({timeframe}): {e}")
            await asyncio.sleep(60)

    def _calc_nfi_indicators(self, symbol: str, timeframe: str):
        df = self.data[symbol][timeframe].copy()
        if len(df) < 20:
            self.data[symbol][timeframe] = df
            return

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]
        typical = (high + low + close) / 3

        # EMAs
        for p in [9, 12, 16, 20, 26, 50, 100, 200]:
            if len(df) >= p:
                df[f"ema_{p}"] = close.ewm(span=p, adjust=False).mean()

        # SMAs
        for p in [9, 16, 21, 30, 200]:
            if len(df) >= p:
                df[f"sma_{p}"] = close.rolling(p).mean()

        # RSI - manual implementation (much faster than pandas_ta)
        def _rsi(series, period):
            delta = series.diff()
            gain = delta.clip(lower=0)
            loss = (-delta).clip(lower=0)
            avg_gain = gain.ewm(span=period, adjust=False).mean()
            avg_loss = loss.ewm(span=period, adjust=False).mean()
            rs = avg_gain / (avg_loss + 1e-10)
            return 100 - (100 / (1 + rs))

        for p in [3, 4, 14, 20]:
            if len(df) >= p:
                rsi_series = _rsi(close, p)
                df[f"rsi_{p}"] = rsi_series

        # BBANDS - manual (much faster than pandas_ta on ARM)
        for period, std in [(20, 2.0), (40, 2.0)]:
            if len(df) >= period:
                sma = close.rolling(period).mean()
                std_val = close.rolling(period).std(ddof=0)
                df[f"bb_lower_{period}_{std}"] = sma - std_val * std
                df[f"bb_middle_{period}_{std}"] = sma
                df[f"bb_upper_{period}_{std}"] = sma + std_val * std
                df[f"bb_width_{period}"] = (sma + std_val * std - (sma - std_val * std)) / (sma + 1e-10)

        # AROON
        if len(df) >= 14:
            aroon = ta.aroon(high, low, length=14)
            df["aroonu_14"] = aroon.iloc[:, 0]
            df["aroond_14"] = aroon.iloc[:, 1] if aroon.shape[1] > 1 else 0

        # STOCHRSI
        if len(df) >= 14:
            sr = ta.stochrsi(close, length=14, rsi_length=14, k=3, d=3)
            if sr is not None:
                df["stochrsi_k"] = sr.iloc[:, 0] if sr.shape[1] > 0 else 50
                df["stochrsi_d"] = sr.iloc[:, 1] if sr.shape[1] > 1 else 50

        # MACD
        if len(df) >= 26:
            macd = ta.macd(close, fast=12, slow=26, signal=9)
            df["macd"] = macd.iloc[:, 0]
            df["macdsignal"] = macd.iloc[:, 1]
            df["macdhist"] = macd.iloc[:, 2]
            df["macdhist_prev"] = macd.iloc[:, 2].shift(1)

        # ADX
        if len(df) >= 15:
            adx_df = ta.adx(high, low, close, length=14)
            df["adx"] = adx_df.iloc[:, 0]
            df["plus_di"] = adx_df.iloc[:, 1] if adx_df.shape[1] > 1 else 0
            df["minus_di"] = adx_df.iloc[:, 2] if adx_df.shape[1] > 2 else 0

        # WILLR
        if len(df) >= 14:
            df["willr_14"] = ta.willr(high, low, close, length=14)

        # CMF
        if len(df) >= 20:
            df["cmf_20"] = ta.cmf(high, low, close, volume, length=20)

        # MFI
        if len(df) >= 14:
            df["mfi_14"] = ta.mfi(high, low, close, volume, length=14)

        # KST - manual implementation (faster)
        if len(df) >= 30:
            roc1 = close.diff(10) / close.shift(10) * 100
            roc2 = close.diff(15) / close.shift(15) * 100
            roc3 = close.diff(20) / close.shift(20) * 100
            roc4 = close.diff(30) / close.shift(30) * 100
            kst_val = roc1.rolling(10).mean() + roc2.rolling(10).mean() * 2 + roc3.rolling(10).mean() * 3 + roc4.rolling(15).mean() * 4
            df["kst"] = kst_val

        # UO (Ultimate Oscillator)
        if len(df) >= 28:
            df["uo_7_14_28"] = ta.uo(high, low, close, length1=7, length2=14, length3=28)

        # CCI
        if len(df) >= 20:
            df["cci_20"] = ta.cci(high, low, close, length=20)

        # OBV
        if len(df) >= 20:
            df["obv"] = ta.obv(close, volume)
            df["obv_ema_20"] = ta.ema(df["obv"], length=20) if len(df) >= 20 else 0

        # Volume ratio (RVol equivalent)
        if len(df) >= 21:
            avg_vol = volume.rolling(20).mean()
            df["volume_ratio"] = volume / (avg_vol + 1e-10)
        else:
            df["volume_ratio"] = 1.0

        # ROC
        for p in [2, 9]:
            if len(df) >= p:
                df[f"roc_{p}"] = ta.roc(close, length=p)

        # ATR
        if len(df) >= 14:
            df["atr"] = ta.atr(high, low, close, length=14)
            df["atr_pct"] = df["atr"] / (close + 1e-10)

        # Rolling highs/lows
        for p in [6, 12, 48]:
            if len(df) >= p:
                df[f"high_max_{p}"] = high.rolling(p).max()
                df[f"low_min_{p}"] = low.rolling(p).min()

        # Percentage change
        if len(df) >= 2:
            df["candle_change_pct"] = close.pct_change() * 100

        # Stoch
        if len(df) >= 14:
            stoch = ta.stoch(high, low, close, k=14, d=3)
            if stoch is not None:
                df["stoch_k"] = stoch.iloc[:, 0] if stoch.shape[1] > 0 else 50
                df["stoch_d"] = stoch.iloc[:, 1] if stoch.shape[1] > 1 else 50

        self.data[symbol][timeframe] = df

    def _calc_btc_indicators(self, symbol: str, timeframe: str):
        btc_key = f"BTC_{timeframe}"
        df = self.data[symbol][btc_key].copy()
        if len(df) < 20:
            self.data[symbol][btc_key] = df
            return

        close = df["close"]
        df["btc_rsi_14"] = ta.rsi(close, length=14) if len(df) >= 14 else 50
        df["btc_ema_20"] = ta.ema(close, length=20) if len(df) >= 20 else 0
        if len(df) >= 200:
            df["btc_ema_200"] = ta.ema(close, length=200)
        if len(df) >= 3:
            df["btc_roc_3"] = ta.roc(close, length=3)
        df["btc_is_bull"] = 0
        if "btc_ema_200" in df.columns and "btc_ema_20" in df.columns and len(df) > 0:
            last = df.iloc[-1]
            close_val = last.get("close", 0)
            ema20 = last.get("btc_ema_20", 0)
            ema200 = last.get("btc_ema_200", 0)
            df["btc_is_bull"] = 1 if (close_val > ema200 and ema20 > ema200) else 0
        self.data[symbol][btc_key] = df

    def get_ec(self, symbol: str) -> dict:
        """Get entry conditions for a symbol, computing NFI-style signals on-the-fly."""
        ec = self.entry_conditions.get(symbol, {})

        # Skip if base timeframe data isn't ready
        df_base = self.data.get(symbol, {}).get(self.base_tf)
        if df_base is None or len(df_base) < 20:
            return ec

        last = df_base.iloc[-1]
        prev = df_base.iloc[-2] if len(df_base) >= 2 else last

        # Helper to read indicator from any timeframe
        def tf_val(tf, col):
            d = self.data.get(symbol, {}).get(tf)
            if d is not None and col in d.columns and len(d) > 0:
                return d.iloc[-1].get(col)
            return None

        def tf_prev(tf, col):
            d = self.data.get(symbol, {}).get(tf)
            if d is not None and col in d.columns and len(d) >= 2:
                return d.iloc[-2].get(col)
            return None

        # BTC helpers
        def btc_val(tf, col):
            d = self.data.get(symbol, {}).get(f"BTC_{tf}")
            if d is not None and col in d.columns and len(d) > 0:
                return d.iloc[-1].get(col)
            return None

        # === MARKET REGIME ===
        adx_val = last.get("adx", 0) or 0
        close_val = last.get("close", 0) or 0
        ema50 = last.get("ema_50", 0) or 0
        ema200 = last.get("ema_200", 0) or 0
        btc_bull = btc_val("1h", "btc_is_bull") or 1
        btc_rsi = btc_val("1h", "btc_rsi_14") or 50

        is_bull = 1 if (close_val > ema200 and ema50 > ema200) else 0
        is_bear = 1 if (close_val < ema200 and ema50 < ema200) else 0

        ec["adx"] = adx_val
        ec["rsi"] = last.get("rsi_14", 50)
        ec["close"] = close_val
        ec["atr"] = float(last.get("atr", 0) or 0)
        ec["atr_pct"] = float(last.get("atr_pct", 0) or 0)
        ec["ema_20"] = float(last.get("ema_20", 0) or 0)
        ec["ema_50"] = float(last.get("ema_50", 0) or 0)
        ec["ema_200"] = float(last.get("ema_200", 0) or 0)
        ec["bb_lower"] = float(last.get("bb_lower_20_2.0", 0) or 0)
        ec["bb_upper"] = float(last.get("bb_upper_20_2.0", 0) or 0)
        ec["close_prev"] = float(prev.get("close", 0) or 0)
        ec["last_price"] = close_val
        ec["adx_slope"] = float(prev.get("adx", 0) or 0) - adx_val
        ec["rvol"] = round(float(last.get("volume_ratio", 1) or 1), 2)
        near_ema20 = ec["ema_20"] > 0 and close_val > ec["ema_20"] * 0.98 and close_val < ec["ema_20"] * 1.02
        ec["trend_pullback_price"] = ec["ema_20"] if near_ema20 else 0
        ec["trend_breakout"] = bool(
            adx_val > 25 and (last.get("rsi_14", 50) or 50) > 50
            and ec["bb_upper"] > 0 and close_val > ec["bb_upper"]
            and ec["trend_uptrend"]
        )
        ec["price_above_50_ema"] = bool(close_val > ec["ema_50"]) if ec["ema_50"] > 0 else False
        ec["regime"] = "trending" if adx_val > 25 else ("high_vol" if last.get("atr_pct", 0) > 0.05 else "sideways")
        ec["trend_uptrend"] = bool(is_bull)
        ec["is_bull"] = is_bull
        ec["is_bear"] = is_bear
        ec["btc_bull"] = btc_bull
        ec["btc_rsi"] = btc_rsi

        # === NFI SHORT SIGNALS (strict, high quality) ===
        ec["short_signal_501"] = self._nfi_short_501(symbol, last, prev, tf_val, tf_prev, btc_val, btc_rsi, is_bear)
        ec["short_signal_502"] = self._nfi_short_502(symbol, last, prev, tf_val, tf_prev, btc_val, btc_rsi, is_bear)

        # === LEGACY SHORT SIGNAL (permissive fallback, same logic as before NFI) ===
        rsi_val = last.get("rsi_14", 50) or 50
        trend_cfg = self.config.get("strategy", {}).get("trend", {})
        short_rsi_th = float(trend_cfg.get("short_rsi_threshold", 40))
        if adx_val > 35:
            rsi_gate = 20
        elif adx_val > 25:
            rsi_gate = short_rsi_th
        else:
            rsi_gate = short_rsi_th
        short_signal_adx = float(trend_cfg.get("short_signal_adx", 20))
        ec["short_signal"] = (
            adx_val > short_signal_adx and rsi_val > rsi_gate
            and not ec.get("trend_uptrend", False)
        )

        # === NFI-STYLE LONG SIGNALS ===
        # === ALL NFI LONG ENTRY CONDITIONS ===
        long_conds = self._nfi_long_conditions(symbol, last, prev, tf_val, tf_prev, btc_val, btc_rsi, is_bull)
        ec.update(long_conds)

        # Store full last row for executor
        ec["last"] = {k: v for k, v in last.items() if not pd.isna(v)} if hasattr(last, 'items') else {}
        ec["price_above_200_ema"] = close_val > ema200 if ema200 > 0 else False

        self.entry_conditions[symbol] = ec
        return ec

    # === EXPANDED UP-MOVE FILTER (NFI X7: 80+ OR'd conditions across 5 timeframes) ===
    def _up_move_pass(self, symbol: str, last, tf_val) -> bool:
        """At least ONE of ~80 conditions must pass (matching NFI X7 #501 up-move filters)."""
        rsi3 = last.get("rsi_3", 50) or 50
        rsi14 = last.get("rsi_14", 50) or 50
        stoch_k = last.get("stochrsi_k", 50) or 50
        aroonu = last.get("aroonu_14", 50) or 50
        aroond = last.get("aroond_14", 50) or 50
        macdh = last.get("macdhist", 0) or 0
        plus_di = last.get("plus_di", 0) or 0
        minus_di = last.get("minus_di", 0) or 0
        willr = last.get("willr_14", -50) or -50
        cmf = last.get("cmf_20", 0) or 0
        mfi = last.get("mfi_14", 50) or 50
        close_val = last.get("close", 0) or 0
        bb_lower = last.get("bb_lower_20_2.0", 0) or 0
        bb_upper = last.get("bb_upper_20_2.0", 0) or 0
        ema20 = last.get("ema_20", 0) or 0
        ema50 = last.get("ema_50", 0) or 0

        def g(tf, col): return tf_val(tf, col) or (50 if "rsi" in col else 0)

        conds = [
            (rsi3 > 3), (rsi3 < 97), (rsi14 > 3),
            (stoch_k < 99), (stoch_d < 99), (aroonu < 100), (aroond < 100),
            (macdh > -1), (plus_di > 0), (minus_di > 0), (willr > -100), (willr < 100),
            (cmf > -1), (cmf < 1), (mfi > 0), (mfi < 100),
            (bb_lower < bb_upper), (ema20 > 0), (ema50 > 0),
            (plus_di > minus_di), (plus_di > 20),
            (rsi14 > 20), (rsi14 < 80),
            (stoch_k > 20), (stoch_k < 80), (stoch_k > (last.get("stochrsi_d", 50) or 50)),
            (aroonu > 25), (aroonu < 75), (aroond < 25),
            (macdh > -0.5), (macdh < 0.5),
            (cmf > -0.1), (cmf < 0.1), (mfi > 30), (mfi < 70),
            (willr > -80), (willr < -20),
            (close_val > ema20 * 0.95) if ema20 > 0 else True,
            (close_val < ema20 * 1.05) if ema20 > 0 else True,
            (close_val > ema50 * 0.95) if ema50 > 0 else True,
            (tf_val("15m", "rsi_3") or 50) > 5,
            (tf_val("15m", "rsi_3") or 50) < 97,
            (tf_val("15m", "aroonu_14") or 50) > 25,
            (tf_val("15m", "aroond_14") or 50) < 25,
            (tf_val("15m", "stochrsi_k") or 50) > 20,
            (tf_val("15m", "stochrsi_k") or 50) < 80,
            (tf_val("15m", "cmf_20") or 0) > -0.1,
            (tf_val("15m", "cmf_20") or 0) < 0.1,
            (tf_val("1h", "rsi_3") or 50) > 5,
            (tf_val("1h", "rsi_3") or 50) < 95,
            (tf_val("1h", "rsi_14") or 50) > 10,
            (tf_val("1h", "rsi_14") or 50) < 90,
            (tf_val("1h", "aroonu_14") or 50) > 30,
            (tf_val("1h", "aroond_14") or 50) < 30,
            (tf_val("1h", "stochrsi_k") or 50) > 20,
            (tf_val("1h", "stochrsi_k") or 50) < 80,
            (tf_val("1h", "cmf_20") or 0) > -0.2,
            (tf_val("1h", "cmf_20") or 0) < 0.2,
            (tf_val("4h", "rsi_3") or 50) > 10,
            (tf_val("4h", "rsi_3") or 50) < 90,
            (tf_val("4h", "rsi_14") or 50) > 15,
            (tf_val("4h", "rsi_14") or 50) < 85,
            (tf_val("4h", "aroonu_14") or 50) > 35,
            (tf_val("4h", "aroond_14") or 50) < 35,
            (tf_val("4h", "stochrsi_k") or 50) > 25,
            (tf_val("4h", "stochrsi_k") or 50) < 75,
            (tf_val("1d", "rsi_3") or 50) > 15,
            (tf_val("1d", "rsi_3") or 50) < 85,
            (tf_val("1d", "rsi_14") or 50) > 20,
            (tf_val("1d", "rsi_14") or 50) < 80,
            ((tf_val("15m", "rsi_3") or 50) > 5 and (tf_val("1h", "rsi_3") or 50) > 5),
            ((tf_val("4h", "aroonu_14") or 50) > 60 and (tf_val("4h", "stochrsi_k") or 50) > 50),
            ((tf_val("1h", "cmf_20") or 0) > 0 and (tf_val("4h", "cmf_20") or 0) > 0),
            (plus_di > minus_di and (tf_val("1h", "plus_di") or 0) > (tf_val("1h", "minus_di") or 0)),
            (stoch_k > 50 and (tf_val("1h", "stochrsi_k") or 50) > 50),
            (aroonu > 50 and (tf_val("4h", "aroonu_14") or 50) > 50),
            (rsi14 > 50 and (tf_val("1h", "rsi_14") or 50) > 50),
            (cmf > 0 and (tf_val("4h", "cmf_20") or 0) > 0),
            (willr > -50 and mfi > 50),
        ]
        return any(conds)

    # === EXPANDED DOWN-MOVE FILTER (for long entries) ===
    def _down_move_pass(self, symbol: str, last, tf_val) -> bool:
        """At least ONE of ~60 conditions must pass."""
        rsi3 = last.get("rsi_3", 50) or 50
        rsi14 = last.get("rsi_14", 50) or 50
        stoch_k = last.get("stochrsi_k", 50) or 50
        aroonu = last.get("aroonu_14", 50) or 50
        aroond = last.get("aroond_14", 50) or 50
        macdh = last.get("macdhist", 0) or 0
        plus_di = last.get("plus_di", 0) or 0
        minus_di = last.get("minus_di", 0) or 0
        willr = last.get("willr_14", -50) or -50
        cmf = last.get("cmf_20", 0) or 0
        mfi = last.get("mfi_14", 50) or 50

        def g(tf, col): return tf_val(tf, col) or (50 if "rsi" in col else 0)
        conds = [
            (rsi3 > 3), (rsi3 < 97), (rsi14 > 3), (rsi14 < 97),
            (stoch_k > 1), (stoch_k < 99), (aroonu < 100), (aroond < 100),
            (macdh > -1), (plus_di > 0), (minus_di > 0), (willr > -100),
            (cmf > -1), (mfi > 0),
            (rsi14 > 20), (rsi14 < 80), (stoch_k > 10), (stoch_k < 90),
            (aroonu > 15), (aroonu < 85), (aroond < 75),
            (macdh > -0.5), (macdh < 0.5), (cmf > -0.1), (cmf < 0.1),
            (mfi > 25), (mfi < 75), (willr > -90), (willr < -10),
            (g("15m", "rsi_3") > 3), (g("15m", "rsi_3") < 97),
            (g("15m", "aroonu_14") < 90), (g("1h", "rsi_3") > 5),
            (g("1h", "rsi_3") < 95), (g("1h", "rsi_14") > 10),
            (g("1h", "rsi_14") < 90), (g("4h", "rsi_14") < 60),
            (g("4h", "aroonu_14") < 100), (g("4h", "stochrsi_k") < 90),
            ((g("1h", "rsi_3") or 50) > 3 and (g("4h", "rsi_14") or 50) < 60),
            ((g("15m", "aroonu_14") or 50) < 80 and (g("1h", "cmf_20") or 0) < 0.1),
        ]
        return any(conds)

    # === SHORT CONDITION #501 ===
    def _nfi_short_501(self, symbol, last, prev, tf_val, tf_prev, btc_val, btc_rsi, is_bear) -> bool:
        try:
            ema12 = last.get("ema_12", 0) or 0
            ema26 = last.get("ema_26", 0) or 0
            ema12_p = prev.get("ema_12", 0) or 0
            ema26_p = prev.get("ema_26", 0) or 0
            bbu = last.get("bb_upper_20_2.0", 0) or 0
            close_val = last.get("close", 0) or 0
            rsi3_1h = tf_val("1h", "rsi_3") or 50
            rsi3_4h = tf_val("4h", "rsi_3") or 50
            rsi3_1d = tf_val("1d", "rsi_3") or 50
            rsi14_1h = tf_val("1h", "rsi_14") or 50
            rsi14_4h = tf_val("4h", "rsi_14") or 50
            if ema12 <= 0 or ema26 <= 0: return False
            up_pass = self._up_move_pass(symbol, last, tf_val)
            if not up_pass: return False
            return all([
                rsi3_1h >= 5.0, rsi3_4h >= 20.0, rsi3_1d >= 20.0,
                rsi14_1h > 20.0, rsi14_4h > 20.0,
                ema12 > ema26,
                (ema12 - ema26) > (close_val * 0.030),
                (ema12_p - ema26_p) > (close_val / 100.0),
                close_val > (bbu * 1.004) if bbu > 0 else True,
                btc_rsi > 35,
            ])
        except Exception: return False

    # === SHORT CONDITION #502 ===
    def _nfi_short_502(self, symbol, last, prev, tf_val, tf_prev, btc_val, btc_rsi, is_bear) -> bool:
        try:
            aroond = last.get("aroond_14", 0) or 0
            stoch_k = last.get("stochrsi_k", 50) or 50
            close_val = last.get("close", 0) or 0
            ema20 = last.get("ema_20", 0) or 0
            bbu = last.get("bb_upper_20_2.0", 0) or 0
            aroond_15m = tf_val("15m", "aroond_14") or 0
            up_pass = self._up_move_pass(symbol, last, tf_val)
            return all([up_pass, aroond < 25, stoch_k > 80,
                close_val > (ema20 * 1.060) if ema20 > 0 else True,
                close_val > (bbu * 0.995) if bbu > 0 else True,
                aroond_15m < 25, btc_rsi > 35])
        except Exception: return False

    # === ALL LONG ENTRY CONDITIONS (NFI X7: 57+ conditions across 6 modes) ===
    def _nfi_long_conditions(self, symbol: str, last, prev, tf_val, tf_prev,
                             btc_val, btc_rsi, is_bull) -> dict:
        """Generate all NFI X7 long entry conditions."""
        ema_bull = is_bull == 1
        close_val = last.get("close", 0) or 0
        ema20 = last.get("ema_20", 0) or 0
        ema50 = last.get("ema_50", 0) or 0
        ema200 = last.get("ema_200", 0) or 0
        bb_lower = last.get("bb_lower_20_2.0", 0) or 0
        bb_upper = last.get("bb_upper_20_2.0", 0) or 0
        rsi14 = last.get("rsi_14", 50) or 50
        aroond = last.get("aroond_14", 50) or 50
        stoch_k = last.get("stochrsi_k", 50) or 50
        low = last.get("low", 0) or 0
        open_val = last.get("open", 0) or 0
        obv = last.get("obv", 0) or 0
        obv_ema = last.get("obv_ema_20", 0) or 0
        adx = last.get("adx", 0) or 0
        vol_ratio = last.get("volume_ratio", 0) or 0
        volume = last.get("volume", 0) or 0
        macdh = last.get("macdhist", 0) or 0
        macdh_p = prev.get("macdhist", 0) or 0
        plus_di = last.get("plus_di", 0) or 0
        minus_di = last.get("minus_di", 0) or 0
        cmf = last.get("cmf_20", 0) or 0
        low_min_6 = last.get("low_min_6", 0) or 0
        willr = last.get("willr_14", -50) or -50
        mfi_val = last.get("mfi_14", 50) or 50

        down_pass = self._down_move_pass(symbol, last, tf_val)
        results = {}

        # --- NORMAL MODE (tags 1-13) ---
        results["long_1"] = down_pass and all([ema_bull, ema50 > 0, low <= ema50 * 1.02,
            close_val > ema50, close_val > open_val, rsi14 > 30, rsi14 < 65, adx > 25,
            vol_ratio > 0.7, plus_di > minus_di, obv > obv_ema, volume > 0, btc_rsi > 35])
        results["long_2"] = down_pass and all([aroond < 25, stoch_k > 80,
            close_val > (ema20 * 1.06) if ema20 > 0 else True,
            close_val > (bb_upper * 0.995) if bb_upper > 0 else True,
            (tf_val("15m", "aroond_14") or 50) < 25, btc_rsi > 35])
        results["long_3"] = down_pass and all([ema_bull, ema50 > 0,
            low <= ema50 * 1.01, close_val > ema50, close_val > open_val,
            rsi14 > 40, rsi14 < 65, btc_rsi > 35])
        results["long_4"] = down_pass and all([macdh > 0, macdh_p <= 0,
            close_val > ema50 if ema50 > 0 else True, rsi14 > 40, rsi14 < 60,
            adx > 15, vol_ratio > 0.8, volume > 0, btc_rsi > 35])
        results["long_5"] = down_pass and all([bb_lower > 0,
            close_val <= bb_lower * 1.005, close_val > open_val,
            rsi14 < 45, adx > 18, vol_ratio > 0.7, volume > 0, btc_rsi > 35])
        ema9 = last.get("ema_9", 0) or 0
        ema16 = last.get("ema_16", 0) or 0
        ema9_p = prev.get("ema_9", 0) or 0
        ema16_p = prev.get("ema_16", 0) or 0
        results["long_6"] = all([ema9 > ema16, ema9_p <= ema16_p,
            close_val > ema200 if ema200 > 0 else True,
            rsi14 > 40, rsi14 < 75, vol_ratio > 0.5, volume > 0, btc_rsi > 35])
        rsi14_prev = prev.get("rsi_14", 50) or 50
        results["long_7"] = all([bb_lower > 0, close_val > bb_lower,
            rsi14_prev < 35, rsi14 > 35, close_val > open_val,
            close_val > ema200 if ema200 > 0 else True,
            adx > 18, vol_ratio > 0.8, volume > 0, btc_rsi > 35])
        results["long_8"] = all([cmf > 0, rsi14 > 40, rsi14 < 60,
            vol_ratio > 0.7, volume > 0, btc_rsi > 35, close_val > ema50 if ema50 > 0 else True])
        results["long_9"] = all([willr < -70, mfi_val > 40,
            vol_ratio > 0.5, volume > 0, btc_rsi > 35,
            close_val > ema200 if ema200 > 0 else True])
        results["long_10"] = all([low_min_6 > 0, close_val >= low_min_6 * 1.005,
            close_val > open_val, rsi14 > 35, rsi14 < 55,
            vol_ratio > 0.5, volume > 0, btc_rsi > 35])
        results["long_11"] = all([adx > 20, plus_di > minus_di,
            vol_ratio > 0.7, volume > 0, btc_rsi > 35,
            close_val > ema50 if ema50 > 0 else True])
        results["long_12"] = all([obv > obv_ema if obv_ema > 0 else True,
            close_val > ema20 if ema20 > 0 else True,
            rsi14 > 40, rsi14 < 65, vol_ratio > 0.5, volume > 0, btc_rsi > 35])
        results["long_13"] = all([
            (tf_val("1h", "plus_di") or 0) > (tf_val("1h", "minus_di") or 0),
            (tf_val("4h", "cmf_20") or 0) > 0,
            rsi14 > 40, rsi14 < 60, volume > 0, btc_rsi > 35,
            close_val > ema200 if ema200 > 0 else True])

        # --- PUMP MODE (tags 21-23) ---
        results["long_21"] = all([vol_ratio > 1.5, rsi14 > 45, rsi14 < 75,
            adx > 25, plus_di > minus_di, volume > 0, btc_rsi > 40,
            (tf_val("1h", "plus_di") or 0) > (tf_val("1h", "minus_di") or 0)])
        results["long_22"] = all([vol_ratio > 2.0, close_val > open_val,
            rsi14 > 50, adx > 25, volume > 0, btc_rsi > 40,
            close_val > (bb_upper * 0.99) if bb_upper > 0 else True])
        results["long_23"] = all([adx > 25, vol_ratio > 1.5,
            plus_di > minus_di, rsi14 > 50, volume > 0, btc_rsi > 40])

        # --- QUICK MODE (tags 41-53, 13 variants) ---
        variants = [(45,75,20,True),(40,70,25,True),(50,80,20,False),(35,65,15,True),
            (45,65,30,True),(40,75,20,True),(50,70,25,False),(45,80,15,True),
            (35,70,20,True),(40,80,15,False),(45,70,25,True),(50,75,20,True),(40,65,30,True)]
        for i, (rmin, rmax, amin, di) in enumerate(variants, 1):
            q = [rmin <= rsi14 <= rmax, adx > amin, volume > 0, btc_rsi > 35]
            if di: q.append(plus_di > minus_di)
            results[f"long_4{i}"] = all(q)

        # --- RAPID MODE (tags 101-110) ---
        for i, (vm, rm) in enumerate([(0.5,40),(0.7,45),(0.3,35),(0.8,50),(0.5,45),
            (0.6,40),(0.4,35),(0.7,50),(0.5,55),(0.8,45)], 1):
            results[f"long_10{i}"] = all([vol_ratio > vm, rsi14 > rm, volume > 0, btc_rsi > 35,
                close_val > ema200 if ema200 > 0 else True])

        # --- SCALP MODE (tags 161-163) ---
        for i, (vm, rl, rh) in enumerate([(0.3,35,55),(0.5,40,60),(0.4,30,50)], 1):
            results[f"long_16{i}"] = all([vol_ratio > vm, rl <= rsi14 <= rh, volume > 0, btc_rsi > 35])

        # --- TOP COINS MODE (tags 141-145) ---
        for i, (need1h, rsi4h) in enumerate([(True,50),(False,45),(True,55),(False,40),(True,60)], 1):
            r = [rsi14 > 40, adx > 20, volume > 0, btc_rsi > 40]
            if need1h: r.append((tf_val("1h", "plus_di") or 0) > (tf_val("1h", "minus_di") or 0))
            results[f"long_14{i}"] = all(r)

        # --- GRIND MODE (tag 120) & BTC MODE (tag 121) ---
        results["long_120"] = all([close_val > ema200 if ema200 > 0 else True,
            rsi14 > 30, rsi14 < 45, vol_ratio > 0.3, volume > 0, adx > 15, btc_rsi > 35])
        results["long_121"] = all([btc_rsi > 50, rsi14 > 45, rsi14 < 65,
            adx > 20, volume > 0, close_val > ema200 if ema200 > 0 else True])

        # --- BACKWARD COMPAT ---
        results["trend_pullback"] = results.get("long_1", False)
        results["ema50_bounce"] = results.get("long_3", False)
        results["bb_bounce"] = results.get("long_5", False)
        results["rsi_oversold"] = results.get("long_7", False)
        results["ema_cross"] = results.get("long_6", False)
        results["macd_reversal"] = results.get("long_4", False)
        return results# === LEGACY: keep check_conditions for backward compat ===
    def check_conditions(self, symbol: str):
        self._prev_entry_conditions[symbol] = self.entry_conditions.get(symbol, {}).copy()
        self.get_ec(symbol)

    def get_regime(self, symbol: str) -> str:
        return self.entry_conditions.get(symbol, {}).get("regime", "unknown")

    def should_enter_trend(self, symbol: str) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        return bool(ec.get("trend_pullback", False) or ec.get("trend_breakout", False))

    def get_trend_price(self, symbol: str) -> float:
        cond = self.entry_conditions.get(symbol, {})
        if cond.get("regime") != "trending":
            return 0
        if cond.get("trend_breakout"):
            return float(cond.get("last_price", 0) or 0)
        return float(cond.get("trend_pullback_price", 0) or 0)

    def evaluate_thesis_add(self, symbol: str, pos_state: dict) -> bool:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        if regime == "sideways":
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

    def evaluate_countertrend_scalp(self, symbol: str, analyst_signal: str = "NEUTRAL") -> int:
        ec = self.entry_conditions.get(symbol, {})
        score = 50
        rsi = ec.get("rsi", 50) or 50
        close = float(ec.get("close", 0) or 0)
        bb_lower = float(ec.get("bb_lower", 0) or 0) or float(ec.get("bb_lower_20_2.0", 0) or 0)
        adx = float(ec.get("adx", 0) or 0)
        adx_slope = float(ec.get("adx_slope", 0) or 0)
        atr_pct = float(ec.get("atr_pct", 0) or 0)
        if rsi < 25: score += 30
        elif rsi < 35: score += 20
        if close > 0 and bb_lower > 0 and close <= bb_lower: score += 20
        if adx > 25 and adx_slope < 0: score += 15
        if atr_pct > 0.03: score -= 15
        return max(0, min(100, score))

    def get_profile_params(self, symbol: str, is_short: bool = False) -> dict:
        ec = self.entry_conditions.get(symbol, {})
        regime = ec.get("regime", "unknown")
        adx = float(ec.get("adx", 0) or 0)
        above_200 = bool(ec.get("price_above_200_ema", False))
        if is_short:
            if regime == "trending" and adx <= 35 and not above_200:
                return {"tp_atr": 2.5, "sl_atr": 3.0, "thesis_add": True}
            elif regime == "trending" and adx <= 35 and above_200:
                return {"tp_atr": 1.0, "sl_atr": 1.0, "thesis_add": False}
            elif adx > 35:
                return {"tp_atr": 3.0, "sl_atr": 2.5, "thesis_add": False}
            else:
                return {"tp_atr": 2.5, "sl_atr": 2.5, "thesis_add": True}
        else:
            if regime == "sideways" or adx < 20:
                return {"tp_atr": 2.0, "sl_atr": 2.0, "thesis_add": True}
            elif regime == "trending" and adx <= 35 and above_200:
                return {"tp_atr": 2.0, "sl_atr": 1.5, "thesis_add": True}
            elif regime == "trending" and adx <= 35 and not above_200:
                return {"tp_atr": 1.0, "sl_atr": 0.8, "thesis_add": False}
            elif adx > 35:
                return {"tp_atr": 2.5, "sl_atr": 2.0, "thesis_add": False}
        return {"tp_atr": 2.0, "sl_atr": 1.5, "thesis_add": True}

    def get_breakeven_pct(self, default: float = 0.2) -> float:
        active_profile = self.config.get("active_profile", "standard")
        profiles = self.config.get("profiles", {})
        prof = profiles.get(active_profile, {})
        strategy = prof.get("strategy", {})
        return float(strategy.get("breakeven_pct", default))

    def should_exit_trend_inversion(self, symbol: str) -> bool:
        return self.exit_conditions[symbol].get("price_below_200_ema_1h", False)

    def _market_panic(self) -> bool:
        btc_key = "BTC/USDT:USDT" if self._is_futures else "BTC/USDT"
        btc_df = self.data.get(btc_key, {}).get(self.base_tf)
        if btc_df is None or len(btc_df) < 4:
            return False
        lookback = min(12, len(btc_df))
        for i in range(1, lookback):
            row = btc_df.iloc[-i]
            drop = (float(row["open"]) - float(row["close"])) / float(row["open"]) * 100
            if drop > 3.0:
                return True
        if lookback >= 4:
            for i in range(1, lookback - 2):
                period_open = float(btc_df.iloc[-(i+2)]["open"])
                period_close = float(btc_df.iloc[-i]["close"])
                if period_open > period_close:
                    cumulative = (period_open - period_close) / period_open * 100
                    if cumulative > 4.5:
                        return True
        return False
        return self.exit_conditions[symbol].get("price_below_200_ema_1h", False)
        return self.exit_conditions[symbol].get("price_below_200_ema_1h", False)

    async def run(self):
        # Backfill sequentially with delays to prevent OOM on 1.8GB server
        for pair in self.pairs:
            for tf in [self.base_tf, "1h", "4h"]:
                await self.backfill(pair, tf)
                await asyncio.sleep(1)
            for tf in BTC_TIMEFRAMES:
                await self.backfill_btc(pair, tf)
                await asyncio.sleep(1)
        # Start watch tasks
        tasks = []
        for pair in self.pairs:
            tasks.append(self.watch_ohlcv(pair, self.base_tf))
            tasks.append(self.watch_ohlcv(pair, "1h"))
            tasks.append(self.watch_btc_ohlcv(pair, "1h"))
        await asyncio.gather(*tasks)
