import asyncio
import json
import logging
import os
import yaml
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandas_ta as ta
from redis import asyncio as aioredis

logger = logging.getLogger(__name__)


CONDITION_TYPES = [
    "price_above_ema200_daily",
    "price_above_ema50_daily",
    "atr_pct_2_to_5",
    "atr_pct_above_0.5",
    "spread_below_0.05",
    "spread_below_0.03",
    "spread_below_0.04",
    "volume_above_50m",
    "volume_above_100m",
    "rsi_below_50",
    "adx_above_25",
]


class WatchlistMonitor:
    def __init__(self, exchange, config: dict, executor, notifier=None):
        self.exchange = exchange
        self.config = config
        self.executor = executor
        self.notifier = notifier
        wl_cfg = config.get("watchlist", config.get("_watchlist", {}))
        self.watched: Dict[str, dict] = wl_cfg.get("pairs", {})
        self.check_interval = wl_cfg.get("check_interval_minutes", 60)
        watchlist_path = config.get("_watchlist_path", "config/watchlist.yaml")
        self._file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), watchlist_path
        ) if not os.path.isabs(watchlist_path) else watchlist_path
        self._previous_states: Dict[str, dict] = {}
        self._last_check: Dict[str, Tuple[bool, dict]] = {}
        self._redis = None
        self._shutdown = False
        self.deepseek_api_key = os.getenv("DEEPSEEK_API_KEY", "")

    async def _connect_redis(self):
        try:
            rc = self.config.get("redis", {})
            pw = rc.get("password", "")
            host = rc.get("host", "127.0.0.1")
            port = int(rc.get("port", 6379))
            if pw:
                self._redis = await aioredis.from_url(
                    f"redis://:{pw}@{host}:{port}",
                    db=int(rc.get("db", 0)),
                    decode_responses=True,
                )
            else:
                self._redis = await aioredis.from_url(
                    f"redis://{host}:{port}",
                    db=int(rc.get("db", 0)),
                    decode_responses=True,
                )
            await self._redis.ping()
        except Exception:
            self._redis = None

    async def _fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 201) -> pd.DataFrame:
        try:
            candles = await asyncio.wait_for(
                self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit), timeout=10
            )
            if not candles:
                return pd.DataFrame()
            rows = [
                {
                    "timestamp": pd.to_datetime(c[0], unit="ms"),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }
                for c in candles
            ]
            return pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).tail(limit)
        except Exception as e:
            logger.warning(f"watchlist fetch_ohlcv error ({symbol}/{timeframe}): {e}")
            return pd.DataFrame()

    async def _check_ema(self, symbol: str, period: int) -> bool:
        df = await self._fetch_ohlcv(symbol, "1d", limit=period + 1)
        if len(df) < period:
            return False
        closes = df["close"].values.astype(float)
        ema = ta.ema(pd.Series(closes), length=period)
        if ema is None or ema.iloc[-1] is None or pd.isna(ema.iloc[-1]):
            return False
        current_price = float(closes[-1])
        return current_price > float(ema.iloc[-1])

    async def _check_atr_pct(self, symbol: str, min_pct: float, max_pct: float) -> bool:
        df = await self._fetch_ohlcv(symbol, "5m", limit=15)
        if len(df) < 14:
            return False
        atr_series = ta.atr(df["high"], df["low"], df["close"], length=14)
        if atr_series is None or atr_series.iloc[-1] is None or pd.isna(atr_series.iloc[-1]):
            return False
        atr = float(atr_series.iloc[-1])
        price = float(df["close"].iloc[-1])
        if price <= 0:
            return False
        atr_pct = (atr / price) * 100
        return min_pct <= atr_pct <= max_pct

    async def _check_spread(self, symbol: str, max_spread: float) -> bool:
        try:
            ticker = await asyncio.wait_for(
                self.exchange.fetch_ticker(symbol), timeout=5
            )
            bid = float(ticker.get("bid") or 0)
            ask = float(ticker.get("ask") or 0)
            if ask <= 0 or bid <= 0:
                return False
            spread = (ask - bid) / ask * 100
            return spread <= max_spread
        except Exception as e:
            logger.warning(f"watchlist spread error ({symbol}): {e}")
            return False

    async def _check_volume(self, symbol: str, min_volume_m: float) -> bool:
        try:
            ticker = await asyncio.wait_for(
                self.exchange.fetch_ticker(symbol), timeout=5
            )
            quote_vol = float(ticker.get("quoteVolume", 0) or 0)
            return quote_vol >= min_volume_m * 1_000_000
        except Exception as e:
            logger.warning(f"watchlist volume error ({symbol}): {e}")
            return False

    async def _check_rsi(self, symbol: str, max_rsi: float) -> bool:
        df = await self._fetch_ohlcv(symbol, "1h", limit=15)
        if len(df) < 14:
            return False
        rsi_series = ta.rsi(df["close"], length=14)
        if rsi_series is None or rsi_series.iloc[-1] is None or pd.isna(rsi_series.iloc[-1]):
            return False
        return float(rsi_series.iloc[-1]) < max_rsi

    async def _check_adx(self, symbol: str, min_adx: float) -> bool:
        df = await self._fetch_ohlcv(symbol, "1h", limit=16)
        if len(df) < 15:
            return False
        adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
        if adx_df is None or adx_df.iloc[-1] is None:
            return False
        adx_val = float(adx_df.iloc[-1, 0])
        return adx_val > min_adx

    async def check_conditions(self, symbol: str, conditions: List[dict]) -> Tuple[bool, Dict[str, bool]]:
        details = {}
        for cond in conditions:
            t = cond["type"]
            try:
                if t == "price_above_ema200_daily":
                    details["EMA200"] = await self._check_ema(symbol, 200)
                elif t == "price_above_ema50_daily":
                    details["EMA50"] = await self._check_ema(symbol, 50)
                elif t == "atr_pct_2_to_5":
                    details["ATR"] = await self._check_atr_pct(symbol, 2, 5)
                elif t == "atr_pct_above_0.5":
                    details["ATR"] = await self._check_atr_pct(symbol, 0.5, 100)
                elif t == "spread_below_0.05":
                    details["Spread"] = await self._check_spread(symbol, 0.05)
                elif t == "spread_below_0.03":
                    details["Spread"] = await self._check_spread(symbol, 0.03)
                elif t == "spread_below_0.04":
                    details["Spread"] = await self._check_spread(symbol, 0.04)
                elif t == "volume_above_50m":
                    details["Vol"] = await self._check_volume(symbol, 50)
                elif t == "volume_above_100m":
                    details["Vol"] = await self._check_volume(symbol, 100)
                elif t == "rsi_below_50":
                    details["RSI"] = await self._check_rsi(symbol, 50)
                elif t == "adx_above_25":
                    details["ADX"] = await self._check_adx(symbol, 25)
                else:
                    logger.warning(f"Unknown condition type: {t}")
                    details[t] = False
            except Exception as e:
                logger.error(f"Condition check error {symbol} {t}: {e}")
                details[t] = False
        if not details:
            return False, {}
        all_met = all(details.values())
        return all_met, details

    def _is_pair_active(self, symbol: str) -> bool:
        return symbol in self.executor.states

    def _save_config(self):
        try:
            new_data = {
                "enabled": True,
                "check_interval_minutes": self.check_interval,
                "pairs": self.watched,
            }
            os.makedirs(os.path.dirname(self._file_path), exist_ok=True)
            with open(self._file_path, "w") as f:
                yaml.dump(new_data, f, default_flow_style=False)
            logger.info(f"Watchlist config saved to {self._file_path}")
        except Exception as e:
            logger.error(f"Failed to save watchlist config: {e}")

    async def _publish_status(self):
        if not self._redis:
            return
        try:
            pairs = []
            for sym in self.watched:
                cached = self._last_check.get(sym)
                if cached:
                    met, details = cached
                else:
                    met, details = False, {}
                active = self._is_pair_active(sym)
                if active and met:
                    status, color = "active", "green"
                elif not active and met:
                    status, color = "ready", "gold"
                else:
                    status, color = "watching", "red"
                CONDITION_LABELS = {
                    "price_above_ema200_daily": "EMA200↑",
                    "price_above_ema50_daily": "EMA50↑",
                    "atr_pct_2_to_5": "ATR 2-5%",
                    "atr_pct_above_0.5": "ATR>0.5",
                    "spread_below_0.05": "Sprd<0.05%",
                    "spread_below_0.04": "Sprd<0.04%",
                    "rsi_below_50": "RSI<50",
                    "volume_above_50m": "Vol>50M",
                    "volume_above_100m": "Vol>100M",
                    "adx_above_25": "ADX>25",
                }
                conditions = [
                    {"label": CONDITION_LABELS.get(k, k), "ok": v}
                    for k, v in details.items()
                ]
                pairs.append({
                    "symbol": sym,
                    "status": status,
                    "color": color,
                    "conditions": conditions,
                    "enabled": active,
                })
            order = {"active": 0, "ready": 1, "watching": 2}
            pairs.sort(key=lambda p: order.get(p["status"], 99))
            await self._redis.set(
                "vortex:watchlist:status",
                json.dumps({"pairs": pairs}),
                ex=120,
            )
        except Exception as e:
            logger.error(f"watchlist publish_status: {e}")

    async def _process_commands(self):
        if not self._redis:
            return
        try:
            raw = await self._redis.lpop("vortex:watchlist:cmd")
            while raw:
                try:
                    cmd = json.loads(raw)
                    action = cmd.get("cmd")
                    symbol = cmd.get("symbol", "").upper()
                    if action == "enable":
                        await self.enable_pair(symbol)
                    elif action == "remove":
                        await self.remove_pair(symbol)
                    elif action == "add":
                        await self._add_pair(symbol)
                except Exception as e:
                    logger.error(f"watchlist cmd error: {e}")
                raw = await self._redis.lpop("vortex:watchlist:cmd")
        except Exception as e:
            logger.error(f"watchlist process_commands: {e}")

    async def enable_pair(self, symbol: str):
        if symbol not in self.watched:
            logger.warning(f"Cannot enable {symbol}: not in watchlist")
            return
        if self._is_pair_active(symbol):
            return
        await self.executor.add_pair(symbol)
        logger.info(f"Manual enable: {symbol}")
        if self.notifier:
            await self.notifier.send_message(f"✅ {symbol} enabled from watchlist")

    async def remove_pair(self, symbol: str):
        if symbol not in self.watched:
            logger.warning(f"Cannot remove {symbol}: not in watchlist")
            return
        if self._is_pair_active(symbol):
            await self.executor.remove_pair(symbol)
            logger.info(f"Removed active pair {symbol} from trading")
        # Remove from watchlist
        if symbol in self.watched:
            del self.watched[symbol]
        self._save_config()
        self._previous_states.pop(symbol, None)
        if self.notifier:
            await self.notifier.send_message(f"❌ {symbol} removed from watchlist")

    async def _add_pair(self, symbol: str):
        if not symbol.endswith("/USDT"):
            symbol = f"{symbol}/USDT"
        if symbol in self.watched:
            return
        conditions = await self.suggest_conditions(symbol)
        self.watched[symbol] = {"conditions": conditions}
        self._save_config()
        cond_str = ", ".join(c["type"] for c in conditions)
        logger.info(f"Added {symbol} to watchlist with conditions: {cond_str}")
        if self.notifier:
            msg = f"📋 {symbol} added to watchlist\nConditions: {cond_str}"
            await self.notifier.send_message(msg)

    async def suggest_conditions(self, symbol: str) -> List[dict]:
        if self.deepseek_api_key:
            try:
                res = await self._suggest_deepseek(symbol)
                if res:
                    return res
            except Exception as e:
                logger.warning(f"DeepSeek suggestion failed: {e}")
        return self._suggest_rules(symbol)

    async def _suggest_deepseek(self, symbol: str) -> Optional[List[dict]]:
        import aiohttp
        prompt = (
            f"Given {symbol} on Binance for a 5-minute scalping strategy, "
            f"suggest 2-4 technical indicators from this list that best determine "
            f"if {symbol} is in good condition for scalping: "
            f"{', '.join(CONDITION_TYPES)}. "
            f"Consider volatility, volume, spread, and price behavior. "
            f"Return ONLY a JSON array of objects: [{{\"type\":\"...\",\"label\":\"...\"}}]"
        )
        headers = {
            "Authorization": f"Bearer {self.deepseek_api_key}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.deepseek.com/v1/chat/completions",
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 150,
                },
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    raise Exception(f"DeepSeek API error {resp.status}")
                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                return json.loads(content)

    def _suggest_rules(self, symbol: str) -> List[dict]:
        base = symbol.split("/")[0]
        large_cap = {"BTC", "ETH", "SOL", "BNB"}
        mid_cap = {"ADA", "LINK", "MATIC", "DOT"}
        volatile = {"XRP", "AVAX", "DOGE"}
        if base in large_cap:
            return [
                {"type": "price_above_ema200_daily", "label": "EMA200"},
                {"type": "atr_pct_2_to_5", "label": "ATR 2-5%"},
                {"type": "spread_below_0.05", "label": "Spread <0.05%"},
            ]
        elif base in mid_cap:
            return [
                {"type": "price_above_ema50_daily", "label": "EMA50"},
                {"type": "rsi_below_50", "label": "RSI <50"},
                {"type": "volume_above_50m", "label": "Vol >$50M"},
            ]
        elif base in volatile:
            return [
                {"type": "atr_pct_above_0.5", "label": "ATR >0.5%"},
                {"type": "spread_below_0.04", "label": "Spread <0.04%"},
            ]
        else:
            return [
                {"type": "atr_pct_2_to_5", "label": "ATR 2-5%"},
                {"type": "spread_below_0.05", "label": "Spread <0.05%"},
            ]

    async def _evaluate(self):
        for symbol, settings in list(self.watched.items()):
            try:
                met, details = await self.check_conditions(
                    symbol, settings["conditions"]
                )
                self._last_check[symbol] = (met, details)
                is_active = self._is_pair_active(symbol)
                prev = self._previous_states.get(symbol, {}).get("met")

                # Safety guard: never auto-disable pairs originally enabled in config.yaml
                originally_enabled = symbol in getattr(self.executor, "all_pairs", [])
                if is_active and not met and not originally_enabled:
                    await self.executor.remove_pair(symbol)
                    msg = f"🛑 Auto-disabled {symbol}: conditions failed"
                    if self.notifier:
                        await self.notifier.send_message(msg)
                    logger.info(msg)
                elif not is_active and met and prev is not True:
                    cond_ok = [
                        k for k, v in details.items() if v
                    ]
                    msg = f"✅ Watchlist: {symbol} is Ready ({', '.join(cond_ok)})"
                    if self.notifier:
                        await self.notifier.send_message(msg)
                    logger.info(msg)

                self._previous_states[symbol] = {"met": met, "active": is_active}
            except Exception as e:
                logger.error(f"Watchlist evaluate error {symbol}: {e}")

    async def run(self):
        await self._connect_redis()
        next_eval = asyncio.get_event_loop().time()
        while not self._shutdown:
            try:
                now = asyncio.get_event_loop().time()
                await self._process_commands()
                if now >= next_eval:
                    await self._evaluate()
                    next_eval = now + self.check_interval * 60
                await self._publish_status()
            except Exception as e:
                logger.error(f"Watchlist run error: {e}")
            await asyncio.sleep(5)

    async def shutdown(self):
        self._shutdown = True
