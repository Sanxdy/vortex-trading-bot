import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import pandas as pd

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "backtest_cache"

TIMEFRAMES = ["5m", "15m", "30m", "1h", "2h", "4h", "1d"]

# All 22 active pairs from config
ALL_PAIRS = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "SUI/USDT", "DOGE/USDT",
    "ADA/USDT", "NEAR/USDT", "TON/USDT", "STX/USDT", "FIL/USDT",
    "ENA/USDT", "TAO/USDT", "INJ/USDT", "IMX/USDT", "W/USDT",
    "JUP/USDT", "ARB/USDT", "FET/USDT", "WIF/USDT", "ALGO/USDT",
    "TIA/USDT", "OP/USDT",
]

SINCE = "2015-01-01T00:00:00Z"


class DataCache:
    def __init__(self, rate_limit_delay: float = 0.1):
        self.exchange = ccxt.binance()
        self.delay = rate_limit_delay
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, symbol: str, timeframe: str) -> Path:
        safe = symbol.replace("/", "_")
        pair_dir = CACHE_DIR / safe
        pair_dir.mkdir(parents=True, exist_ok=True)
        return pair_dir / f"{timeframe}.parquet"

    def load(self, symbol: str, timeframe: str) -> pd.DataFrame:
        path = self._cache_path(symbol, timeframe)
        if path.exists():
            df = pd.read_parquet(path)
            return df
        return pd.DataFrame()

    def has_data(self, symbol: str, timeframe: str) -> bool:
        return self._cache_path(symbol, timeframe).exists()

    def cache_status(self) -> dict:
        status = {}
        for pair in ALL_PAIRS:
            status[pair] = {}
            for tf in TIMEFRAMES:
                path = self._cache_path(pair, tf)
                if path.exists():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    df = pd.read_parquet(path)
                    if not df.empty:
                        from_date = datetime.fromtimestamp(df["timestamp"].min() / 1000).date()
                        to_date = datetime.fromtimestamp(df["timestamp"].max() / 1000).date()
                        status[pair][tf] = {
                            "candles": len(df),
                            "size_mb": round(size_mb, 1),
                            "from": str(from_date),
                            "to": str(to_date),
                        }
        return status

    def cache_summary(self) -> str:
        status = self.cache_status()
        lines = []
        total_candles = 0
        total_size = 0
        for pair in ALL_PAIRS:
            for tf in TIMEFRAMES:
                info = status.get(pair, {}).get(tf)
                if info:
                    total_candles += info["candles"]
                    total_size += info["size_mb"]
                    lines.append(f"{pair:12s} {tf:4s} {info['candles']:>8d}c {info['size_mb']:>6.1f}MB {info['from']} → {info['to']}")
        lines.append(f"\nTotal: {total_candles:,} candles, {total_size:.1f} MB")
        return "\n".join(lines)

    async def fetch_all(self, symbol: str, timeframe: str) -> pd.DataFrame:
        path = self._cache_path(symbol, timeframe)
        since_ms = int(datetime(2015, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        all_candles = []
        end_time = None
        page = 0
        max_pages = 5000

        while page < max_pages:
            params = {}
            if end_time is not None:
                params["endTime"] = end_time
            try:
                chunk = await asyncio.to_thread(
                    self.exchange.fetch_ohlcv, symbol, timeframe, limit=1000, params=params
                )
            except Exception as e:
                print(f"  [{symbol} {timeframe}] Error: {e}")
                await asyncio.sleep(5)
                continue

            if not chunk or len(chunk) < 2:
                break

            all_candles = chunk + all_candles
            end_time = all_candles[0][0] - 1
            first_ts = all_candles[0][0]
            first_date = datetime.fromtimestamp(first_ts / 1000).date()
            page += 1

            if page % 10 == 1:
                print(f"  {symbol:10s} {timeframe:4s} page {page:3d} — {first_date} ({len(all_candles):>7,} candles)")

            if first_ts <= since_ms:
                break

            await asyncio.sleep(self.delay)

        if all_candles:
            df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = df[df["timestamp"] >= since_ms].reset_index(drop=True)
            if not df.empty:
                df.to_parquet(path, index=False)
                first_date = datetime.fromtimestamp(df["timestamp"].min() / 1000).date()
                last_date = datetime.fromtimestamp(df["timestamp"].max() / 1000).date()
                print(f"  {symbol:10s} {timeframe:4s} ✅ cached {len(df):,} candles ({first_date} → {last_date})")
                return df

        print(f"  {symbol:10s} {timeframe:4s} ⚠️ no data")
        return pd.DataFrame()

    async def build_all(self, timeframes: list = None, pairs: list = None):
        if timeframes is None:
            timeframes = TIMEFRAMES
        if pairs is None:
            pairs = ALL_PAIRS

        total = len(pairs) * len(timeframes)
        done = 0
        start_time = time.time()

        for pair in pairs:
            for tf in timeframes:
                done += 1
                path = self._cache_path(pair, tf)
                if path.exists():
                    print(f"  {pair:10s} {tf:4s} already cached — skipping")
                    continue
                print(f"\n[{done}/{total}] Fetching {pair} {tf}...")
                await self.fetch_all(pair, tf)

                elapsed = time.time() - start_time
                rate = done / elapsed * 3600 if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  Progress: {done}/{total} — {rate:.0f} pairs-TF/hour — ETA: {eta:.0f} min")

        elapsed = time.time() - start_time
        print(f"\n✅ Cache build complete in {elapsed/60:.1f} minutes")
        print(self.cache_summary())


if __name__ == "__main__":
    import sys

    cache = DataCache(rate_limit_delay=0.05)

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        print(cache.cache_summary())
    elif len(sys.argv) > 1 and sys.argv[1] == "fetch":
        pair = sys.argv[2] if len(sys.argv) > 2 else None
        tf = sys.argv[3] if len(sys.argv) > 3 else None
        pairs = [pair] if pair else None
        tfs = [tf] if tf else None
        asyncio.run(cache.build_all(timeframes=tfs, pairs=pairs))
    else:
        asyncio.run(cache.build_all(
            timeframes=["5m", "15m", "1h", "4h"],
        ))
