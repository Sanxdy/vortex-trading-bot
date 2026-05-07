import asyncio
import time
import sys
import os
from dotenv import load_dotenv
sys.path.insert(0, "src")

from exchange_wrapper import ExchangeWrapper

load_dotenv()

SYMBOL = "SOL/USDT"

async def measure_latency():
    config = {
        "exchange": {
            "name": "binance",
            "api_key": os.getenv("EXCHANGE_API_KEY", ""),
            "api_secret": os.getenv("EXCHANGE_API_SECRET", ""),
            "testnet": True,
            "rate_limit": {"max_requests": 1200, "interval": 60}
        }
    }
    exchange = ExchangeWrapper(config)

    t0 = time.perf_counter()
    await exchange.connect()
    print(f"Connection established in {(time.perf_counter()-t0)*1000:.0f}ms\n")

    t0 = time.perf_counter()
    ticker = await exchange.watch_ticker(SYMBOL)
    elapsed = (time.perf_counter() - t0) * 1000
    now_ms = time.time() * 1000
    prop_latency = now_ms - ticker["timestamp"]
    print(f"First tick: received in {elapsed:.0f}ms | Price: {ticker['last']}")
    print(f"Propagation latency (exchange→local): {prop_latency:.0f}ms\n")

    print("Measuring ticker update frequency (10 samples)...")
    for i in range(10):
        t0 = time.perf_counter()
        ticker = await exchange.watch_ticker(SYMBOL)
        elapsed = (time.perf_counter() - t0) * 1000
        now_ms = time.time() * 1000
        prop = now_ms - ticker["timestamp"]
        print(f"  #{i+1}: wait={elapsed:.0f}ms | propagation={prop:.0f}ms | price={ticker['last']}")

    await exchange.close()
    print("\nTestnet connected successfully. Bot is ready to run.")

if __name__ == "__main__":
    asyncio.run(measure_latency())
