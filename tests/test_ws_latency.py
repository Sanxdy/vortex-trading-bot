import asyncio
import time
import sys
sys.path.insert(0, "src")

from exchange_wrapper import ExchangeWrapper

SYMBOL = "SOL/USDT"
NUM_SAMPLES = 10

async def measure_latency():
    config = {
        "exchange": {
            "name": "binance",
            "api_key": "",
            "api_secret": "",
            "testnet": False,
            "rate_limit": {"max_requests": 1200, "interval": 60}
        }
    }
    exchange = ExchangeWrapper(config)
    await exchange.connect()
    print(f"Measuring WebSocket latency for {SYMBOL} ({NUM_SAMPLES} samples)...\n")
    latencies = []
    for i in range(NUM_SAMPLES):
        start = time.perf_counter()
        ticker = await exchange.watch_ticker(SYMBOL)
        elapsed = (time.perf_counter() - start) * 1000
        latencies.append(elapsed)
        print(f"Sample {i+1}: {elapsed:.2f}ms | Last price: {ticker['last']}")
        await asyncio.sleep(0.5)
    avg = sum(latencies) / len(latencies)
    print(f"\nAverage latency: {avg:.2f}ms")
    print(f"Min: {min(latencies):.2f}ms | Max: {max(latencies):.2f}ms")
    if avg < 100:
        print("✅ Latency under 100ms target")
    else:
        print("⚠️ Latency exceeds 100ms target")
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(measure_latency())
