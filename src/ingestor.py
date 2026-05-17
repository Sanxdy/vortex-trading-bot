import asyncio
import json
from redis import asyncio as aioredis
from exchange_wrapper import ExchangeWrapper

class Ingestor:
    def __init__(self, config: dict, exchange: ExchangeWrapper):
        self.config = config
        self.exchange = exchange
        self.redis = None
        self.pairs = [p["name"] for p in config["pairs"] if p.get("enabled", True)]

    async def connect_redis(self):
        redis_url = f"redis://:{self.config['redis']['password']}@{self.config['redis']['host']}:{self.config['redis']['port']}" if self.config['redis']['password'] else f"redis://{self.config['redis']['host']}:{self.config['redis']['port']}"
        self.redis = await aioredis.from_url(redis_url, db=self.config["redis"]["db"], decode_responses=True)

    async def watch_one(self, symbol: str):
        key = f"vortex:ticker:{symbol.replace('/', '_')}"
        while True:
            try:
                ticker = await asyncio.wait_for(self.exchange.watch_ticker(symbol), timeout=10)
                last = ticker.get("last") or 0
                bid = ticker.get("bid") or 0
                ask = ticker.get("ask") or 0
                if not last and not bid and not ask:
                    await asyncio.sleep(5)
                    continue
                data = json.dumps({
                    "last": float(last),
                    "bid": float(bid),
                    "ask": float(ask),
                    "timestamp": ticker["timestamp"]
                })
                await self.redis.set(key, data)
                await self.redis.expire(key, 60)
            except Exception as e:
                print(f"Ingestor ({symbol}): {e}")
                await asyncio.sleep(1)

    async def run(self):
        await self.connect_redis()
        print(f"Starting ticker ingestor for {len(self.pairs)} pairs")
        await asyncio.gather(*[self.watch_one(p) for p in self.pairs])

    async def close(self):
        if self.redis:
            await self.redis.close()
