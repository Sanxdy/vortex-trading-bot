import asyncio
import json
import aioredis
from exchange_wrapper import ExchangeWrapper

class Ingestor:
    def __init__(self, config: dict, exchange: ExchangeWrapper):
        self.config = config
        self.exchange = exchange
        self.redis = None
        self.symbol = config["grid"]["pair"]
        self.redis_key = f"vortex:ticker:{self.symbol.replace('/', '_')}"

    async def connect_redis(self):
        redis_url = f"redis://:{self.config['redis']['password']}@{self.config['redis']['host']}:{self.config['redis']['port']}" if self.config['redis']['password'] else f"redis://{self.config['redis']['host']}:{self.config['redis']['port']}"
        self.redis = await aioredis.from_url(
            redis_url,
            db=self.config["redis"]["db"],
            decode_responses=True
        )

    async def run(self):
        await self.connect_redis()
        print(f"Starting ticker ingestor for {self.symbol}")
        while True:
            try:
                ticker = await self.exchange.watch_ticker(self.symbol)
                ticker_data = {
                    "last": float(ticker["last"]),
                    "bid": float(ticker["bid"]),
                    "ask": float(ticker["ask"]),
                    "timestamp": ticker["timestamp"]
                }
                await self.redis.set(self.redis_key, json.dumps(ticker_data))
                await self.redis.expire(self.redis_key, 60)
            except Exception as e:
                print(f"Ingestor error: {e}")
                await asyncio.sleep(1)

    async def close(self):
        if self.redis:
            await self.redis.close()
