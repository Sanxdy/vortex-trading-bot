import asyncio
from redis import asyncio as aioredis
from exchange_wrapper import ExchangeWrapper
from notifier import Notifier

class Heartbeat:
    def __init__(self, config: dict, exchange: ExchangeWrapper, notifier: Notifier, executor: 'Executor'):
        self.config = config
        self.exchange = exchange
        self.notifier = notifier
        self.executor = executor
        self.interval = 30
        self.is_healthy = True
        self.redis = None

    async def _connect_redis(self):
        if self.redis:
            try:
                await self.redis.ping()
                return
            except Exception:
                pass
        try:
            rc = self.config["redis"]
            url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
            self.redis = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
            await self.redis.ping()
        except Exception:
            pass

    async def run(self):
        print("Starting heartbeat monitor")
        while True:
            try:
                await self.exchange.fetch_time()
                self.is_healthy = True
                await self._connect_redis()
                if self.redis:
                    kill = await self.redis.get("vortex:kill:signal")
                    if kill:
                        await self.redis.delete("vortex:kill:signal")
                        await self.notifier.send_message("❌ Kill signal received from dashboard")
                        await self.executor.trigger_kill_switch()
                        break
                await asyncio.sleep(self.interval)
            except Exception as e:
                self.is_healthy = False
                print(f"Heartbeat failed: {e}")
                await self.notifier.send_message(f"⚠️ Connection lost: {e}")
                await self.executor.trigger_kill_switch()
                break
