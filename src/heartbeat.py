import asyncio
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

    async def run(self):
        print("Starting heartbeat monitor")
        while True:
            try:
                await self.exchange.fetch_time()
                self.is_healthy = True
                await asyncio.sleep(self.interval)
            except Exception as e:
                self.is_healthy = False
                print(f"Heartbeat failed: {e}")
                await self.notifier.send_message(f"⚠️ Connection lost: {e}")
                await self.executor.trigger_kill_switch()
                break
