import asyncio
import time
from redis import asyncio as aioredis
from exchange_wrapper import ExchangeWrapper
from notifier import Notifier
from activity import push_activity

class Heartbeat:
    def __init__(self, config: dict, exchange: ExchangeWrapper, notifier: Notifier, executor: 'Executor'):
        self.config = config
        self.exchange = exchange
        self.notifier = notifier
        self.executor = executor
        self.interval = 30
        self.is_healthy = True
        self.redis = None
        self._last_scanner_alert = 0

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
        consecutive_failures = 0
        max_failures = 3
        while True:
            try:
                await asyncio.wait_for(self.exchange.fetch_time(), timeout=10)
                consecutive_failures = 0
                self.is_healthy = True
                await self._connect_redis()
                if self.redis:
                    kill = await self.redis.get("vortex:kill:signal")
                    if kill:
                        await self.redis.delete("vortex:kill:signal")
                        await self.notifier.send_message("❌ Kill signal received from system")
                        await self.executor.trigger_kill_switch()
                        break
                    exit_keys = await self.redis.keys("vortex:exit:signal:*")
                    for key in exit_keys:
                        raw_symbol = key.split("vortex:exit:signal:")[-1]
                        symbol = raw_symbol.replace("_", "/")
                        reason = await self.redis.get(key) or "manual_tp"
                        await self.redis.delete(key)
                        await push_activity(f"Manual exit signal: {symbol} ({reason})", "info")
                        await self.executor.graceful_exit_pair(symbol, reason)
                    # ── Scanner watchdog ──
                    now = time.time()
                    scan_keys = await self.redis.keys("vortex:scan:*")
                    if not scan_keys:
                        if now - self._last_scanner_alert > 300:
                            self._last_scanner_alert = now
                            await self.notifier.send_message(
                                "⚠️ Scanner stopped — no pairs scanned in 2+ minutes")
                    else:
                        latest = 0
                        for k in scan_keys:
                            v = await self.redis.get(k)
                            if v:
                                try:
                                    latest = max(latest, float(v))
                                except (ValueError, TypeError):
                                    pass
                        if now - latest > 120 and now - self._last_scanner_alert > 300:
                            self._last_scanner_alert = now
                            await self.notifier.send_message(
                                f"⚠️ Scanner stalled — {len(scan_keys)} pairs frozen for >2min")
                await asyncio.sleep(self.interval)
            except asyncio.TimeoutError:
                await self._connect_redis()
                bt_running = await self.redis.get("vortex:backtest:running") if self.redis else None
                if bt_running:
                    print(f"Heartbeat timeout — backtest running, skipping")
                    continue
                consecutive_failures += 1
                self.is_healthy = False
                print(f"Heartbeat timeout ({consecutive_failures}/{max_failures})")
            except Exception as e:
                consecutive_failures += 1
                self.is_healthy = False
                print(f"Heartbeat failed ({consecutive_failures}/{max_failures}): {e}")
                if consecutive_failures >= max_failures:
                    await self.notifier.send_message(f"⚠️ Connection lost after {max_failures} consecutive failures")
                    await self.executor.trigger_kill_switch()
                    break
