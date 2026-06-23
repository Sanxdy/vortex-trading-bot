import asyncio
from db import TimescaleDB


class AsyncDB:
    """Wraps TimescaleDB — every DB method runs in thread pool, never blocks event loop."""

    def __init__(self, config: dict, exchange: str = "spot"):
        self._db = TimescaleDB(config, exchange)

    @property
    def conn(self):
        return self._db.conn

    @property
    def exchange(self):
        return self._db.exchange

    async def _run(self, method, *args):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: method(*args))

    def connect(self):
        self._db.connect()

    async def log_decision(self, symbol, decision, reason="", regime="",
                           adx=0, atr=0, rsi=0, price=0, balance=0,
                           trend_uptrend=None):
        await self._run(self._db.log_decision, symbol, decision, reason,
                        regime, adx, atr, rsi, price, balance, trend_uptrend)

    async def log_trade(self, trade: dict):
        await self._run(self._db.log_trade, trade)

    async def log_balance_snapshot(self, usdt_balance: float, total_value: float):
        await self._run(self._db.log_balance_snapshot, usdt_balance, total_value)

    async def get_daily_pnl(self) -> float:
        return await self._run(self._db.get_daily_pnl)

    async def get_total_pnl(self) -> float:
        return await self._run(self._db.get_total_pnl)

    async def get_recent_pnls(self, limit: int = 50) -> list:
        return await self._run(self._db.get_recent_pnls, limit)

    async def get_recent_decisions(self, symbol: str, limit: int = 5) -> list:
        return await self._run(self._db.get_recent_decisions, symbol, limit)

    async def get_pair_performance(self, symbol: str, lookback_days: int = 30) -> dict:
        return await self._run(self._db.get_pair_performance, symbol, lookback_days)

    async def get_pair_performance_rankings(self, lookback_days: int = 30) -> list:
        return await self._run(self._db.get_pair_performance_rankings, lookback_days)

    async def get_avg_entry_price(self, symbol: str) -> float:
        return await self._run(self._db.get_avg_entry_price, symbol)

    async def mark_cancelled(self, symbol: str):
        await self._run(self._db.mark_cancelled, symbol)

    async def close(self):
        await self._run(self._db.close)
