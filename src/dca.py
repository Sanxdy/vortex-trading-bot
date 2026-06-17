"""Dollar Cost Average strategy — buy at fixed intervals, sell at TP."""
import asyncio, json, os, time, yaml
from datetime import datetime, timezone
from typing import Optional
from redis import asyncio as aioredis

class DCA:
    def __init__(self, config: dict, exchange, db, notifier, allocator):
        self.config = config
        self.exchange = exchange
        self.db = db
        self.notifier = notifier
        self.allocator = allocator
        self.dca_config = config.get("dca", {})
        self.enabled = self.dca_config.get("enabled", False)
        self.interval_minutes = self.dca_config.get("interval_minutes", 240)
        self.amount_per_trade = self.dca_config.get("amount_per_trade", 15.0)
        self.tp_percent = self.dca_config.get("tp_percent", 3.0)
        self.pairs = self.dca_config.get("pairs", ["BTC/USDT", "ETH/USDT", "SOL/USDT"])
        self.positions = {}
        self.redis = None
        self.redis_key = "vortex:dca:next_buy"

    async def _connect(self):
        if self.redis:
            return
        try:
            rc = self.config.get("redis", {})
            self.redis = await aioredis.from_url(
                f"redis://{rc.get('host','localhost')}:{rc.get('port',6379)}",
                password=rc.get("password", None), db=rc.get("db", 0),
                decode_responses=True)
        except Exception as e:
            print(f"DCA redis connect error: {e}")

    async def start(self):
        if not self.enabled or not self.pairs:
            return
        await self._connect()
        while True:
            try:
                await self._cycle()
                await asyncio.sleep(60)
            except Exception as e:
                print(f"DCA error: {e}")
                await asyncio.sleep(60)

    async def _cycle(self):
        now = time.time()
        next_buy = float('inf')
        try:
            if self.redis:
                v = await self.redis.get(self.redis_key)
                if v:
                    next_buy = float(v)
        except Exception:
            pass

        if now >= next_buy:
            for pair in self.pairs:
                await self._dca_buy(pair)
            await self._check_tp()
            new_next = str(now + self.interval_minutes * 60)
            try:
                if self.redis:
                    await self.redis.setex(self.redis_key, 86400, new_next)
            except Exception as e:
                print(f"DCA redis set error: {e}")
        else:
            remain = int(next_buy - now)
            if remain % 300 < 61:
                print(f"DCA next buy in {remain//60}m")

    async def _dca_buy(self, pair: str):
        try:
            ticker = await self.exchange.fetch_ticker(pair)
            price = float(ticker.get("last", 0))
            if price <= 0:
                return
            size = round(self.amount_per_trade / price, 6)
            if size <= 0:
                return
            client_id = f"dca_{pair.replace('/','')}_{int(datetime.now(timezone.utc).timestamp())}"
            order = await self.exchange.create_market_buy_order(pair, size, client_id)
            fill_price = self._order_avg_price(order) or price
            fill_qty = float(order.get("filled", size))
            batch = {
                "entry_price": fill_price,
                "qty": fill_qty,
                "tp_price": round(fill_price * (1 + self.tp_percent / 100), 8),
                "entry_time": datetime.now(timezone.utc).isoformat(),
            }
            if pair not in self.positions:
                self.positions[pair] = []
            self.positions[pair].append(batch)
            self.db.log_trade({
                "timestamp": datetime.now(timezone.utc), "pair": pair,
                "side": "buy", "price": fill_price, "quantity": fill_qty,
                "order_id": client_id, "status": "closed",
                "grid_level": None, "realized_pnl": None,
            })
            avg = sum(b["entry_price"] * b["qty"] for b in self.positions[pair]) / max(sum(b["qty"] for b in self.positions[pair]), 0.0001)
            msg = f"📥 DCA {pair}: bought ${self.amount_per_trade:.0f} @ ${fill_price:.4f}, avg ${avg:.4f}"
            print(msg)
            if self.notifier:
                await self.notifier.send_message(msg)
        except Exception as e:
            print(f"DCA buy {pair} failed: {e}")

    async def _check_tp(self):
        for pair, batches in list(self.positions.items()):
            remaining = []
            for batch in batches:
                try:
                    ticker = await self.exchange.fetch_ticker(pair)
                    price = float(ticker.get("last", 0))
                    if price >= batch["tp_price"]:
                        qty = batch["qty"]
                        client_id = f"dca_tp_{pair.replace('/','')}_{int(datetime.now(timezone.utc).timestamp())}"
                        order = await self.exchange.create_market_sell_order(pair, qty, client_id)
                        sell_price = self._order_avg_price(order) or price
                        pnl = round((sell_price - batch["entry_price"]) * qty, 2)
                        self.db.log_trade({
                            "timestamp": datetime.now(timezone.utc), "pair": pair,
                            "side": "sell", "price": sell_price, "quantity": qty,
                            "order_id": client_id, "status": "closed",
                            "grid_level": None, "realized_pnl": pnl,
                        })
                        msg = f"✅ DCA {pair}: TP at ${sell_price:.4f}, PnL ${pnl:.2f}"
                        print(msg)
                        if self.notifier:
                            await self.notifier.send_message(msg)
                        continue
                except Exception as e:
                    print(f"DCA TP check {pair} failed: {e}")
                remaining.append(batch)
            self.positions[pair] = remaining

    def _order_avg_price(self, order: dict) -> float:
        filled = float(order.get("filled", 0))
        cost = float(order.get("cost", 0))
        if filled > 0 and cost > 0:
            return round(cost / filled, 8)
        price = order.get("price")
        if price:
            return float(price)
        return 0.0
