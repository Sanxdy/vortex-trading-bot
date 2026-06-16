"""Dollar Cost Average strategy — buy at fixed intervals, sell at TP."""
import asyncio, json, os, yaml
from datetime import datetime, timezone
from typing import Optional

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
        self.amount_per_trade = self.dca_config.get("amount_per_trade", 5.0)
        self.tp_percent = self.dca_config.get("tp_percent", 3.0)
        self.pairs = self.dca_config.get("pairs", [])
        self.positions = {}  # symbol -> list of DCA batches

    async def start(self):
        if not self.enabled or not self.pairs:
            return
        while True:
            try:
                await asyncio.sleep(self.interval_minutes * 60)
                for pair in self.pairs:
                    await self._dca_buy(pair)
                await self._check_tp()
            except Exception as e:
                print(f"DCA error: {e}")

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
            avg_price = sum(b["entry_price"] * b["qty"] for b in self.positions[pair]) / sum(b["qty"] for b in self.positions[pair]) if self.positions[pair] else 0
            print(f"DCA {pair}: bought {fill_qty} @ ${fill_price:.4f}, avg ${avg_price:.4f}")
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
                        print(f"DCA {pair}: TP hit! Sold @ ${sell_price:.4f}, PnL ${pnl:.2f}")
                        continue  # batch sold, don't add to remaining
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
