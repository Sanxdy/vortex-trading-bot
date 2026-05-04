import asyncio
from exchange_wrapper import ExchangeWrapper
from strategist import Strategist
from notifier import Notifier
from db import TimescaleDB
from typing import List, Dict

class Executor:
    def __init__(self, config: dict, exchange: ExchangeWrapper, strategist: Strategist, notifier: Notifier):
        self.config = config
        self.exchange = exchange
        self.strategist = strategist
        self.notifier = notifier
        self.db = TimescaleDB(config)
        self.db.connect()
        self.symbol = config["grid"]["pair"]
        self.grid_levels: List[Dict] = []
        self.is_grid_active = False
        self.last_rebalance = None

    async def calculate_grid_levels(self, center_price: float) -> List[Dict]:
        width = self.config["grid"]["width_percent"] / 100
        count = self.config["grid"]["count"]
        buy_count = count // 2
        sell_count = count // 2
        levels = []
        for i in range(1, buy_count + 1):
            price = center_price * ((1 - width) ** i)
            levels.append({"type": "buy", "price": round(price, 4), "level": -i})
        for i in range(1, sell_count + 1):
            price = center_price * ((1 + width) ** i)
            levels.append({"type": "sell", "price": round(price, 4), "level": i})
        return sorted(levels, key=lambda x: x["price"])

    async def place_grid_orders(self, levels: List[Dict]):
        balance = await self.exchange.fetch_balance()
        usdt_balance = balance["USDT"]["free"]
        equity_per_level = usdt_balance * (self.config["grid"]["equity_percent_per_level"] / 100)
        for level in levels:
            side = level["type"]
            price = level["price"]
            amount = round(equity_per_level / price, 4)
            ticker = await self.exchange.watch_ticker(self.symbol)
            spread = (ticker["ask"] - ticker["bid"]) / ticker["last"]
            if spread > self.config["risk"]["slippage_max_percent"] / 100:
                print(f"Spread too high ({spread:.4f}), skipping {side} at {price}")
                continue
            order = await self.exchange.create_limit_order(self.symbol, side, amount, price)
            self.db.log_trade({
                "timestamp": order["timestamp"],
                "pair": self.symbol,
                "side": side,
                "price": order["price"],
                "quantity": order["amount"],
                "order_id": order.get("id"),
                "status": order["status"],
                "grid_level": level["level"],
                "realized_pnl": None
            })
            print(f"Placed {side} at {price} for {amount} SOL")

    async def watch_order_fills(self):
        while True:
            try:
                orders = await self.exchange.watch_orders(self.symbol)
                for order in orders:
                    if order["status"] == "closed":
                        side = order["side"]
                        fill_price = float(order["average"])
                        amount = float(order["filled"])
                        if side == "buy":
                            sell_price = round(fill_price * (1 + self.config["grid"]["width_percent"] / 100), 4)
                            sell_order = await self.exchange.create_limit_order(self.symbol, "sell", amount, sell_price)
                            self.db.log_trade({
                                "timestamp": sell_order["timestamp"],
                                "pair": self.symbol,
                                "side": "sell",
                                "price": sell_order["price"],
                                "quantity": sell_order["amount"],
                                "order_id": sell_order.get("id"),
                                "status": sell_order["status"],
                                "grid_level": None,
                                "realized_pnl": round((sell_price - fill_price) * amount, 4)
                            })
                            await self.notifier.send_message(f"✅ Buy filled at {fill_price}, placed sell at {sell_price}")
                        elif side == "sell":
                            buy_price = round(fill_price * (1 - self.config["grid"]["width_percent"] / 100), 4)
                            buy_order = await self.exchange.create_limit_order(self.symbol, "buy", amount, buy_price)
                            self.db.log_trade({
                                "timestamp": buy_order["timestamp"],
                                "pair": self.symbol,
                                "side": "buy",
                                "price": buy_order["price"],
                                "quantity": buy_order["amount"],
                                "order_id": buy_order.get("id"),
                                "status": buy_order["status"],
                                "grid_level": None,
                                "realized_pnl": round((fill_price - buy_price) * amount, 4)
                            })
                            await self.notifier.send_message(f"✅ Sell filled at {fill_price}, placed buy at {buy_price}")
            except Exception as e:
                print(f"Order watch error: {e}")
                await asyncio.sleep(1)

    async def check_exit_conditions(self):
        while self.is_grid_active:
            if self.strategist.should_exit_take_profit():
                await self.notifier.send_message("🎉 Take profit triggered (upper Bollinger Band)")
                await self.cancel_all_and_exit()
                break
            if self.grid_levels:
                lowest_level = min(level["price"] for level in self.grid_levels if level["type"] == "buy")
                stop_price = lowest_level * (1 - self.config["strategy"]["exit"]["stop_loss"]["percent_below_lowest_grid"] / 100)
                ticker = await self.exchange.watch_ticker(self.symbol)
                current_price = ticker["last"]
                if current_price < stop_price:
                    await self.notifier.send_message(f"🛑 Stop loss triggered: price {current_price} < {stop_price}")
                    await self.cancel_all_and_exit()
                    await asyncio.sleep(4 * 3600)
                    break
            if self.strategist.should_exit_trend_inversion():
                await self.notifier.send_message("📉 Trend inversion: 1h close below 200 EMA")
                await self.cancel_all_and_exit()
                break
            await asyncio.sleep(10)

    async def cancel_all_and_exit(self):
        self.is_grid_active = False
        await self.exchange.cancel_all_orders(self.symbol)
        balance = await self.exchange.fetch_balance()
        sol_balance = balance["SOL"]["free"]
        if sol_balance > 0:
            await self.exchange.create_market_sell_order(self.symbol, sol_balance)
        await self.notifier.send_message("🔴 All orders cancelled, positions liquidated")

    async def trigger_kill_switch(self):
        await self.cancel_all_and_exit()
        await self.notifier.send_message("❌ Kill switch activated, bot stopping")
        await self.exchange.close()
        self.db.close()

    async def run(self):
        print("Starting executor")
        while True:
            if not self.is_grid_active:
                if self.strategist.should_enter():
                    await self.notifier.send_message("🚀 Entry conditions met, deploying grid")
                    ticker = await self.exchange.watch_ticker(self.symbol)
                    center_price = ticker["last"]
                    self.grid_levels = await self.calculate_grid_levels(center_price)
                    await self.place_grid_orders(self.grid_levels)
                    self.is_grid_active = True
                    self.last_rebalance = asyncio.get_event_loop().time()
                    asyncio.create_task(self.watch_order_fills())
                    asyncio.create_task(self.check_exit_conditions())
            else:
                current_time = asyncio.get_event_loop().time()
                if (current_time - self.last_rebalance) > (self.config["strategy"]["rebalance_interval_hours"] * 3600):
                    await self.notifier.send_message("🔄 Rebalancing grid")
                    await self.exchange.cancel_all_orders(self.symbol)
                    ticker = await self.exchange.watch_ticker(self.symbol)
                    new_center = ticker["last"]
                    self.grid_levels = await self.calculate_grid_levels(new_center)
                    await self.place_grid_orders(self.grid_levels)
                    self.last_rebalance = current_time
            await asyncio.sleep(10)
