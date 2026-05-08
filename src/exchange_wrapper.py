import ccxt.pro as ccxtpro
from typing import Optional

class ExchangeWrapper:
    def __init__(self, config: dict):
        self.exchange_id = config["exchange"]["name"]
        self.api_key = config["exchange"]["api_key"]
        self.api_secret = config["exchange"]["api_secret"]
        self.testnet = config["exchange"]["testnet"]
        self.rate_limit = config["exchange"]["rate_limit"]
        self.exchange: Optional[ccxtpro.Exchange] = None

    async def connect(self):
        exchange_class = getattr(ccxtpro, self.exchange_id)
        rate_limit_ms = int(self.rate_limit["interval"] / self.rate_limit["max_requests"] * 1000)
        self.exchange = exchange_class({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
            'rateLimit': rate_limit_ms,
            'options': {
                'testnet': self.testnet,
                'defaultType': 'spot',
            }
        })
        await self.exchange.load_markets()
        print(f"Connected to {self.exchange_id} ({'testnet' if self.testnet else 'live'})")

    async def watch_ticker(self, symbol: str):
        return await self.exchange.watch_ticker(symbol)

    async def fetch_ticker(self, symbol: str):
        return await self.exchange.fetch_ticker(symbol)

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        return await self.exchange.watch_ohlcv(symbol, timeframe)

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 250):
        return await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

    async def watch_orders(self, symbol: str):
        return await self.exchange.watch_orders(symbol)

    async def create_limit_order(self, symbol: str, side: str, amount: float, price: float):
        return await self.exchange.create_limit_order(symbol, side, amount, price)

    async def cancel_all_orders(self, symbol: str):
        try:
            return await self.exchange.cancel_all_orders(symbol)
        except Exception:
            pass

    async def create_market_sell_order(self, symbol: str, amount: float):
        return await self.exchange.create_order(symbol, 'market', 'sell', amount, None)

    async def create_market_buy_order(self, symbol: str, amount: float):
        return await self.exchange.create_order(symbol, 'market', 'buy', amount, None)

    async def fetch_balance(self):
        return await self.exchange.fetch_balance()

    async def fetch_time(self):
        return await self.exchange.fetch_time()

    async def close(self):
        if self.exchange:
            await self.exchange.close()
