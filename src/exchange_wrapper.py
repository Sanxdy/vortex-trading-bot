import asyncio
from decimal import Decimal, InvalidOperation
from typing import Optional

import ccxt.pro as ccxtpro

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
        is_futures = self.exchange_id in ("binanceusdm", "binancecoinm")
        opts = {
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
            'rateLimit': rate_limit_ms,
        }
        if is_futures:
            opts['options'] = {
                'defaultType': 'future',
            }
            if self.testnet:
                opts['options']['sandboxMode'] = False
                opts['options']['defaultType'] = 'future'
                opts['aiohttp_trust_env'] = True
        else:
            opts['options'] = {
                'testnet': self.testnet,
                'defaultType': 'spot',
                'fetchMarkets': ['spot'],
            }
        self.exchange = exchange_class(opts)
        if self.testnet:
            # Force testnet URLs since api.binance.com is ISP-blocked in some regions
            if not is_futures:
                self.exchange.urls = {
                    'api': {
                        'public': 'https://testnet.binance.vision/api/v3',
                        'private': 'https://testnet.binance.vision/api/v3',
                    },
                    'www': 'https://testnet.binance.vision',
                    'doc': 'https://binance-docs.github.io/apidocs/spot/en',
                }
            else:
                self.exchange.enable_demo_trading(True)
        await self.exchange.load_markets()
        await self.exchange.load_time_difference()
        self.exchange.options['adjustForTimeDifference'] = True
        self.exchange.options['recvWindow'] = 10000
        print(f"Connected to {self.exchange_id} ({'testnet' if self.testnet else 'live'}){' FUTURES' if is_futures else ' SPOT'}")

    def _client_params(self, client_order_id: Optional[str] = None, params: Optional[dict] = None) -> dict:
        merged = dict(params or {})
        if client_order_id:
            # Binance's API name is newClientOrderId; CCXT forwards it in params.
            merged.setdefault("newClientOrderId", client_order_id)
        return merged

    def _market(self, symbol: str) -> dict:
        if not self.exchange:
            raise RuntimeError("Exchange is not connected")
        market = self.exchange.markets.get(symbol)
        if not market:
            raise ValueError(f"Unknown market: {symbol}")
        return market

    def _min_cost(self, symbol: str) -> Decimal:
        value = self.get_min_notional(symbol)
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return Decimal("10")

    def normalize_limit_order(self, symbol: str, amount: float, price: float) -> tuple[str, str]:
        self._market(symbol)
        amount_s = self.exchange.amount_to_precision(symbol, amount)
        price_s = self.exchange.price_to_precision(symbol, price)
        cost = Decimal(amount_s) * Decimal(price_s)
        min_cost = self._min_cost(symbol)
        if Decimal(amount_s) <= 0:
            raise ValueError(f"{symbol} amount rounds to zero")
        if Decimal(price_s) <= 0:
            raise ValueError(f"{symbol} price rounds to zero")
        if cost < min_cost:
            raise ValueError(f"{symbol} order notional {cost} is below minimum {min_cost}")
        return amount_s, price_s

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        self._market(symbol)
        return self.exchange.amount_to_precision(symbol, amount)

    async def watch_ticker(self, symbol: str):
        return await self.exchange.watch_ticker(symbol)

    async def fetch_ticker(self, symbol: str):
        return await self.exchange.fetch_ticker(symbol)

    async def fetch_tickers(self):
        return await self.exchange.fetch_tickers()

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        return await self.exchange.watch_ohlcv(symbol, timeframe)

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 250):
        return await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

    async def watch_orders(self, symbol: str):
        return await self.exchange.watch_orders(symbol)

    async def create_limit_order(self, symbol: str, side: str, amount: float, price: float,
                                 client_order_id: Optional[str] = None, params: Optional[dict] = None):
        amount_s, price_s = self.normalize_limit_order(symbol, amount, price)
        return await self.exchange.create_order(
            symbol, 'limit', side, amount_s, price_s,
            self._client_params(client_order_id, params)
        )

    async def create_post_only_limit_order(self, symbol: str, side: str, amount: float, price: float,
                                           client_order_id: Optional[str] = None):
        amount_s, price_s = self.normalize_limit_order(symbol, amount, price)
        return await self.exchange.create_order(
            symbol, 'LIMIT_MAKER', side, amount_s, price_s,
            self._client_params(client_order_id)
        )

    async def fetch_order(self, order_id: str, symbol: str):
        return await self.exchange.fetch_order(order_id, symbol)

    async def fetch_open_orders(self, symbol: str):
        return await self.exchange.fetch_open_orders(symbol)

    async def cancel_order(self, order_id: str, symbol: str):
        return await self.exchange.cancel_order(order_id, symbol)

    async def cancel_all_orders(self, symbol: str):
        try:
            return await self.exchange.cancel_all_orders(symbol)
        except Exception:
            pass

    async def cancel_bot_orders(self, symbol: str, client_id_prefix: str):
        cancelled = []
        try:
            orders = await self.fetch_open_orders(symbol)
        except Exception:
            return cancelled
        for order in orders:
            cid = order.get("clientOrderId") or order.get("info", {}).get("clientOrderId") or order.get("info", {}).get("origClientOrderId") or ""
            if not str(cid).startswith(client_id_prefix):
                continue
            try:
                cancelled.append(await self.cancel_order(order["id"], symbol))
            except Exception:
                pass
        return cancelled

    async def create_market_sell_order(self, symbol: str, amount: float, client_order_id: Optional[str] = None):
        amount_s = self.exchange.amount_to_precision(symbol, amount)
        if Decimal(amount_s) <= 0:
            raise ValueError(f"{symbol} market sell amount rounds to zero")
        return await self.exchange.create_order(symbol, 'market', 'sell', amount_s, None, self._client_params(client_order_id))

    async def create_market_buy_order(self, symbol: str, amount: float, client_order_id: Optional[str] = None):
        amount_s = self.exchange.amount_to_precision(symbol, amount)
        if Decimal(amount_s) <= 0:
            raise ValueError(f"{symbol} market buy amount rounds to zero")
        return await self.exchange.create_order(symbol, 'market', 'buy', amount_s, None, self._client_params(client_order_id))

    async def fetch_balance(self):
        return await self.exchange.fetch_balance()

    async def fetch_time(self):
        return await self.exchange.fetch_time()

    def get_min_notional(self, symbol: str) -> float:
        m = self.exchange.markets.get(symbol)
        if m:
            try:
                return float(m['limits']['cost']['min'])
            except (KeyError, TypeError, ValueError):
                pass
        return 10.0

    async def fetch_funding_rate(self, symbol: str) -> float:
        """Fetch current funding rate for futures pairs. Returns decimal (0.0001 = 0.01%). Returns 0 for spot."""
        try:
            if hasattr(self.exchange, "fetch_funding_rate"):
                rate = await self.exchange.fetch_funding_rate(symbol)
                return float(rate.get("fundingRate", 0))
        except Exception:
            pass
        return 0.0

    async def fetch_trading_fee(self, symbol: str):
        if hasattr(self.exchange, "fetch_trading_fee"):
            return await self.exchange.fetch_trading_fee(symbol)
        return None

    async def resync_time(self):
        while True:
            await asyncio.sleep(7200)
            try:
                await self.exchange.load_time_difference()
                print("Time re-synced with Binance")
            except Exception as e:
                print(f"Time sync failed: {e}")

    async def close(self):
        if self.exchange:
            await self.exchange.close()
