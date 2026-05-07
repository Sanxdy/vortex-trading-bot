import aiohttp
import asyncio
import json
import xml.etree.ElementTree as ET

COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
    "XRP": "ripple", "ADA": "cardano", "SOL": "solana",
    "DOGE": "dogecoin", "AVAX": "avalanche-2", "DOT": "polkadot",
    "LINK": "chainlink", "MATIC": "matic-network", "UNI": "uniswap",
    "SHIB": "shiba-inu", "LTC": "litecoin", "ATOM": "cosmos",
    "XLM": "stellar", "TRX": "tron", "NEAR": "near"
}

SCAN_COINS = {
    "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
    "XRP": "ripple", "ADA": "cardano", "SOL": "solana",
    "DOGE": "dogecoin", "AVAX": "avalanche-2", "DOT": "polkadot",
    "LINK": "chainlink", "MATIC": "matic-network", "UNI": "uniswap",
    "SHIB": "shiba-inu", "LTC": "litecoin", "ATOM": "cosmos",
    "XLM": "stellar", "TRX": "tron", "NEAR": "near",
    "APT": "aptos", "ARB": "arbitrum", "OP": "optimism",
    "FIL": "filecoin", "ALGO": "algorand", "AAVE": "aave",
    "ICP": "internet-computer", "EGLD": "elrond-erd-2",
    "FTM": "fantom", "SAND": "the-sandbox", "MANA": "decentraland",
    "AXS": "axie-infinity", "CHZ": "chiliz", "CRV": "curve-dao-token",
    "GRT": "the-graph", "ENJ": "enjincoin", "ZIL": "zilliqa",
    "IOTA": "iota", "COMP": "compound-governance-token",
    "YFI": "yearn-finance", "SUSHI": "sushi", "SNX": "havven",
    "BAT": "basic-attention-token", "ZEC": "zcash", "DASH": "dash",
    "EOS": "eos", "VET": "vechain", "THETA": "theta-token",
}

ONCHAIN_SOURCES = {
    "BTC": {"key": None, "url": "https://mempool.space/api/v1/fees/recommended"},
    "SOL": {"key": "solscan", "url": "https://pro-api.solscan.io/v2.0/account/tokens?address=So11111111111111111111111111111111111111112"},
    "ETH": {"key": "etherscan", "url": "https://api.etherscan.io/api?module=stats&action=ethprice"},
    "BNB": {"key": "bscscan", "url": None},
}

RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://decrypt.co/feed",
]

COIN_KEYWORDS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "BNB": ["bnb", "binance coin"],
    "ADA": ["cardano", "ada"],
    "DOGE": ["dogecoin", "doge"],
    "AVAX": ["avalanche", "avax"],
    "DOT": ["polkadot", "dot"],
    "LINK": ["chainlink", "link"],
}

class Analyst:
    def __init__(self, config: dict):
        self.config = config
        self.deepseek_key = config.get("deepseek", {}).get("api_key", "")

    async def fetch_metrics(self, symbol: str) -> dict:
        ticker = symbol.split("/")[0]
        coin_id = COINGECKO_IDS.get(ticker)
        if not coin_id:
            return {}
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
                    params={"vs_currency": "usd", "days": "7"}, timeout=10
                )
                data = await resp.json()
                prices = [p[1] for p in data.get("prices", [])]
                if not prices:
                    return {}
                return {
                    "current_price": prices[-1],
                    "price_change_7d_pct": round(((prices[-1] - prices[0]) / prices[0] * 100), 2),
                    "high_7d": max(prices), "low_7d": min(prices),
                    "volatility_pct": round(((max(prices) - min(prices)) / min(prices) * 100), 2)
                }
        except Exception as e:
            print(f"Analyst: CoinGecko error ({symbol}): {e}")
            return {}

    async def fetch_rss(self, url: str) -> list:
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(url, timeout=10)
                text = await resp.text()
                root = ET.fromstring(text)
                ns = {"": "http://www.w3.org/2005/Atom"}
                items = []
                for item in root.iter("item"):
                    title = item.findtext("title", "")
                    items.append({"title": title, "source": url.split("/")[2]})
                for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                    title = entry.findtext("{http://www.w3.org/2005/Atom}title", "")
                    items.append({"title": title, "source": url.split("/")[2]})
                return items
        except Exception as e:
            print(f"Analyst: RSS error ({url}): {e}")
            return []

    async def fetch_news(self, symbol: str) -> list:
        ticker = symbol.split("/")[0]
        keywords = COIN_KEYWORDS.get(ticker, [ticker.lower()])
        all_items = await asyncio.gather(*[self.fetch_rss(url) for url in RSS_FEEDS])
        seen = set()
        matched = []
        for feed in all_items:
            for item in feed:
                title_lower = item["title"].lower()
                if any(k in title_lower for k in keywords):
                    if item["title"] not in seen:
                        seen.add(item["title"])
                        matched.append(item)
        return matched[:10]

    async def fetch_onchain(self, symbol: str) -> dict:
        ticker = symbol.split("/")[0]
        source = ONCHAIN_SOURCES.get(ticker)
        if not source or not source["url"]:
            return {}
        if source["key"]:
            api_key = self.config.get(source["key"], {}).get("api_key", "")
            if not api_key:
                return {}
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(source["url"], timeout=10)
                data = await resp.json()
                return {"onchain": data.get("data", data)[:3] if isinstance(data.get("data"), list) else data}
        except Exception as e:
            print(f"Analyst: On-chain error ({symbol}): {e}")
            return {}

    async def analyze_by_deepseek(self, symbol: str, metrics: dict, news: list, onchain: dict) -> dict:
        if not self.deepseek_key:
            return {"safe": True, "verdict": "NO_API_KEY", "reason": "DeepSeek not configured"}

        parts = [f"Analyze {symbol} for mean reversion grid trading."]
        if metrics:
            parts.append(f"\nMarket (7d): ${metrics.get('current_price','?')} | {metrics.get('price_change_7d_pct','?')}% | range ${metrics.get('low_7d','?')}-${metrics.get('high_7d','?')} | vol {metrics.get('volatility_pct','?')}%")
        if news:
            parts.append("\nRecent headlines:\n" + "\n".join(f"- [{n['source']}] {n['title']}" for n in news[:5]))
        if onchain:
            parts.append(f"\nOn-chain:\n{json.dumps(onchain, indent=2)[:300]}")

        parts.append("\nIs this market SAFE for a grid bot (needs sideways oscillation)? Strong trends are dangerous.")
        parts.append('Reply ONLY valid JSON: {"safe": true/false, "verdict": "SAFE"/"STRONG_UPTREND"/"STRONG_DOWNTREND"/"HIGH_VOLATILITY", "reason": "brief", "confidence": 0-100}')

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {self.deepseek_key}", "Content-Type": "application/json"},
                    json={"model": "deepseek-chat", "messages": [{"role": "user", "content": "\n".join(parts)}], "temperature": 0.1, "max_tokens": 300},
                    timeout=15
                )
                content = (await resp.json())["choices"][0]["message"]["content"]
                return json.loads(content.strip().strip("`").replace("json", "").strip())
        except Exception as e:
            print(f"Analyst: DeepSeek error ({symbol}): {e}")
            return {"safe": True, "reason": f"DeepSeek error: {e}"}

    async def should_enter(self, symbol: str) -> dict:
        print(f"Analyst: Analyzing {symbol}...")
        metrics = await self.fetch_metrics(symbol)
        if not metrics:
            return {"safe": True, "verdict": "NO_DATA", "reason": "Could not fetch market data"}
        news = await self.fetch_news(symbol)
        onchain = await self.fetch_onchain(symbol)
        return await self.analyze_by_deepseek(symbol, metrics, news, onchain)

    _suggest_cache = None
    _suggest_cache_time = 0

    async def suggest_pairs(self, force: bool = False) -> list:
        return await self._suggest_pairs(force)

    async def _suggest_pairs(self, force: bool = False) -> list:
        now = asyncio.get_event_loop().time()
        if not force and self._suggest_cache and (now - self._suggest_cache_time) < 300:
            return self._suggest_cache

        if not self.deepseek_key:
            return [{"ticker": "N/A", "reason": "DeepSeek key not configured"}]

        print("Analyst: Scanning market (sideways + uptrend)...")
        ids = list(SCAN_COINS.values())
        tickers = {v: k for k, v in SCAN_COINS.items()}

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={
                        "ids": ",".join(ids),
                        "vs_currencies": "usd",
                        "include_24hr_change": "true"
                    },
                    timeout=15
                )
                if resp.status != 200:
                    text = await resp.text()
                    print(f"Analyst: CoinGecko returned {resp.status}: {text[:200]}")
                    if resp.status == 429:
                        if self._suggest_cache:
                            return self._suggest_cache
                        return [{"ticker": "N/A", "reason": "CoinGecko rate limited. Wait 1 min and retry."}]
                    return [{"ticker": "N/A", "reason": f"CoinGecko HTTP {resp.status}"}]
                quick = await resp.json()
        except asyncio.TimeoutError:
            print("Analyst: CoinGecko timeout")
            if self._suggest_cache:
                return self._suggest_cache
            return [{"ticker": "N/A", "reason": "CoinGecko timed out. Try again later."}]
        except Exception as e:
            print(f"Analyst: Quick scan error: {e}")
            return [{"ticker": "N/A", "reason": f"CoinGecko error: {e}"}]

        if not quick or "status" in quick:
            print(f"Analyst: CoinGecko empty/error response: {quick}")
            if self._suggest_cache:
                return self._suggest_cache
            return [{"ticker": "N/A", "reason": "CoinGecko returned no data (rate limited?). Wait 1 min."}]

        sideways = []
        uptrend = []
        for coin_id, data in quick.items():
            ticker = tickers.get(coin_id, coin_id.upper())
            change = data.get("usd_24h_change")
            price = data.get("usd")
            if change is None or price is None:
                continue
            if abs(change) < 3:
                sideways.append((ticker, coin_id, price, change))
            elif change >= 3:
                uptrend.append((ticker, coin_id, price, change))

        if not sideways and not uptrend:
            msg = "No suitable coins found."
            self._suggest_cache = [{"ticker": "N/A", "reason": msg}]
            self._suggest_cache_time = now
            return self._suggest_cache

        sideways.sort(key=lambda x: abs(x[3]))
        uptrend.sort(key=lambda x: x[3], reverse=True)
        top_sideways = sideways[:5]
        top_uptrend = uptrend[:5]

        all_top = top_sideways + top_uptrend
        details = []
        for ticker, coin_id, price, change_24h in all_top:
            await asyncio.sleep(1.2)
            m = await self.fetch_metrics(f"{ticker}/USDT")
            if m:
                m["ticker"] = ticker
                m["change_24h"] = round(change_24h, 2)
                m["category"] = "sideways" if (ticker, coin_id, price, change_24h) in top_sideways else "uptrend"
                details.append(m)

        if not details:
            return [{"ticker": "N/A", "reason": "Could not fetch detailed data"}]

        prompt_parts = [
            "You are a crypto market analyst. Two categories of coins are provided.",
            "",
            "CATEGORY 1 — SIDEWAYS (best for grid bot, mean reversion):",
            "CATEGORY 2 — UPTREND (bot can handle but suboptimal, may re-center often).",
            "",
            "For each coin, here is 7-day market data:",
        ]
        for d in details:
            prompt_parts.append(
                f"[{d['category'].upper()}] {d['ticker']}: ${d['current_price']} | "
                f"24h: {d.get('change_24h','?')}% | 7d: {d['price_change_7d_pct']}% | "
                f"range ${d['low_7d']}-${d['high_7d']} | volatility {d['volatility_pct']}%"
            )

        prompt_parts.append(
            "\nYou MUST return EXACTLY 5 coins: 3 SIDEWAYS + 2 UPTREND. "
            "Rank by suitability for mean reversion grid trading. "
            "Include category in each entry. "
            "Reply ONLY valid JSON array NO MARKDOWN: "
            '[{"ticker": "BTC", "category": "sideways", "reason": "...", "score": 0-100}, {"ticker": "ETH", "category": "uptrend", "reason": "...", "score": 0-100}]'
        )

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {self.deepseek_key}", "Content-Type": "application/json"},
                    json={"model": "deepseek-chat", "messages": [{"role": "user", "content": "\n".join(prompt_parts)}], "temperature": 0.1, "max_tokens": 600},
                    timeout=20
                )
                content = (await resp.json())["choices"][0]["message"]["content"]
                result = json.loads(content.strip().strip("`").replace("json", "").strip())
                if isinstance(result, list):
                    self._suggest_cache = result
                    self._suggest_cache_time = now
                    return result
                return [{"ticker": "N/A", "reason": "Bad response format"}]
        except Exception as e:
            print(f"Analyst: DeepSeek suggest error: {e}")
            return [{"ticker": "N/A", "reason": f"DeepSeek error: {e}"}]
