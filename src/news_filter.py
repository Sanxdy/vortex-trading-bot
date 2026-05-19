import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

import aiohttp

logger = logging.getLogger(__name__)

SHARPE_FEED_URL = "https://www.sharpe.ai/api/v1/news/feed"

MACRO_EVENT_KEYWORDS = [
    "cpi", "fomc", "rate decision", "fed chair", "nonfarm",
    "interest rate", "inflation", "central bank", "federal reserve",
    "jobs report", "gdp", "treasury yield",
]

SHOCK_KEYWORDS = [
    "hack", "exploit", "drain", "bridge attack",
    "sec lawsuit", "chapter 11", "bankrupt",
    "halt trading", "emergency action",
]

CAUTION_KEYWORDS = [
    "liquidation", "volatility", "margin call",
    "exchange outage", "network congestion",
]


class NewsFilter:
    def __init__(self, check_window_hours: int = 2):
        self.check_window_hours = check_window_hours
        self._last_check: Dict[str, datetime] = {}
        self._cache: Dict[str, float] = {}
        self._reason_cache: Dict[str, str] = {}

    async def get_risk_multiplier(self, symbol: str) -> float:
        now = datetime.now(timezone.utc)
        if symbol in self._last_check:
            elapsed = (now - self._last_check[symbol]).total_seconds()
            if elapsed < 60 and symbol in self._cache:
                return self._cache[symbol]
        coin = symbol.split("/")[0].upper()
        try:
            articles = await self._fetch_articles(coin)
            mult, reason = self._analyze(articles, now)
            self._cache[symbol] = mult
            self._reason_cache[symbol] = reason
            self._last_check[symbol] = now
            return mult
        except Exception as e:
            logger.error(f"News check failed for {symbol}: {e}")
            self._cache[symbol] = 1.0
            self._reason_cache[symbol] = f"api_error: {e}"
            self._last_check[symbol] = now
            return 1.0

    async def _fetch_articles(self, coin: str) -> List[Dict[str, Any]]:
        params = {"coin": coin, "limit": 50}
        api_key = os.getenv("SHARPE_API_KEY", "")
        headers = {"Accept": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(SHARPE_FEED_URL, params=params, headers=headers) as resp:
                    if resp.status == 401:
                        logger.error("Sharpe API 401 — get a free key at https://www.sharpe.ai/login")
                        return []
                    if resp.status != 200:
                        logger.warning(f"Sharpe API returned {resp.status}")
                        return []
                    data = await resp.json()
                    articles = data.get("data", {}).get("articles", [])
                    logger.debug(f"Sharpe returned {len(articles)} articles for {coin}")
                    return articles
        except asyncio.TimeoutError:
            logger.warning(f"Sharpe API timeout for {coin} after 5s")
            return []
        except Exception as e:
            logger.error(f"Sharpe API error for {coin}: {e}")
            return []

    def _analyze(self, articles: List[Dict], now: datetime) -> tuple[float, str]:
        cutoff = now - timedelta(hours=self.check_window_hours)
        recent = []
        for art in articles:
            pub_str = art.get("published") or art.get("published_at")
            if not pub_str:
                continue
            try:
                pub_time = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            if pub_time >= cutoff:
                recent.append(art)
        if not recent:
            return 1.0, "no_recent_news"

        all_titles = " ".join((art.get("title") or "") for art in recent).lower()

        for kw in SHOCK_KEYWORDS:
            if kw in all_titles:
                return 0.3, f"extreme: '{kw}'"

        for kw in MACRO_EVENT_KEYWORDS:
            if kw in all_titles:
                return 0.5, f"macro: '{kw}'"

        for kw in CAUTION_KEYWORDS:
            if kw in all_titles:
                return 0.7, f"caution: '{kw}'"

        return 1.0, "normal"

    def get_event_summary(self, symbol: str) -> str:
        cached = self._cache.get(symbol)
        reason = self._reason_cache.get(symbol, "No news data available")
        if cached is None:
            return "No news data available"
        return f"[News] {symbol}: multiplier={cached} | reason={reason}"
