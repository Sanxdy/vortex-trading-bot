import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field

import aiohttp

logger = logging.getLogger(__name__)

SHARPE_FEED_URL = "https://www.sharpe.ai/api/v1/news/feed"

BINARY_EVENT_KEYWORDS = [
    "sec lawsuit", "sec charges", "sec hearing",
    "regulation", "regulatory crackdown", "ban", "delist",
    "doj", "department of justice", "cfpb", "senate hearing",
    "congressional hearing", "enforcement action",
    "cease and desist", "halt trading", "emergency action",
    "exploit", "hack", "drain", "bridge attack",
]

PANIC_KEYWORDS = [
    "crash", "freeze", "halt", "suspension", "delisting",
    "insolvent", "bankrupt", "emergency", "force close",
    "rug pull", "scam", "phishing", "drain", "exploit",
]

@dataclass
class NewsSignal:
    allow_trade: bool
    panic_score: float = 0.0
    important_count: int = 0
    bullish_pct: float = 50.0
    bearish_pct: float = 50.0
    top_headline: str = ""
    reason: str = ""
    raw_posts: List[Dict[str, Any]] = field(default_factory=list)

class NewsFilter:
    def __init__(self, max_panic_score: int = 70, max_important_count: int = 5,
                 min_bullish_pct: float = 40.0, check_window_hours: int = 2):
        self.max_panic_score = max_panic_score
        self.max_important_count = max_important_count
        self.min_bullish_pct = min_bullish_pct
        self.check_window_hours = check_window_hours
        self._last_check: Dict[str, datetime] = {}
        self._cache: Dict[str, NewsSignal] = {}

    async def should_trade(self, symbol: str) -> NewsSignal:
        now = datetime.now(timezone.utc)
        if symbol in self._last_check:
            elapsed = (now - self._last_check[symbol]).total_seconds()
            if elapsed < 60 and symbol in self._cache:
                return self._cache[symbol]
        coin = symbol.split("/")[0].upper()
        try:
            articles = await self._fetch_articles(coin)
            signal = self._analyze(symbol, articles, now)
            self._cache[symbol] = signal
            self._last_check[symbol] = now
            return signal
        except Exception as e:
            logger.error(f"News check failed for {symbol}: {e}")
            fallback = NewsSignal(allow_trade=True, reason=f"api_error: {e}")
            self._cache[symbol] = fallback
            self._last_check[symbol] = now
            return fallback

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
        except asyncio.TimeoutError:
            logger.warning(f"Sharpe API timeout for {coin} after 5s")
            return []
        except Exception as e:
            logger.error(f"Sharpe API error for {coin}: {e}")
            return []

    def _analyze(self, symbol: str, articles: List[Dict], now: datetime) -> NewsSignal:
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
            return NewsSignal(allow_trade=True, reason="no_recent_news")

        all_titles = " ".join((art.get("title") or "") for art in recent).lower()
        for kw in BINARY_EVENT_KEYWORDS:
            if kw in all_titles:
                return NewsSignal(allow_trade=False, top_headline=recent[0].get("title", ""),
                                  reason=f"binary_event: '{kw}'", raw_posts=recent)

        panic = self._panic_score(recent)
        if panic >= self.max_panic_score:
            return NewsSignal(allow_trade=False, panic_score=panic,
                              top_headline=recent[0].get("title", ""),
                              reason=f"panic_score {panic}", raw_posts=recent)

        important = sum(1 for a in recent if a.get("source_tier") in (1, 2))
        if important >= self.max_important_count:
            return NewsSignal(allow_trade=False, important_count=important,
                              top_headline=recent[0].get("title", ""),
                              reason=f"important_count {important}", raw_posts=recent)
        return NewsSignal(allow_trade=True, panic_score=panic, important_count=important,
                          reason="all_checks_passed", raw_posts=recent)

    def _panic_score(self, articles: List[Dict]) -> float:
        score = 0.0
        for art in articles:
            title = (art.get("title") or "").lower()
            tier = art.get("source_tier", 3)
            w = 1.5 if tier == 1 else 1.2 if tier == 2 else 1.0
            for kw in PANIC_KEYWORDS:
                if kw in title:
                    score += 15 * w
        return min(score, 100.0)

    def get_event_summary(self, symbol: str) -> str:
        cached = self._cache.get(symbol)
        if not cached:
            return "No news data available"
        return (f"[News] {symbol}: allow={cached.allow_trade} | "
                f"panic={cached.panic_score} | important={cached.important_count} | "
                f"reason={cached.reason}")
