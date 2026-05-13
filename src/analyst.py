import aiohttp
import asyncio
import json
import math
import xml.etree.ElementTree as ET

import pandas as pd
import pandas_ta as ta

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

PROFILE_CONFIGS = {
    "scalper": {
        "desc": "0.4% arithmetic grid, 10 levels, 5m",
        "weights": {"rvol": 0.30, "atr_pct": 0.25, "spread": 0.15, "candle_eff": 0.10, "momentum": 0.10, "adx_moderate": 0.10},
        "penalties": [
            (lambda m: m.get("spread_raw", 0) > 0.15, 15),
            (lambda m: m.get("rvol_raw", 0) < 0.5, 10),
            (lambda m: m.get("adx_slope_raw", 0) < -5, 10),
        ],
        "bonuses": [
            (lambda m: m.get("ema_alignment") in ("perfect_bullish", "bullish"), 5),
            (lambda m: m.get("rvol_raw", 0) > 2 and m.get("adx_raw", 0) > 20, 5),
        ],
    },
    "standard": {
        "desc": "1.5% geometric grid, 20 levels, 15m",
        "weights": {"sideways_bonus": 0.25, "liquidity": 0.20, "atr_pct": 0.15, "candle_eff": 0.15, "low_rvol": 0.15, "ema_alignment": 0.10},
        "penalties": [
            (lambda m: m.get("rvol_raw", 0) > 2.5, 15),
            (lambda m: m.get("adx_raw", 0) > 30, 10),
            (lambda m: m.get("spread_raw", 0) > 0.2, 10),
        ],
        "bonuses": [
            (lambda m: m.get("regime") == "sideways", 5),
            (lambda m: m.get("candle_eff_raw", 0) > 0.7, 5),
        ],
    },
    "conservative": {
        "desc": "2% geometric grid, 15 levels, 15m",
        "weights": {"low_vol": 0.35, "liquidity": 0.25, "low_rvol": 0.15, "candle_eff": 0.15, "sideways_bonus": 0.10},
        "penalties": [
            (lambda m: m.get("rvol_raw", 0) > 2, 20),
            (lambda m: m.get("atr_pct_raw", 0) > 4, 20),
            (lambda m: m.get("regime") == "volatile", 15),
            (lambda m: m.get("spread_raw", 0) > 0.1, 10),
        ],
        "bonuses": [
            (lambda m: m.get("regime") == "sideways", 10),
            (lambda m: m.get("adx_raw", 0) < 15, 5),
        ],
    },
    "trend_only": {
        "desc": "Trend pullback entries only, 15m",
        "weights": {"adx": 0.25, "ema_alignment": 0.25, "candle_eff": 0.20, "rvol": 0.15, "liquidity": 0.15},
        "penalties": [
            (lambda m: m.get("adx_slope_raw", 0) < 0, 15),
            (lambda m: m.get("regime") == "sideways", 15),
            (lambda m: m.get("candle_eff_raw", 0) < 0.4, 10),
            (lambda m: m.get("rvol_raw", 0) > 3 and m.get("adx_raw", 0) < 20, 10),
        ],
        "bonuses": [
            (lambda m: m.get("adx_slope_raw", 0) > 5, 10),
            (lambda m: m.get("ema_alignment") == "perfect_bullish", 5),
        ],
    },
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
        self.fallback_key = config.get("fallback", {}).get("api_key", "")
        self.fallback_endpoint = config.get("fallback", {}).get("endpoint", "")
        self.fallback_model = config.get("fallback", {}).get("model", "")
        self.db = None

    async def _llm_completion(self, system_prompt: str, user_prompt: str) -> dict:
        try:
            return await self._provider_call(
                "https://api.deepseek.com/chat/completions",
                self.deepseek_key, "deepseek-chat",
                system_prompt, user_prompt
            )
        except Exception as e:
            print(f"Analyst: DeepSeek error ({system_prompt[:30]}...): {e}")
            if self.fallback_key and self.fallback_endpoint and self.fallback_model:
                print("Analyst: falling back to secondary LLM")
                try:
                    return await self._provider_call(
                        self.fallback_endpoint,
                        self.fallback_key, self.fallback_model,
                        system_prompt, user_prompt
                    )
                except Exception as e2:
                    print(f"Analyst: fallback error: {e2}")
                    return {"safe": True, "verdict": "NO_DATA", "reason": f"LLM error: {e2}"}
            return {"safe": True, "verdict": "NO_DATA", "reason": f"DeepSeek error: {e}"}

    async def _provider_call(self, endpoint: str, api_key: str, model: str, system_prompt: str, user_prompt: str) -> dict:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                endpoint,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ], "temperature": 0.1, "max_tokens": 300},
                timeout=15
            )
            content = (await resp.json())["choices"][0]["message"]["content"]
            return json.loads(content.strip().strip("`").replace("json", "").strip())

    def _build_memory_prompt(self, symbol: str) -> str:
        if not self.db:
            return ""
        recent = self.db.get_recent_decisions(symbol, limit=5)
        if not recent:
            return ""
        lines = ["\nYour recent trading history for this pair:"]
        for d in recent:
            outcome = f"PnL: ${d['outcome']:+.2f}" if d['outcome'] != 0 else "no fill yet"
            lines.append(f"  {d['timestamp'][:16]} | {d['decision']} | regime: {d['regime']} | ADX: {d['adx']} | RSI: {d['rsi']} | {outcome}")
        lines.append("Apply these lessons to your current analysis.\n")
        return "\n".join(lines)

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

    _should_enter_cache: dict = {}
    _should_enter_cache_time: dict = {}

    async def should_enter(self, symbol: str) -> dict:
        now = asyncio.get_event_loop().time()
        cached = self._should_enter_cache.get(symbol)
        cached_time = self._should_enter_cache_time.get(symbol, 0)
        if cached and (now - cached_time) < 1800:
            return cached
        print(f"Analyst: Analyzing {symbol}...")
        memory = self._build_memory_prompt(symbol)
        metrics = await self.fetch_metrics(symbol)
        if not metrics:
            return {"safe": True, "verdict": "NO_DATA", "reason": "Could not fetch market data"}
        news = await self.fetch_news(symbol)
        onchain = await self.fetch_onchain(symbol)

        async def technical_agent():
            prompt = f"Analyze {symbol} for mean reversion grid trading.\n"
            if metrics:
                prompt += f"\nMarket (7d): ${metrics.get('current_price','?')} | {metrics.get('price_change_7d_pct','?')}% | range ${metrics.get('low_7d','?')}-${metrics.get('high_7d','?')} | vol {metrics.get('volatility_pct','?')}%"
            prompt += "\nFocus on: ADX trend strength, RSI levels, Bollinger Band position, ATR volatility. Is this pair in a safe range for grid trading?"
            prompt += memory
            prompt += 'Reply ONLY valid JSON: {"safe": true/false, "verdict": "SAFE"/"STRONG_UPTREND"/"STRONG_DOWNTREND"/"HIGH_VOLATILITY", "reason": "brief", "confidence": 0-100}'
            return await self._llm_completion("You are a technical analyst specialized in grid trading.", prompt)

        async def sentiment_agent():
            if not news and not onchain:
                return {"safe": True, "verdict": "SKIP", "reason": "No news or on-chain data", "confidence": 50}
            prompt = f"Analyze news and on-chain data for {symbol}.\n"
            if news:
                prompt += "\nRecent headlines:\n" + "\n".join(f"- [{n['source']}] {n['title']}" for n in news[:5])
            if onchain:
                prompt += f"\nOn-chain:\n{json.dumps(onchain, indent=2)[:300]}"
            prompt += "\nIs the market mood bullish, bearish, or neutral? Any events that could disrupt a grid bot?"
            prompt += memory
            prompt += 'Reply ONLY valid JSON: {"safe": true/false, "verdict": "SAFE"/"STRONG_UPTREND"/"STRONG_DOWNTREND"/"HIGH_VOLATILITY", "reason": "brief", "confidence": 0-100}'
            return await self._llm_completion("You are a sentiment analyst monitoring market mood and news.", prompt)

        results = await asyncio.gather(technical_agent(), sentiment_agent(), return_exceptions=True)
        tech_result = results[0] if not isinstance(results[0], Exception) else {"safe": True, "verdict": "NO_DATA", "reason": f"Technical agent error: {results[0]}", "confidence": 0}
        sent_result = results[1] if not isinstance(results[1], Exception) else {"safe": True, "verdict": "SKIP", "reason": "Sentiment agent unavailable", "confidence": 50}

        combined = self._merge_verdicts(tech_result, sent_result)
        self._should_enter_cache[symbol] = combined
        self._should_enter_cache_time[symbol] = now
        return combined

    def _merge_verdicts(self, tech: dict, sent: dict) -> dict:
        for v in [tech, sent]:
            if v.get("verdict") in ("STRONG_DOWNTREND", "HIGH_VOLATILITY") and not v.get("safe", True):
                return {"safe": False, "verdict": v["verdict"], "reason": f"{v.get('reason','')} (confirmed by multi-agent)", "confidence": min(int(v.get("confidence", 70)), 100)}
        if not tech.get("safe", True):
            return {"safe": False, "verdict": tech["verdict"], "reason": f"Technical: {tech.get('reason','')}", "confidence": min(int(tech.get("confidence", 70)), 100)}
        t_conf = int(tech.get("confidence", 50))
        s_conf = int(sent.get("confidence", 50))
        avg_conf = (t_conf + s_conf) // 2
        if tech.get("safe") and sent.get("safe"):
            avg_conf = min(int(avg_conf * 1.2), 100)
        return {"safe": True, "verdict": "SAFE", "reason": f"Tech: {tech.get('reason','')} | Sentiment: {sent.get('reason','')}", "confidence": avg_conf}

    _suggest_cache = None
    _suggest_cache_time = 0

    async def suggest_pairs(self, exchange, strategist=None, active_profile="standard", force=False):
        return await self._suggest_pairs(exchange, strategist, active_profile, force)

    async def _suggest_pairs(self, exchange, strategist, active_profile, force):
        now = asyncio.get_event_loop().time()
        if not force and self._suggest_cache and (now - self._suggest_cache_time) < 300:
            return self._suggest_cache

        if not self.deepseek_key:
            return [{"ticker": "N/A", "reason": "DeepSeek key not configured"}]

        profile = PROFILE_CONFIGS.get(active_profile, PROFILE_CONFIGS["standard"])
        print(f"Analyst: Scanning market for {active_profile} ({profile['desc']})...")

        # ---- Stage 1: Quick scan via fetch_tickers ----
        try:
            all_tickers = await exchange.fetch_tickers()
        except Exception as e:
            print(f"Analyst: Binance tickers error: {e}")
            if self._suggest_cache:
                return self._suggest_cache
            return [{"ticker": "N/A", "reason": f"Binance error: {e}"}]

        candidates = {}
        for ticker in SCAN_COINS:
            pair = f"{ticker}/USDT"
            t = all_tickers.get(pair)
            if not t:
                continue
            price = t.get("last")
            change = t.get("percentage")
            volume = t.get("quoteVolume", 0)
            bid, ask = t.get("bid"), t.get("ask")
            spread = ((ask - bid) / bid * 100) if bid and ask and bid > 0 else 0.5
            if price is None or change is None or volume < 100_000 or spread > 0.5:
                continue
            candidates[ticker] = {"price": price, "change": change, "volume": volume, "spread": spread}

        if len(candidates) < 3:
            msg = "Not enough tradeable coins found."
            self._suggest_cache = [{"ticker": "N/A", "reason": msg}]
            self._suggest_cache_time = now
            return self._suggest_cache

        sorted_candidates = sorted(candidates.items(), key=lambda x: x[1]["volume"] * abs(x[1]["change"] or 0), reverse=True)
        shortlist = [t for t, _ in sorted_candidates[:12]]

        # ---- Stage 2: Deep scan — fetch OHLCV + compute indicators ----
        async def fetch_one(ticker):
            try:
                ohlcv = await exchange.fetch_ohlcv(f"{ticker}/USDT", "1h", limit=50)
                return ticker, ohlcv
            except Exception:
                return ticker, []

        ohlcv_results = await asyncio.gather(*[fetch_one(t) for t in shortlist])
        ohlcv_map = {t: data for t, data in ohlcv_results if len(data) >= 21}

        indicators = {}
        for ticker in shortlist:
            if ticker not in ohlcv_map:
                continue
            c = candidates[ticker]
            ind = self._compute_indicators(ticker, ohlcv_map[ticker], c)
            if ind:
                indicators[ticker] = ind

        if not indicators:
            return [{"ticker": "N/A", "reason": "Could not compute indicators for any candidate"}]

        # ---- Stage 3: Deterministic scoring ----
        scored = self._score_pairs(indicators, active_profile, profile)
        top_five = scored[:5]

        # ---- Stage 4: DeepSeek reasoning for top 3-5 ----
        reasoning = await self._deepseek_reasoning(top_five, active_profile, profile)

        # Merge reasoning into results
        reason_map = {r["ticker"]: r for r in reasoning} if reasoning else {}
        for item in top_five:
            r = reason_map.get(item["ticker"], {})
            item["reasoning"] = r.get("reasoning", "")
            item["confidence"] = r.get("confidence", "medium")
            item["danger"] = r.get("danger")

        self._suggest_cache = top_five
        self._suggest_cache_time = now
        return top_five

    # ---- Helpers ----

    def _compute_indicators(self, ticker, ohlcv, ticker_row):
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        if len(df) < 21:
            return None
        try:
            atr_series = df.ta.atr(length=14)
            rsi_series = df.ta.rsi(length=14)
            adx_df = df.ta.adx(length=14)
            ema20 = df.ta.ema(length=20)
            ema50 = df.ta.ema(length=50)
            ema200 = df.ta.ema(length=200) if len(df) >= 200 else None
        except Exception:
            return None

        last_close = df["close"].iloc[-1]
        atr_val = atr_series.iloc[-1] if atr_series is not None and not pd.isna(atr_series.iloc[-1]) else 0
        atr_pct = (atr_val / last_close * 100) if last_close > 0 else 0
        adx_val = adx_df.iloc[-1, 0] if adx_df is not None and not pd.isna(adx_df.iloc[-1, 0]) else 0
        adx_slope = (adx_df.iloc[-1, 0] - adx_df.iloc[-4, 0]) if adx_df is not None and len(adx_df) >= 4 and not pd.isna(adx_df.iloc[-1, 0]) and not pd.isna(adx_df.iloc[-4, 0]) else 0
        rsi_val = rsi_series.iloc[-1] if rsi_series is not None and not pd.isna(rsi_series.iloc[-1]) else 50

        volumes = df["volume"].values
        avg_vol = volumes[-21:-1].mean() if len(volumes) > 21 else volumes.mean()
        rvol = (volumes[-1] / avg_vol) if avg_vol > 0 else 1.0

        effs = []
        for i in range(-20, 0):
            candle_range = df["high"].iloc[i] - df["low"].iloc[i]
            if candle_range > 0:
                effs.append(abs(df["close"].iloc[i] - df["open"].iloc[i]) / candle_range)
            elif len(effs) == 0:
                effs.append(0.5)
        candle_eff = sum(effs) / len(effs) if effs else 0.5

        # EMA alignment
        ema20_v = ema20.iloc[-1] if ema20 is not None and not pd.isna(ema20.iloc[-1]) else None
        ema50_v = ema50.iloc[-1] if ema50 is not None and not pd.isna(ema50.iloc[-1]) else None
        ema200_v = ema200.iloc[-1] if ema200 is not None and not pd.isna(ema200.iloc[-1]) else None

        if ema20_v and ema50_v and ema200_v and not pd.isna(ema20_v) and not pd.isna(ema50_v) and not pd.isna(ema200_v):
            if ema20_v > ema50_v > ema200_v and last_close > ema20_v:
                ema_alignment = "perfect_bullish"
            elif ema20_v > ema50_v and last_close > ema20_v:
                ema_alignment = "bullish"
            elif ema50_v > ema20_v and last_close < ema20_v:
                ema_alignment = "bearish"
            else:
                ema_alignment = "neutral"
        elif ema20_v and ema50_v:
            if ema20_v > ema50_v and last_close > ema20_v:
                ema_alignment = "bullish"
            else:
                ema_alignment = "neutral"
        else:
            ema_alignment = "neutral"

        # Regime classification (same logic as strategist)
        avg_atr = atr_series.mean() if atr_series is not None else 0
        atr_spike = atr_val > avg_atr * 2 if avg_atr > 0 else False
        if adx_val > 25:
            regime = "trending"
        elif atr_spike:
            regime = "volatile"
        else:
            regime = "sideways"

        # Pullback distance for trend_only (price distance from EMA20 as %)
        pullback_pct = abs(last_close - ema20_v) / ema20_v * 100 if ema20_v and ema20_v > 0 else 999

        return {
            "price": ticker_row["price"],
            "change_24h": round(ticker_row["change"], 2),
            "volume_24h": round(ticker_row["volume"]),
            "spread": round(ticker_row["spread"], 3),
            "atr_pct": round(atr_pct, 2), "rvol": round(rvol, 2),
            "adx": round(adx_val, 1), "adx_slope": round(adx_slope, 1),
            "rsi": round(rsi_val, 1), "candle_eff": round(candle_eff, 2),
            "regime": regime, "ema_alignment": ema_alignment,
            "momentum": abs(ticker_row["change"]),
            "pullback_pct": round(pullback_pct, 2),
        }

    def _score_pairs(self, indicators, profile_name, profile_cfg):
        tickers = list(indicators.keys())
        fields = ["rvol", "atr_pct", "adx", "adx_slope", "spread", "liquidity", "momentum", "candle_eff"]
        raw_vals = {f: [indicators[t][f] for t in tickers] for f in fields if f in indicators[tickers[0]]}
        raw_vals["liquidity"] = [math.log(max(v, 1)) for v in [indicators[t].get("volume_24h", 1) for t in tickers]]

        norm = {}
        for f, vals in raw_vals.items():
            mn, mx = min(vals), max(vals)
            norm[f] = {t: (v - mn) / (mx - mn) if mx > mn else 0.5 for v, t in zip(vals, tickers)}

        scored = []
        for t in tickers:
            d = indicators[t]
            n = {f: norm[f][t] for f in norm}
            n["spread"] = 1 - n.get("spread", 0)
            n["low_vol"] = 1 - n.get("atr_pct", 0)
            n["low_rvol"] = 1 - n.get("rvol", 0)
            n["adx_moderate"] = math.exp(-((n.get("adx", 0) - 0.4) ** 2) / (2 * 0.25 ** 2))
            n["candle_eff"] = d.get("candle_eff", 0.5)
            n["sideways_bonus"] = 1.0 if d.get("regime") == "sideways" else 0.0
            n["liquidity"] = n.get("liquidity", 0.5)
            n["rvol"] = n.get("rvol", 0.5)
            n["atr_pct"] = n.get("atr_pct", 0.5)
            n["momentum"] = n.get("momentum", 0.5)
            n["adx"] = n.get("adx", 0.5)
            n["adx_slope"] = n.get("adx_slope", 0.5)

            ema_map = {"perfect_bullish": 1.0, "bullish": 0.8, "neutral": 0.5, "bearish": 0.2}
            n["ema_alignment"] = ema_map.get(d.get("ema_alignment", "neutral"), 0.5)

            base = sum(n.get(k, 0) * w for k, w in profile_cfg["weights"].items()) * 100

            m = {**d, "spread_raw": d.get("spread", 0), "rvol_raw": d.get("rvol", 0),
                 "adx_raw": d.get("adx", 0), "adx_slope_raw": d.get("adx_slope", 0),
                 "atr_pct_raw": d.get("atr_pct", 0), "candle_eff_raw": d.get("candle_eff", 0)}

            penalty_total = sum(pen for cond, pen in profile_cfg["penalties"] if cond(m))
            bonus_total = sum(bon for cond, bon in profile_cfg["bonuses"] if cond(m))
            final = max(0, min(100, base - penalty_total + bonus_total))

            scored.append({"ticker": t, "score": round(final, 1), "metrics": d})

        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored

    async def _deepseek_reasoning(self, top_five, profile_name, profile_cfg):
        if not self.deepseek_key:
            return []

        price_lines = []
        for item in top_five:
            m = item["metrics"]
            price_lines.append(
                f"{item['ticker']} (score {item['score']}/100): "
                f"${m['price']} | 24h: {m['change_24h']}% | RVOL {m['rvol']} | "
                f"ATR {m['atr_pct']}% | ADX {m['adx']} ({m['adx_slope']:+.1f}) | "
                f"RSI {m['rsi']} | eff {m['candle_eff']} | spread {m['spread']}% | "
                f"{m['regime']} | {m['ema_alignment']}"
            )

        prompt = (
            f"Active profile: {profile_name} ({profile_cfg['desc']}).\n"
            f"Top candidates (score and raw metrics):\n" + "\n".join(price_lines) + "\n\n"
            "For each ticker, provide a brief reasoning (1 sentence), confidence (high/medium/low), "
            "and a danger warning if applicable (or null).\n"
            "Reply ONLY valid JSON array NO MARKDOWN:\n"
            '[{"ticker": "SOL", "reasoning": "...", "confidence": "high", "danger": null}]'
        )

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {self.deepseek_key}", "Content-Type": "application/json"},
                    json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.1, "max_tokens": 500},
                    timeout=20
                )
                content = (await resp.json())["choices"][0]["message"]["content"]
                result = json.loads(content.strip().strip("`").replace("json", "").strip())
                if isinstance(result, list):
                    return result
                return []
        except Exception as e:
            print(f"Analyst: DeepSeek reasoning error: {e}")
            return []
