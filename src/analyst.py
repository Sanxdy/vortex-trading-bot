import aiohttp
import asyncio
import json
import math
import os
import xml.etree.ElementTree as ET

import pandas as pd
import pandas_ta as ta

from activity import push_activity

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
        # Prevent a startup "analysis stampede" from exhausting memory / sockets.
        # 6 symbols * 2 LLM calls + RSS/onchain calls can overwhelm small containers otherwise.
        self._sem = asyncio.Semaphore(int(os.getenv("ANALYST_CONCURRENCY", "2")))
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        timeout = aiohttp.ClientTimeout(total=15)
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def close(self):
        try:
            if self._session and not self._session.closed:
                await self._session.close()
        except Exception:
            pass

    async def _llm_completion(self, system_prompt: str, user_prompt: str) -> dict:
        try:
            return await self._provider_call(
                "https://api.deepseek.com/chat/completions",
                self.deepseek_key, "deepseek-chat",
                system_prompt, user_prompt
            )
        except Exception as e:
            print(f"Analyst: DeepSeek error ({system_prompt[:30]}...): {e}")
            await push_activity(f"DeepSeek error: {e}", "error")
            if self.fallback_key and self.fallback_endpoint and self.fallback_model:
                print("Analyst: falling back to secondary LLM")
                await push_activity("Falling back to secondary LLM", "warn")
                try:
                    return await self._provider_call(
                        self.fallback_endpoint,
                        self.fallback_key, self.fallback_model,
                        system_prompt, user_prompt
                    )
                except Exception as e2:
                    print(f"Analyst: fallback error: {e2}")
                    await push_activity(f"Analyst fallback error: {e2}", "error")
                    return {"safe": True, "verdict": "NO_DATA", "reason": f"LLM error: {e2}"}
            return {"safe": True, "verdict": "NO_DATA", "reason": f"DeepSeek error: {e}"}

    async def _provider_call(self, endpoint: str, api_key: str, model: str, system_prompt: str, user_prompt: str) -> dict:
        session = await self._get_session()
        resp = await session.post(
                endpoint,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ], "temperature": 0.1, "max_tokens": 300},
            )
        data = await resp.json()
        content = (data["choices"][0]["message"]["content"])
        return self._parse_llm_json(content)

    def _parse_llm_json(self, content: str) -> dict:
        import re
        try:
            return json.loads(content.strip().strip("`").replace("json", "").strip())
        except json.JSONDecodeError:
            pass
        m = re.search(r'```(?:json)?\s*([\s\S]*?)```', content)
        if m:
            try:
                return json.loads(m.group(1).strip())
            except json.JSONDecodeError:
                pass
        m = re.search(r'\{[\s\S]*\}', content)
        if m:
            try:
                return json.loads(m.group(0))
            except (json.JSONDecodeError, ValueError):
                pass
        fixed = re.sub(r',(\s*[}\]])', r'\1', content)
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass
        return {"safe": True, "verdict": "NO_DATA", "reason": "Could not parse LLM response", "confidence": 0}

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

