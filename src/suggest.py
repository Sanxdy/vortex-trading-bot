import asyncio
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

MIN_24H_VOLUME_USDT = 10_000_000
MAX_SPREAD_PCT = 0.05
MAX_ATR_5M_PCT = 0.5

TOP50_SYMBOLS = {
    "BTC", "ETH", "USDT", "BNB", "SOL", "USDC", "XRP", "DOGE", "ADA", "AVAX",
    "DOT", "LINK", "MATIC", "UNI", "BCH", "LTC", "LEO", "XLM", "XMR", "ATOM",
    "ETC", "FIL", "APT", "HBAR", "NEAR", "VET", "ICP", "ARB", "OP", "INJ",
    "GRT", "ALGO", "EGLD", "FTM", "THETA", "XTZ", "SAND", "MANA", "AXS", "CHZ",
    "KLAY", "ENJ", "STX", "FLOW", "LDO", "GALA", "CRV", "COMP", "MKR", "AAVE",
}

BLACKLIST = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FDUSD",
    "DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF",
}

ADX_THRESHOLDS = {
    "extreme_trend": 35,
    "strong_trend": 30,
    "weak_trend": 25,
    "no_trend": 20,
}

def compute_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        change = closes[i] - closes[i - 1]
        if change > 0:
            gains += change
        else:
            losses -= change
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))

def compute_adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(highs) < period + 1:
        return 50.0
    dm_plus: List[float] = []
    dm_minus: List[float] = []
    tr: List[float] = []
    for i in range(1, len(highs)):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        dm_plus.append(up if up > down and up > 0 else 0)
        dm_minus.append(down if down > up and down > 0 else 0)
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr = sum(tr[:period]) / period if period > 0 else 0
    if atr == 0:
        return 50.0
    smoothed_dm_plus = sum(dm_plus[:period])
    smoothed_dm_minus = sum(dm_minus[:period])
    di_plus = (smoothed_dm_plus / atr) * 100
    di_minus = (smoothed_dm_minus / atr) * 100
    dx = (abs(di_plus - di_minus) / (di_plus + di_minus)) * 100 if (di_plus + di_minus) > 0 else 0
    return min(100.0, max(0.0, dx))

def compute_efficiency(prices: List[float]) -> float:
    if len(prices) < 2:
        return 0.5
    net = abs(prices[-1] - prices[0])
    path = sum(abs(prices[i] - prices[i - 1]) for i in range(1, len(prices)))
    return net / path if path > 0 else 0

def compute_rvol(ohlcv: List[List], period: int = 20) -> float:
    if len(ohlcv) < period:
        return 0.5
    volumes = [c[5] for c in ohlcv]
    current = float(volumes[-1])
    avg = sum(float(v) for v in volumes[-period:]) / period
    return current / avg if avg > 0 else 1.0

async def fetch_top50_pairs(exchange) -> List[str]:
    try:
        markets = await exchange.exchange.load_markets()
    except Exception as e:
        logger.error(f"Failed to load markets: {e}")
        return []
    candidates = []
    for symbol, market in markets.items():
        if not symbol.endswith("/USDT"):
            continue
        base = symbol.split("/")[0]
        if base in BLACKLIST:
            continue
        if base not in TOP50_SYMBOLS:
            continue
        candidates.append(symbol)
    return candidates

async def calculate_pair_score(exchange, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        ticker = await exchange.fetch_ticker(symbol)
        last = ticker.get("last", 0)
        bid = ticker.get("bid", 0)
        ask = ticker.get("ask", 0)
        quote_volume = ticker.get("quoteVolume", 0)
        if not last or last <= 0:
            return None
        if quote_volume < MIN_24H_VOLUME_USDT:
            return None
        if bid <= 0 or ask <= 0:
            return None
        spread = (ask - bid) / ask * 100
        if spread > MAX_SPREAD_PCT:
            return None

        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="5m", limit=30)
        if len(ohlcv) < 15:
            return None

        closes = [float(c[4]) for c in ohlcv]
        highs = [float(c[2]) for c in ohlcv]
        lows = [float(c[3]) for c in ohlcv]

        tr_values = []
        for i in range(1, len(ohlcv)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            tr_values.append(tr)
        atr = sum(tr_values) / len(tr_values) if tr_values else 0
        atr_pct = (atr / last) * 100 if last > 0 else 0
        if atr_pct > MAX_ATR_5M_PCT:
            return None

        adx = compute_adx(highs, lows, closes, period=14)
        rsi = compute_rsi(closes, period=14)
        efficiency = compute_efficiency(closes[-15:])
        rvol = compute_rvol(ohlcv, period=20)

        score = 0.0
        spread_score = max(0, 1 - spread / MAX_SPREAD_PCT)
        score += spread_score * 2.5
        vol_score = min(1.0, quote_volume / 50_000_000)
        score += vol_score * 1.5
        if rvol >= 0.7:
            score += 1.0
        elif rvol >= 0.4:
            score += 0.5
        atr_score = max(0, 1 - atr_pct / MAX_ATR_5M_PCT)
        score += atr_score * 2.0
        if adx < ADX_THRESHOLDS["no_trend"]:
            adx_score = 1.0
        elif adx < ADX_THRESHOLDS["weak_trend"]:
            adx_score = 0.8
        elif adx < ADX_THRESHOLDS["strong_trend"]:
            adx_score = 0.4
        elif adx < ADX_THRESHOLDS["extreme_trend"]:
            adx_score = 0.1
        else:
            adx_score = 0.0
        score += adx_score * 2.0
        if 40 <= rsi <= 60:
            rsi_score = 1.0
        elif 35 <= rsi <= 65:
            rsi_score = 0.6
        else:
            rsi_score = 0.2
        score += rsi_score * 1.0
        score += efficiency * 1.5
        normalized = min(100, (score / 12.0) * 100)

        return {
            "symbol": symbol,
            "score": round(normalized, 1),
            "last": last,
            "spread": round(spread, 4),
            "adx": round(adx, 1),
            "atr_pct": round(atr_pct, 3),
            "rsi": round(rsi, 1),
            "rvol": round(rvol, 2),
            "efficiency": round(efficiency, 2),
            "quote_volume": quote_volume,
        }
    except Exception as e:
        logger.warning(f"Scoring failed for {symbol}: {e}")
        return None

async def get_suggestions(exchange, limit: int = 5) -> List[Dict[str, Any]]:
    candidates = await fetch_top50_pairs(exchange)
    if not candidates:
        return []
    tasks = [calculate_pair_score(exchange, sym) for sym in candidates]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    valid = [r for r in results if isinstance(r, dict)]
    valid.sort(key=lambda x: x["score"], reverse=True)
    return valid[:limit]
