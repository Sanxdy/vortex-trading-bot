def score_pairs(all_pairs: dict) -> dict:
    scores = {}
    for symbol, ec in all_pairs.items():
        regime = ec.get("regime", "unknown")
        adx = float(ec.get("adx", 0) or 0)
        rsi = float(ec.get("rsi", 50) or 50)
        atr_pct = float(ec.get("atr_pct", 0) or 0)
        rvol = float(ec.get("rvol", 1) or 1)
        bb_lower = float(ec.get("bb_lower", 0) or 0)
        close = float(ec.get("close", 0) or 0)
        short_signal = bool(ec.get("short_signal", False))
        trend_uptrend = bool(ec.get("trend_uptrend", False))
        trend_pullback = ec.get("trend_pullback", False)

        if adx <= 0:
            scores[symbol] = 0
            continue

        score = 50

        if adx > 25:
            score += 25
            if "trending" in regime:
                score += 5
        elif adx < 20:
            score -= 10

        if 40 <= rsi <= 60:
            score += 20
        elif rsi < 30 or rsi > 70:
            score -= 15
        elif rsi < 35 or rsi > 65:
            score -= 5

        if rvol > 1.2:
            score += 15
        elif rvol < 0.5:
            score -= 10

        if trend_pullback:
            score += 20

        if short_signal and not trend_uptrend:
            score += 15
        elif short_signal and trend_uptrend:
            score -= 20

        if atr_pct > 5:
            score -= 15
        elif atr_pct > 3:
            score -= 5

        scores[symbol] = max(0, min(100, score))

    return scores
