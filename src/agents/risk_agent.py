def assess_risk(symbol: str, ec: dict, budget: float, streak: int = 0) -> dict:
    regime = ec.get("regime", "unknown")
    adx = float(ec.get("adx", 0) or 0)
    rsi = float(ec.get("rsi", 50) or 50)
    atr_pct = float(ec.get("atr_pct", 0) or 0)
    rvol = float(ec.get("rvol", 1) or 1)
    default = {"size_mult": 1.0, "sl_atr": 2.5, "reason": "default"}

    if not ec or adx <= 0:
        return default

    reasons = []
    mult = 1.0
    sl_atr = 2.5

    if atr_pct > 5:
        mult *= 0.5
        sl_atr = 3.5
        reasons.append(f"high_vol_{atr_pct:.1f}%_x0.5")
    elif atr_pct > 3:
        mult *= 0.7
        sl_atr = 3.0
        reasons.append(f"vol_{atr_pct:.1f}%_x0.7")

    if adx > 35 and 40 < rsi < 60:
        mult *= 1.5
        sl_atr = 2.0
        reasons.append(f"strong_trend_adx{adx:.0f}_x1.5")
    elif adx > 25 and 35 < rsi < 65:
        mult *= 1.2
        reasons.append(f"trend_adx{adx:.0f}_x1.2")
    elif adx < 20:
        mult *= 0.7
        sl_atr = 3.0
        reasons.append(f"chop_adx{adx:.0f}_x0.7")

    if rvol > 2:
        mult *= 1.2
        reasons.append(f"high_vol_rvol{rvol:.1f}_x1.2")
    elif rvol < 0.5:
        mult *= 0.8
        reasons.append(f"low_vol_rvol{rvol:.1f}_x0.8")

    if rsi < 30:
        mult *= 0.6
        sl_atr = 3.5
        reasons.append(f"extreme_rsi{rsi:.0f}_x0.6")
    elif rsi > 70:
        mult *= 0.6
        sl_atr = 3.5
        reasons.append(f"extreme_rsi{rsi:.0f}_x0.6")

    if streak >= 3:
        mult *= 0.5
        reasons.append(f"loss_streak_{streak}_x0.5")
    elif streak >= 2:
        mult *= 0.7
        reasons.append(f"loss_streak_{streak}_x0.7")

    mult = round(max(0.3, min(2.0, mult)), 2)
    sl_atr = round(max(1.5, min(5.0, sl_atr)), 1)

    return {"size_mult": mult, "sl_atr": sl_atr, "reason": "|".join(reasons)}
