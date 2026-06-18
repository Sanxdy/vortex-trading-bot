def assess_risk(symbol: str, ec: dict, budget: float, streak: int = 0) -> dict:
    regime = ec.get("regime", "unknown")
    adx = float(ec.get("adx", 0) or 0)
    rsi = float(ec.get("rsi", 50) or 50)
    atr_pct = float(ec.get("atr_pct", 0) or 0)
    rvol = float(ec.get("rvol", 1) or 1)
    default = {"size_mult": 1.0, "sl_atr": 2.5, "tp_atr": 2.0, "reason": "default"}

    if not ec or adx <= 0:
        return default

    reasons = []
    mult = 1.0
    sl_atr = 1.5
    tp_atr = 2.0

    if atr_pct > 5:
        mult *= 0.5
        sl_atr = 2.0
        tp_atr = 1.5
        reasons.append(f"high_vol_{atr_pct:.1f}%_x0.5")
    elif atr_pct > 3:
        mult *= 0.7
        sl_atr = 1.5
        tp_atr = 1.5
        reasons.append(f"vol_{atr_pct:.1f}%_x0.7")

    if adx > 35:
        mult *= 1.5
        sl_atr = 1.0
        tp_atr = 3.0
        reasons.append(f"strong_trend_adx{adx:.0f}_tp3.0")
    elif adx > 25:
        mult *= 1.2
        sl_atr = 1.0
        tp_atr = 2.5
        reasons.append(f"trend_adx{adx:.0f}_tp2.5")
    elif adx < 20:
        mult *= 0.7
        sl_atr = 1.5
        tp_atr = 1.5
        reasons.append(f"chop_adx{adx:.0f}_tp1.5")

    if rvol > 2:
        mult *= 1.2
        reasons.append(f"high_vol_rvol{rvol:.1f}")
    elif rvol < 0.5:
        mult *= 0.8
        reasons.append(f"low_vol_rvol{rvol:.1f}")

    if rsi < 30:
        mult *= 0.6
        sl_atr = 2.0
        tp_atr = 1.5
        reasons.append(f"extreme_rsi{rsi:.0f}")
    elif rsi > 70:
        mult *= 0.6
        sl_atr = 2.0
        tp_atr = 1.5
        reasons.append(f"extreme_rsi{rsi:.0f}")

    if streak >= 3:
        mult *= 0.5
        reasons.append(f"loss_streak_{streak}")
    elif streak >= 2:
        mult *= 0.7
        reasons.append(f"loss_streak_{streak}")

    mult = round(max(0.3, min(2.0, mult)), 2)
    sl_atr = round(max(1.0, min(4.0, sl_atr)), 1)
    tp_atr = round(max(1.5, min(4.0, tp_atr)), 1)

    return {"size_mult": mult, "sl_atr": sl_atr, "tp_atr": tp_atr, "reason": "|".join(reasons)}
