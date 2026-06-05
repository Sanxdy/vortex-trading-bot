"""Minimal pandas_ta compatibility for Python 3.9.
Provides the subset of TA functions used by backtest/run.py.
"""
import numpy as np
import pandas as pd


def bbands(close, length=20, std=2):
    sma = close.rolling(length).mean()
    sd = close.rolling(length).std()
    return pd.DataFrame({
        f"BBU_{length}_{std}.0": sma + sd * std,
        f"BBM_{length}_{std}.0": sma,
        f"BBL_{length}_{std}.0": sma - sd * std,
        f"BBB_{length}_{std}.0": sma,
        f"BBP_{length}_{std}.0": (close - sma + sd * std) / (2 * sd * std),
    })


def ema(close, length=None):
    return close.ewm(span=length, adjust=False).mean()


def atr(high, low, close, length=14):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=length, adjust=False).mean()


def rsi(close, length=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=length, adjust=False).mean()
    avg_loss = loss.ewm(span=length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def adx(high, low, close, length=14):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    up = high - high.shift()
    down = low.shift() - low
    plus_dm = ((up > down) & (up > 0)).astype(float) * up
    minus_dm = ((down > up) & (down > 0)).astype(float) * down
    atr_val = tr.ewm(span=length, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(span=length, adjust=False).mean() / atr_val
    minus_di = 100 * minus_dm.ewm(span=length, adjust=False).mean() / atr_val
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    return pd.DataFrame({
        f"ADX_{length}": dx.ewm(span=length, adjust=False).mean(),
        f"DMN_{length}": minus_di,
        f"DMP_{length}": plus_di,
    })


def supertrend(high, low, close, length=7, multiplier=3):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    atr_val = tr.ewm(span=length, adjust=False).mean()
    hl_avg = (high + low) / 2
    upper = hl_avg + multiplier * atr_val
    lower = hl_avg - multiplier * atr_val
    st = pd.Series(np.nan, index=close.index)
    direction = pd.Series(1, index=close.index)
    for i in range(1, len(close)):
        if close.iloc[i] > upper.iloc[i - 1]:
            direction.iloc[i] = 1
        elif close.iloc[i] < lower.iloc[i - 1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i - 1]
        if direction.iloc[i] == 1:
            st.iloc[i] = lower.iloc[i] if st.iloc[i - 1] != lower.iloc[i] else min(upper.iloc[i], st.iloc[i - 1])
        else:
            st.iloc[i] = upper.iloc[i] if st.iloc[i - 1] != upper.iloc[i] else max(lower.iloc[i], st.iloc[i - 1])
    return pd.DataFrame({
        f"SUPERT_{length}_{multiplier}.0": st,
        f"SUPERTd_{length}_{multiplier}.0": direction,
        f"SUPERTl_{length}_{multiplier}.0": lower,
        f"SUPERTu_{length}_{multiplier}.0": upper,
    })
