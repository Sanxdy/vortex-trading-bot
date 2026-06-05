import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

@pytest.fixture
def sample_config():
    return {
        "exchange": {"name": "binance", "testnet": True},
        "pairs": [
            {"name": "BTC/USDT", "enabled": True, "grid": {"type": "geometric", "default_width_percent": 2.0, "default_count": 2, "default_equity_percent_per_level": 1.0, "profile_max_levels": 2}},
            {"name": "ETH/USDT", "enabled": True, "grid": {"type": "geometric", "default_width_percent": 2.0, "default_count": 2, "default_equity_percent_per_level": 1.0, "profile_max_levels": 2}},
            {"name": "DOGE/USDT", "enabled": True, "grid": {"type": "geometric", "default_width_percent": 2.0, "default_count": 2, "default_equity_percent_per_level": 1.0, "profile_max_levels": 2}},
        ],
        "strategy": {
            "regime": {"adx_period": 14, "adx_trend_threshold": 20, "atr_vol_spike": 2.0},
            "entry": {"timeframe": "4h", "ema_period": 50, "bb_threshold": 0.005},
            "trend": {"risk_percent": 1.0, "tp_atr": 1.0, "trail_atr": 1.0},
        },
        "grid": {"enabled": False, "type": "geometric", "default_width_percent": 2.0, "default_count": 2},
        "risk": {"max_daily_loss_percent": 5, "slippage_max_percent": 0.05},
        "profiles": {
            "sideway": {
                "strategy": {
                    "entry": {"timeframe": "4h"},
                    "trend": {"risk_percent": 1.0},
                    "exit": {"stop_loss": {"percent_below_lowest_grid": 3.0, "atr_multiplier": 1.5}},
                    "breakeven_pct": 0.2,
                },
                "risk": {"slippage_max_percent": 0.05},
            }
        },
        "allocator": {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6},
        "execution": {"force_market_entries": True, "cancel_bot_orders_on_start": True, "sweep_on_start": False},
        "fees": {"maker": 0.001, "taker": 0.001},
        "redis": {"host": "localhost", "port": 6379, "db": 0, "password": ""},
        "entry_paths": {},
    }

@pytest.fixture
def mock_redis():
    m = AsyncMock()
    m.get.return_value = None
    m.set.return_value = True
    m.setex.return_value = True
    m.delete.return_value = True
    m.exists.return_value = False
    m.keys.return_value = []
    return m

@pytest.fixture
def mock_exchange():
    ex = AsyncMock()
    ex.fetch_ticker.return_value = {"bid": 100.0, "ask": 100.1, "last": 100.05, "timestamp": 0}
    ex.watch_ticker.return_value = {"bid": 100.0, "ask": 100.1, "last": 100.05, "timestamp": 0}
    ex.fetch_balance.return_value = {"USDT": {"free": 250.0}}
    ex.get_min_notional.return_value = 10.0
    return ex

@pytest.fixture
def sample_ec():
    return {
        "DOGE/USDT": {
            "regime": "trending",
            "adx": 41.5,
            "adx_slope": 8.1,
            "rsi": 23.3,
            "atr": 0.0024,
            "rvol": 0.95,
            "trend_uptrend": False,
            "trend_pullback": False,
            "price_above_50_ema": False,
            "price_above_200_ema": False,
            "ema_20": 0.094,
            "ema_50": 0.097,
            "bb_lower": 0.0866,
        },
        "BTC/USDT": {
            "regime": "trending",
            "adx": 35.0,
            "adx_slope": 5.0,
            "rsi": 55.0,
            "atr": 500.0,
            "rvol": 1.2,
            "trend_uptrend": True,
            "trend_pullback": True,
            "price_above_50_ema": True,
            "price_above_200_ema": True,
            "ema_20": 95000,
            "ema_50": 92000,
            "bb_lower": 88000,
        },
        "ETH/USDT": {
            "regime": "sideways",
            "adx": 18.0,
            "adx_slope": 1.0,
            "rsi": 48.0,
            "atr": 20.0,
            "rvol": 0.6,
            "trend_uptrend": False,
            "trend_pullback": False,
            "price_above_50_ema": True,
            "price_above_200_ema": True,
            "ema_20": 3200,
            "ema_50": 3150,
            "bb_lower": 3000,
        },
    }
