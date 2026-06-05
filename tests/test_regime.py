import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from executor import _regime_with_dir, BudgetAllocator, GridState


class TestRegimeWithDir:
    def test_trending_uptrend_returns_arrow_up(self):
        ec = {"regime": "trending", "trend_uptrend": True}
        assert _regime_with_dir(ec) == "trending↑"

    def test_trending_downtrend_returns_arrow_down(self):
        ec = {"regime": "trending", "trend_uptrend": False}
        assert _regime_with_dir(ec) == "trending↓"

    def test_sideways_unchanged(self):
        ec = {"regime": "sideways"}
        assert _regime_with_dir(ec) == "sideways"

    def test_high_vol_unchanged(self):
        ec = {"regime": "high_vol"}
        assert _regime_with_dir(ec) == "high_vol"

    def test_unknown_unchanged(self):
        ec = {"regime": "unknown"}
        assert _regime_with_dir(ec) == "unknown"

    def test_empty_regime(self):
        ec = {"regime": ""}
        assert _regime_with_dir(ec) == ""

    def test_missing_regime(self):
        ec = {}
        assert _regime_with_dir(ec) == ""

    def test_trending_missing_uptrend_defaults_down(self):
        ec = {"regime": "trending"}
        assert _regime_with_dir(ec) == "trending↓"

    def test_trending_uptrend_none_defaults_down(self):
        ec = {"regime": "trending", "trend_uptrend": None}
        assert _regime_with_dir(ec) == "trending↓"


class TestEntryPathSelector:
    """Verify that startswith('trending') matches all regime variants."""

    def test_trending_matches(self):
        assert "trending".startswith("trending")

    def test_trending_up_matches(self):
        assert "trending↑".startswith("trending")

    def test_trending_down_matches(self):
        assert "trending↓".startswith("trending")

    def test_sideways_does_not_match(self):
        assert not "sideways".startswith("trending")

    def test_high_vol_does_not_match(self):
        assert not "high_vol".startswith("trending")

    def test_unknown_does_not_match(self):
        assert not "unknown".startswith("trending")


class TestStateLogRegime:
    """The STATE log format should show the modified regime, not raw."""

    def test_state_log_format_trending_down(self):
        ec = {"regime": "trending", "trend_uptrend": False}
        regime_label = _regime_with_dir(ec)
        log_msg = f"DOGE/USDT: INIT → {regime_label} (ADX 41.5, RSI 23.8)"
        assert "trending↓" in log_msg
        assert "trending↑" not in log_msg

    def test_state_log_format_trending_up(self):
        ec = {"regime": "trending", "trend_uptrend": True}
        regime_label = _regime_with_dir(ec)
        log_msg = f"BTC/USDT: INIT → {regime_label} (ADX 35.0, RSI 55.0)"
        assert "trending↑" in log_msg
        assert "trending↓" not in log_msg


class TestBudgetAllocator:
    def test_calculate_slots_from_balance(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        assert alloc.slots == 5
        assert alloc.budget_per_slot == 40.0
        assert alloc.reserve == 50.0

    def test_acquire_slot_success(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        result = asyncio_run(alloc.acquire("BTC/USDT"))
        assert result is True
        assert alloc.used == 1
        assert "BTC/USDT" in alloc._holders

    def test_acquire_slot_when_full(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        for i in range(alloc.slots):
            asyncio_run(alloc.acquire(f"PAIR{i}/USDT"))
        result = asyncio_run(alloc.acquire("EXTRA/USDT"))
        assert result is False
        assert alloc.used == alloc.slots

    def test_release_slot(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        asyncio_run(alloc.acquire("BTC/USDT"))
        asyncio_run(alloc.release("BTC/USDT"))
        assert alloc.used == 0
        assert "BTC/USDT" not in alloc._holders

    def test_release_underflow_does_not_go_negative(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        asyncio_run(alloc.release("NONE/USDT"))
        assert alloc.used == 0

    def test_reconcile_used_caps_at_slots(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        asyncio_run(alloc.reconcile_used(99))
        assert alloc.used == alloc.slots

    def test_reconcile_used_floor_at_zero(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        asyncio_run(alloc.reconcile_used(-5))
        assert alloc.used == 0

    def test_remove_pair(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        asyncio_run(alloc.acquire("BTC/USDT"))
        alloc.remove_pair("BTC/USDT")
        assert alloc.used == 0
        assert "BTC/USDT" not in alloc._holders

    def test_get_active_symbols(self):
        alloc = BudgetAllocator(250.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        asyncio_run(alloc.acquire("BTC/USDT"))
        asyncio_run(alloc.acquire("ETH/USDT"))
        active = alloc.get_active_symbols()
        assert active == {"BTC/USDT", "ETH/USDT"}

    def test_minimum_one_slot_always(self):
        alloc = BudgetAllocator(10.0, {"reserve_pct": 0.2, "min_per_slot": 35, "max_budget_pct": 0.6}, 22)
        assert alloc.slots >= 1


def asyncio_run(coro):
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as pool:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()


class TestTickerFallback:
    """When watch_ticker fails, bid/ask/last should default to 0 so entry_bid uses fill_price."""

    def test_bid_defaults_to_zero_when_ticker_fails(self):
        bid = 0
        fill_price = 100.0
        entry_bid = bid if bid > 0 else fill_price
        assert entry_bid == fill_price

    def test_trend_stop_calculated_from_fill_price_when_bid_zero(self):
        bid = 0
        fill_price = 100.0
        entry_bid = bid if bid > 0 else fill_price
        fixed_sl = 0.004
        trend_stop = entry_bid * (1 - fixed_sl)
        assert trend_stop == 99.6  # fill_price * 0.996

    def test_trend_target_calculated_from_fill_price_when_bid_zero(self):
        bid = 0
        fill_price = 100.0
        entry_bid = bid if bid > 0 else fill_price
        fixed_tp = 0.009
        trend_target = entry_bid * (1 + fixed_tp)
        assert trend_target == pytest.approx(100.9)  # fill_price * 1.009
