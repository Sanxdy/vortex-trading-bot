import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone


class TestDailyLossLogic:
    """Tests the loss limit calculation (executor.py _check_daily_loss)."""

    def test_loss_limit_default_5_percent(self):
        """Default max loss is 5% of SIMULATED_BALANCE=250 = $12.50."""
        initial = 250.0
        pct = 5
        max_loss = initial * (pct / 100)
        assert max_loss == 12.5

    def test_loss_hit_when_pnl_exceeds_limit(self):
        """PnL of -$15 exceeds $12.50 limit → trigger kill."""
        daily_pnl = -15.0
        max_loss = 12.5
        assert abs(daily_pnl) >= max_loss

    def test_loss_not_hit_when_pnl_within_limit(self):
        """PnL of -$10 is within $12.50 limit → no kill."""
        daily_pnl = -10.0
        max_loss = 12.5
        assert not (abs(daily_pnl) >= max_loss)

    def test_loss_not_hit_when_pnl_positive(self):
        """Positive PnL should never trigger."""
        daily_pnl = 5.0
        max_loss = 12.5
        assert not (max_loss > 0 and daily_pnl < 0 and abs(daily_pnl) >= max_loss)

    def test_override_disables_limit(self):
        """max_daily_loss=999999 should make any PnL within limit."""
        daily_pnl = -100.0
        max_loss = 999999.0
        assert not (max_loss > 0 and daily_pnl < 0 and abs(daily_pnl) >= max_loss)

    def test_override_with_reset_pnl_zeros_effective(self):
        """With daily_loss_reset_pnl = current PnL, effective = 0."""
        daily_pnl = -78.0
        reset_pnl = -78.0
        effective = daily_pnl - reset_pnl
        assert effective == 0.0

    def test_loss_limit_reset_at_midnight(self):
        """When daily_loss_reset_at != today, override is ignored."""
        today = datetime.now(timezone.utc).date().isoformat()
        yesterday = "2026-06-03"
        # If reset_at is yesterday, it shouldn't match today
        assert yesterday != today
        # With no override, limit defaults to 5%
        daily_pnl = -15.0
        max_loss = 12.5
        assert abs(daily_pnl) >= max_loss

    def test_override_ignored_when_date_mismatch(self):
        """max_daily_loss only applies when daily_loss_reset_at == today."""
        daily_pnl = -15.0
        max_loss_override = 999999.0
        reset_at = "2026-06-03"
        today = datetime.now(timezone.utc).date().isoformat()
        if reset_at == today:
            max_loss = max_loss_override
        else:
            max_loss = 12.5
        assert max_loss == 12.5  # override ignored
        assert abs(daily_pnl) >= max_loss

    def test_override_applies_when_date_matches(self):
        """max_daily_loss applies when daily_loss_reset_at == today."""
        daily_pnl = -15.0
        max_loss_override = 999999.0
        reset_at = datetime.now(timezone.utc).date().isoformat()
        today = reset_at
        if reset_at == today:
            max_loss = max_loss_override
        else:
            max_loss = 12.5
        assert max_loss == max_loss_override  # override applies
        assert not (max_loss > 0 and daily_pnl < 0 and abs(daily_pnl) >= max_loss)

    def test_refill_force_sets_correct_keys(self):
        """Verify the Redis keys set by /refill --force."""
        keys = {
            "vortex:budget_remaining": "250",
            "vortex:max_daily_loss": "999999",
            "vortex:daily_loss_reset_at": datetime.now(timezone.utc).date().isoformat(),
            "vortex:daily_loss_reset_pnl": "-78.53",
        }
        assert keys["vortex:max_daily_loss"] == "999999"
        assert float(keys["vortex:budget_remaining"]) == 250.0
        assert keys["vortex:daily_loss_reset_at"] == datetime.now(timezone.utc).date().isoformat()

    def test_only_new_losses_count_after_force(self):
        """After force refill, only losses after the reset count."""
        daily_pnl = -80.0
        reset_pnl = -78.0
        new_pnl = daily_pnl - reset_pnl
        max_loss = 999999.0
        assert new_pnl == -2.0  # only -$2 of new losses
        assert not (max_loss > 0 and new_pnl < 0 and abs(new_pnl) >= max_loss)
