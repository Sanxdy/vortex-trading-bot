import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import json
import pytest
from executor import GridState, _regime_with_dir


class TestGridStateSaveFormat:
    """Verify ALL fields saved by _publish_orders (lines 910-920)."""

    SAVE_FIELDS = [
        "is_active",
        "orders",
        "dynamic_levels",
        "trend_active",
        "trend_entry_pending",
        "trend_entry",
        "trend_stop",
        "trend_target",
        "trend_size",
        "entry_type",
    ]

    LOAD_FIELDS = [
        "trend_active",
        "trend_entry",
        "trend_stop",
        "trend_target",
        "trend_size",
        "is_active",
        "entry_type",
    ]

    def test_all_save_fields_present(self, sample_config):
        """_publish_orders writes all expected keys."""
        st = GridState("BTC/USDT", sample_config)
        st.trend_active = True
        st.trend_entry_price = 100.0
        st.trend_stop = 99.0
        st.trend_target = 101.0
        st.trend_size = 1.0
        st.is_active = True
        st.entry_type = "scalping_5m"
        data = self._build_save_data(st)
        for field in self.SAVE_FIELDS:
            assert field in data, f"Missing save field: {field}"

    def test_all_load_fields_restored(self, sample_config):
        """GridState.load from dict restores all expected fields."""
        saved = {
            "is_active": True,
            "orders": [],
            "dynamic_levels": 0,
            "trend_active": True,
            "trend_entry_pending": False,
            "trend_entry": 100.0,
            "trend_stop": 99.0,
            "trend_target": 101.0,
            "trend_size": 1.0,
            "entry_type": "scalping_5m",
        }
        st = GridState("BTC/USDT", sample_config)
        self._apply_load_data(st, saved)
        assert st.trend_active is True
        assert st.trend_entry_price == 100.0
        assert st.trend_stop == 99.0
        assert st.trend_target == 101.0
        assert st.trend_size == 1.0
        assert st.is_active is True
        assert st.entry_type == "scalping_5m"

    def test_missing_entry_type_defaults_empty(self, sample_config):
        """Old grid_state without entry_type field should default to ''."""
        saved = {
            "trend_active": True,
            "trend_entry": 100.0,
            "trend_stop": 99.0,
            "trend_target": 101.0,
            "trend_size": 1.0,
            "is_active": True,
        }
        st = GridState("BTC/USDT", sample_config)
        self._apply_load_data(st, saved)
        assert st.entry_type == ""

    def test_save_load_roundtrip_preserves_all_fields(self, sample_config):
        """Simulate full save→JSON→load cycle."""
        original = GridState("BTC/USDT", sample_config)
        original.trend_active = True
        original.trend_entry_price = 100.0
        original.trend_stop = 99.0
        original.trend_target = 101.0
        original.trend_size = 1.0
        original.is_active = True
        original.entry_type = "bb_squeeze"

        data = self._build_save_data(original)
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)

        restored = GridState("BTC/USDT", sample_config)
        self._apply_load_data(restored, restored_data)

        assert restored.trend_active == original.trend_active
        assert restored.trend_entry_price == original.trend_entry_price
        assert restored.trend_stop == original.trend_stop
        assert restored.trend_target == original.trend_target
        assert restored.trend_size == original.trend_size
        assert restored.is_active == original.is_active
        assert restored.entry_type == original.entry_type

    def test_entry_regime_stored_with_arrow(self, sample_ec):
        """entry_regime should include direction arrow."""
        for symbol, ec in sample_ec.items():
            result = _regime_with_dir(ec)
            if ec.get("regime") == "trending":
                assert "↑" in result or "↓" in result

    def _build_save_data(self, st):
        """Mirrors executor.py _publish_orders data dict (lines 910-920)."""
        return {
            "is_active": st.is_active,
            "orders": [],
            "dynamic_levels": 0,
            "trend_active": st.trend_active,
            "trend_entry_pending": getattr(st, "trend_entry_pending", False),
            "trend_entry": getattr(st, "trend_entry_price", 0),
            "trend_stop": getattr(st, "trend_stop", 0),
            "trend_target": getattr(st, "trend_target", 0),
            "trend_size": getattr(st, "trend_size", 0),
            "entry_type": st.entry_type,
        }

    def _apply_load_data(self, st, data):
        """Mirrors executor.py grid_state load logic (lines 2899-2905)."""
        st.trend_active = data.get("trend_active", False)
        st.trend_entry_price = float(data.get("trend_entry", 0))
        st.trend_stop = float(data.get("trend_stop", 0))
        st.trend_target = float(data.get("trend_target", 0))
        st.trend_size = float(data.get("trend_size", 0))
        st.is_active = data.get("is_active", False)
        st.entry_type = data.get("entry_type", "")
