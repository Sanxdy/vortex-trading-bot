"""API contract tests — curl the running dashboard and verify response shapes.

Requires the bot and dashboard to be running on the server.
Set DASHBOARD_URL env var, or defaults to http://localhost:8000.
"""

import os
import json
import urllib.request
import urllib.error
import pytest

DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8000")


def _get(path):
    url = DASHBOARD_URL + path
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, ConnectionRefusedError, json.JSONDecodeError) as e:
        pytest.skip(f"Cannot reach {url}: {e}")


STATUS_KEYS = ["online", "profile", "pairs", "slots", "used", "budget_per_slot",
               "reserve", "holders"]
BUDGET_KEYS = ["remaining", "total", "percent"]
CONDITIONS_PAIR_KEYS = ["regime", "adx", "rsi", "atr", "rvol", "trend_active",
                        "trend_entry", "trend_stop", "trend_target", "entry_type"]
CONDITIONS_META_KEYS = ["regime_mode", "breakout", "trading_mode"]
CONDITIONS_STATS_KEYS = ["cycles", "signals", "rejected", "executed"]
CONFIG_KEYS = ["pairs", "profiles", "strategy", "execution", "fees"]
DECISIONS_KEYS = ["total", "decisions"]
TRADES_KEYS = ["trades", "total"]
PNL_SUMMARY_KEYS = ["trades", "wins", "losses", "realized_pnl"]
ORDERS_KEYS = ["orders", "dynamic"]
SYSTEM_KEYS = ["cpu", "mem", "disk"]

ENDPOINTS = [
    ("/api/status", STATUS_KEYS),
    ("/api/budget-status", BUDGET_KEYS),
    ("/api/fear-greed", ["value", "classification"]),
    ("/api/config", CONFIG_KEYS),
    ("/api/decisions?limit=1", DECISIONS_KEYS),
    ("/api/trades?limit=1", TRADES_KEYS),
    ("/api/pnl/summary", PNL_SUMMARY_KEYS),
    ("/api/orders/active", ORDERS_KEYS),
    ("/api/activity", None),  # list
    ("/api/pnl/by-regime", ["regimes"]),
]


@pytest.mark.parametrize("path,expected_keys", ENDPOINTS)
def test_endpoint_contract(path, expected_keys):
    j = _get(path)
    if expected_keys:
        for key in expected_keys:
            assert key in j, f"Missing key in {path}: {key}"


def test_conditions_contract():
    j = _get("/api/conditions")
    for key in ["pairs", "_meta", "_stats"]:
        assert key in j, f"Missing key in /api/conditions: {key}"
    if j.get("pairs"):
        pair = list(j["pairs"].values())[0]
        for key in CONDITIONS_PAIR_KEYS:
            assert key in pair, f"Missing pair field: {key}"
    meta = j.get("_meta", {})
    for key in CONDITIONS_META_KEYS:
        assert key in meta, f"Missing _meta field: {key}"
    stats = j.get("_stats", {})
    for key in CONDITIONS_STATS_KEYS:
        assert key in stats, f"Missing _stats field: {key}"


def test_system_contract():
    j = _get("/api/system")
    for key in SYSTEM_KEYS:
        assert key in j, f"Missing key in /api/system: {key}"
    assert "percent" in j["cpu"]
    assert "total" in j["mem"]
    assert "used" in j["disk"]


def test_conditions_entry_type_shows_arrow():
    """Regime in conditions should have direction for trending pairs."""
    j = _get("/api/conditions")
    if not j.get("pairs"):
        pytest.skip("No pairs available")
    for sym, pair in j["pairs"].items():
        if pair.get("regime") == "trending":
            assert pair.get("entry_type") is not None


def test_decisions_regime_has_arrow():
    """Recent decisions should have regime with arrow for trending."""
    j = _get("/api/decisions?limit=5")
    decisions = j.get("decisions", j.get("items", []))
    if not decisions:
        pytest.skip("No decisions available")
    for d in decisions[:3]:
        regime = d.get("regime", "")
        if regime.startswith("trending"):
            assert "↑" in regime or "↓" in regime, f"Missing arrow in regime: {regime}"


def test_orders_no_zero_prices():
    """Stop and tp_target orders must have price > 0. Zero prices corrupt the dashboard."""
    j = _get("/api/orders/active")
    orders = j.get("orders", [])
    for o in orders:
        if o["side"] in ("stop", "tp_target"):
            price = float(o.get("price", 0))
            assert price > 0, f"Order {o['symbol']} {o['side']} has price {price} — should not be published"
