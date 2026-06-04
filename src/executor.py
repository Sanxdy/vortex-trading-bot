import asyncio
import itertools
import json
import os
import aiohttp
import time
from datetime import datetime, timezone
from redis import asyncio as aioredis
from exchange_wrapper import ExchangeWrapper
from strategist import Strategist
from notifier import Notifier
from db import TimescaleDB
from analyst import Analyst
from news_filter import NewsFilter
from activity import push_activity, init_activity
from enum import Enum
from typing import List, Dict, Optional, Tuple

class TradingMode(str, Enum):
    TECHNICAL_ONLY = "technical_only"
    AI_OBSERVE_ONLY = "ai_observe_only"
    TECHNICAL_PLUS_AI = "technical_plus_ai"

class BudgetAllocator:
    def __init__(self, total_balance: float, alloc_cfg: dict, pair_count: int):
        reserve_pct = alloc_cfg.get("reserve_pct", 0.20)
        min_per_slot = alloc_cfg.get("min_per_slot", 50)
        max_budget_pct = alloc_cfg.get("max_budget_pct", 0.10)

        deployable = total_balance * (1 - reserve_pct)
        max_budget = max(deployable * max_budget_pct, min_per_slot)
        self.slots = min(pair_count, max(1, int(deployable / min_per_slot)))
        raw_budget = deployable / self.slots if self.slots > 0 else 0
        if self.slots == 1:
            self.budget_per_slot = round(deployable, 2)
        else:
            self.budget_per_slot = round(min(max_budget, raw_budget), 2)
        self.reserve = round(total_balance - (self.budget_per_slot * self.slots), 2)
        self.used = 0
        self._lock = asyncio.Lock()
        self._holders: set[str] = set()

    async def acquire(self, symbol: str = "") -> bool:
        async with self._lock:
            if self.used < self.slots:
                self.used += 1
                if symbol:
                    self._holders.add(symbol)
                return True
            return False

    async def release(self, symbol: str = ""):
        async with self._lock:
            self.used = max(0, self.used - 1)
            if symbol:
                self._holders.discard(symbol)

    def get_active_symbols(self) -> set[str]:
        return self._holders.copy()

    def remove_pair(self, symbol: str):
        if symbol in self._holders:
            self._holders.discard(symbol)
            self.used = max(0, self.used - 1)

    async def reconcile_used(self, used: int):
        async with self._lock:
            self.used = max(0, min(int(used), int(self.slots)))


def _regime_with_dir(ec: dict) -> str:
    r = ec.get("regime", "")
    if r == "trending":
        return "trending↑" if ec.get("trend_uptrend") else "trending↓"
    return r


class GridState:
    def __init__(self, symbol: str, config: dict):
        self.symbol = symbol
        self.pair_config = None
        for p in config["pairs"]:
            if p["name"] == symbol:
                self.pair_config = p
                break
        gc = self.pair_config["grid"] if self.pair_config else config["grid"]
        self.grid_type = config["grid"].get("type", "geometric")
        self.width = gc.get("width_percent", config["grid"]["default_width_percent"]) / 100
        self.count = gc.get("count", config["grid"]["default_count"])
        self.equity_pct = gc.get("equity_percent_per_level", config["grid"]["default_equity_percent_per_level"])
        self.levels: List[Dict] = []
        self.is_active = False
        self.last_rebalance = 0
        self.last_entry_attempt = 0
        self.fill_counts = {"buy": 0, "sell": 0}
        self.consecutive_losses = 0
        self.cooldown_until = 0.0
        self.continuation_losses = 0
        self.continuation_cooldown = 0.0
        self.breakeven_activated = False
        self.last_analyst_verdict: Optional[dict] = None
        self.sideway_wins = 0
        self.sideway_losses = 0
        self.trend_active = False
        self.trend_entry_price = 0.0
        self.trend_stop = 0.0
        self.trend_target = 0.0
        self.trend_size = 0.0
        self.trend_high = 0.0
        self._ai_size_mult = 1.0
        self.entry_adx = 0.0
        self.entry_rsi = 0.0
        self.entry_regime = ""
        self.atr = 0.0
        self.filled_cost = 0.0
        self.filled_qty = 0.0
        self.fill_lots: List[Dict] = []
        self.pair_budget = 0.0
        self.min_notional = 10.0
        self.slot_acquired = False
        self.processed_order_ids = set()
        self.bullets_fired = 0
        self.avg_entry_price = 0.0
        self.entry_type = ""
        self.trend_entry_pending = False
        self.trend_entry_order_id = ""
        self.trend_entry_client_id = ""
        self.trend_entry_started = 0.0
        self._ct_risk: Optional[dict] = None
        self.tranche1_sold: bool = False
        self._analyst_size_mult: float = 1.0
        self._news_size_mult: float = 1.0

class Executor:
    def __init__(self, config: dict, exchange: ExchangeWrapper, strategist: Strategist, notifier: Notifier):
        self.config = config
        self.exchange = exchange
        self.strategist = strategist
        self.notifier = notifier
        self.analyst: Optional[Analyst] = None
        self.news_filter: Optional[NewsFilter] = None
        self.db = TimescaleDB(config)
        self.db.connect()
        self.all_pairs = [p["name"] for p in config["pairs"] if p.get("enabled", True)]
        self.allocator: Optional[BudgetAllocator] = None
        self.pair_budget = 0.0
        self.states: Dict[str, GridState] = {}
        self._pair_tasks: Dict[str, asyncio.Task] = {}
        self.redis = None
        self._daily_loss_notified = False
        self._kill_in_progress = False
        execution_cfg = config.get("execution", {})
        self.client_id_prefix = execution_cfg.get("client_order_id_prefix", "vx")
        self.manage_only_bot_orders = execution_cfg.get("manage_only_bot_orders", True)
        self.post_only_grid = execution_cfg.get("post_only_grid", True)
        self.post_only_trend = execution_cfg.get("post_only_trend", False)
        self.cancel_bot_orders_on_start = execution_cfg.get("cancel_bot_orders_on_start", True)
        self.sweep_on_start = execution_cfg.get("sweep_on_start", False)
        timeout_cfg = execution_cfg.get("timeout", {})
        self.timeout = {
            "continuation": timeout_cfg.get("continuation", 90),
            "breakout": timeout_cfg.get("breakout", 90),
            "countertrend": timeout_cfg.get("countertrend", 300),
            "sideways": timeout_cfg.get("sideways", 600),
        }
        offset_cfg = execution_cfg.get("offset", {})
        self.offset = {
            "continuation": offset_cfg.get("continuation", 0.0003),
            "breakout": offset_cfg.get("breakout", 0.0003),
            "countertrend": offset_cfg.get("countertrend", 0.0),
            "sideways": offset_cfg.get("sideways", 0.0),
        }
        self.max_spread_pct = execution_cfg.get("max_spread_pct", 0.0015)
        self._order_seq = itertools.count(1)
        self._auto_profile_last = 0.0

        ap = config.get("auto_profile", {})
        self.auto_profile_enabled = bool(ap.get("enabled", False)) or self._env_bool("AUTO_PROFILE", False)
        self.auto_profile_interval = int(ap.get("interval_seconds", 1800))
        self.auto_profile_min_hold = int(ap.get("min_hold_seconds", 7200))
        self.auto_profile_flat_only = bool(ap.get("switch_only_when_flat", True))
        self._regime_mode: str = "auto"  # "normal", "auto", "countertrend"
        self._last_normal_trade: float = 0  # timestamp of last normal-mode entry
        self._pending_mode: Optional[str] = None  # queued mode switch during auto
        perf_cfg = config.get("performance_guard", {})
        self.performance_guard_enabled = bool(perf_cfg.get("enabled", True))
        self.performance_guard_lookback_days = int(perf_cfg.get("lookback_days", 30))
        self.performance_guard_min_trades = int(perf_cfg.get("min_trades", 20))
        self.performance_guard_top_n = int(perf_cfg.get("top_n_pairs", 6))
        self.performance_guard_min_win_rate = float(perf_cfg.get("min_win_rate", 0.18))
        self.performance_guard_min_net_pnl = float(perf_cfg.get("min_net_pnl", -1.0))
        self.performance_guard_pause_hours = float(perf_cfg.get("pause_hours", 24))
        self.performance_guard_cache_seconds = int(perf_cfg.get("cache_seconds", 900))
        self._pair_perf_cache: dict = {}
        self.entry_lock = asyncio.Lock()
        self._prev_regime: Dict[str, str] = {}
        self._last_rejection: Dict[str, Tuple[str, float]] = {}
        self._rejection_agg: Dict[str, int] = {}
        self._cycle_count = 0
        self._signal_count = 0
        self._reject_count = 0
        self._exec_count = 0
        tm = config.get("trading_mode", "ai_observe_only")
        self.trading_mode = TradingMode(tm) if tm in [m.value for m in TradingMode] else TradingMode.AI_OBSERVE_ONLY
        self._ai_would_have_blocked = 0
        self._ai_would_have_resized = 0
        self._last_analyst_regime: Dict[str, str] = {}
        self._last_analyst_time: Dict[str, float] = {}

    def _log(self, tag: str, msg: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}][{tag}] {msg}")

    def _env_bool(self, name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    def _log_slot_event(self, symbol: str, event: str, reason: str = ""):
        try:
            used = self.allocator.used if self.allocator else 0
            slots = self.allocator.slots if self.allocator else 0
            tag = f"{reason} ({used}/{slots})".strip()
            ec = self.strategist.entry_conditions.get(symbol, {})
            self.db.log_decision(symbol, event, tag,
                regime=ec.get("regime", ""),
                adx=ec.get("adx", 0),
                atr=ec.get("atr", 0),
                rsi=ec.get("rsi", 0),
                price=ec.get("last_price", 0) or ec.get("close", 0) or 0)
        except Exception:
            pass

    async def _acquire_slot(self, state: 'GridState', reason: str) -> bool:
        if not self.allocator:
            return True
        async with self.entry_lock:
            ok = await self.allocator.acquire(state.symbol)
            if ok:
                state.slot_acquired = True
                print(f"  [{state.symbol}] 🔒 Slot acquired for {reason}")
                self._log_slot_event(state.symbol, "SLOT_ACQUIRE", reason)
            else:
                print(f"  [{state.symbol}] gate: slot full ({self.allocator.used}/{self.allocator.slots}) for {reason}")
        return ok

    async def _release_slot(self, state: 'GridState', reason: str):
        if state.slot_acquired and self.allocator:
            try:
                await self.allocator.release(state.symbol)
            except Exception:
                pass
            state.slot_acquired = False
            self._log_slot_event(state.symbol, "SLOT_RELEASE", reason)

    async def _trend_preflight(self, state: GridState, reason: str) -> tuple[bool, str]:
        """
        Quick checks before acquiring a slot for trend entries.
        Goal: avoid SLOT_ACQUIRE churn when the trade can't be placed anyway.
        """
        ec = self.strategist.entry_conditions.get(state.symbol, {})
        atr = float(ec.get("atr", 0) or 0)
        if atr <= 0:
            return False, "preflight_no_atr"
        # Start with strategist-computed entry, but be defensive: it can be 0 while data warms up.
        entry_price = float(self.strategist.get_trend_price(state.symbol) or 0)
        if entry_price <= 0:
            entry_price = float(ec.get("close", 0) or 0)
        try:
            # Prefer REST ticker for stability; ws `watch_ticker` is often unreliable on spot testnet.
            ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
            ask = float(ticker.get("ask") or 0)
            bid = float(ticker.get("bid") or 0)
            last = float(ticker.get("last") or 0)
            px = ask or bid or last or entry_price
            if px <= 0:
                raise ValueError("no_ticker_price")
            if ec.get("trend_breakout") or ec.get("regime") == "sideways":
                entry_price = px
            else:
                best_bid = bid or last or px
                entry_price = round(best_bid * 1.001, 8)
                ema_20 = float(ec.get("ema_20", 0) or 0)
                if ema_20 > 0 and entry_price > ema_20 * 1.01:
                    if state._ct_risk is not None:
                        entry_price = round(float(ema_20), 8)
                    else:
                        return False, "preflight_chase_blocked"
        except Exception:
            # Fallback to candle close if we have it; otherwise skip so we don't churn slots.
            if entry_price <= 0:
                entry_price = float(ec.get("close", 0) or 0)
            if entry_price <= 0:
                return False, "preflight_no_ticker"
        if entry_price <= 0:
            return False, "preflight_invalid_entry"

        simulated = os.getenv("SIMULATED_BALANCE")
        try:
            balance = await self.exchange.fetch_balance()
            usdt = float(balance["USDT"]["free"])
            if simulated:
                usdt = min(usdt, float(simulated))
        except Exception:
            usdt = float(simulated) if simulated else 0.0
        trend_cfg = self.config["strategy"].get("trend", {})
        risk_pct = float(trend_cfg.get("risk_percent", 2.0)) / 100
        trail_atr = float(self.strategist.get_profile_params(state.symbol).get("sl_atr", 2.0))
        risk_amount = min(usdt * risk_pct, state.pair_budget * 0.5)
        if risk_amount <= 0:
            return False, "preflight_no_usdt"
        size = round(risk_amount / (atr * trail_atr), 4)
        max_notional_5pct = usdt * 0.05
        size = min(size, max_notional_5pct / entry_price) if entry_price > 0 else size
        base_notional = size * entry_price
        if base_notional < 5:
            return False, f"preflight_too_small_${base_notional:.2f}"

        # For the common limit-buy path, validate precision/min-notional early.
        adx = float(ec.get("adx", 0) or 0)
        if adx <= 30:
            try:
                self.exchange.normalize_limit_order(state.symbol, size, entry_price)
            except Exception as e:
                return False, f"preflight_reject:{str(e)[:80]}"
        return True, "ok"

    async def _pair_performance_gate(self, symbol: str) -> tuple[bool, str]:
        if not self.performance_guard_enabled or not self.db:
            return True, "ok"
        now = time.time()
        cache = self._pair_perf_cache.get("rankings")
        if not cache or (now - cache.get("ts", 0)) > self.performance_guard_cache_seconds:
            rankings = await asyncio.to_thread(self.db.get_pair_performance_rankings, self.performance_guard_lookback_days)
            cache = {"ts": now, "rankings": rankings}
            self._pair_perf_cache["rankings"] = cache
        rankings = cache.get("rankings", [])
        if not rankings:
            return True, "ok"
        ranked = [r for r in rankings if int(r.get("trades", 0)) >= self.performance_guard_min_trades]
        if not ranked:
            return True, "ok"
        by_pair = {r["pair"]: r for r in ranked}
        current = by_pair.get(symbol)
        if not current:
            return True, "ok"
        rank = sorted(ranked, key=lambda r: (r["net_pnl"], r["win_rate"], r["trades"]), reverse=True)
        rank_index = next((i for i, r in enumerate(rank, start=1) if r["pair"] == symbol), None)
        if rank_index is None:
            return True, "ok"
        if rank_index <= self.performance_guard_top_n:
            return True, "ok"
        net_pnl = float(current.get("net_pnl", 0))
        win_rate = float(current.get("win_rate", 0))
        if net_pnl > 0 or win_rate >= self.performance_guard_min_win_rate:
            return True, "ok"
        pause_secs = int(self.performance_guard_pause_hours * 3600)
        return False, f"poor_recent_performance:rank{rank_index}_net{net_pnl:+.2f}_wr{win_rate:.2f}_tr{int(current.get('trades', 0))}_pause{pause_secs//3600}h"

    def _write_env_var(self, key: str, value: str):
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        try:
            if os.path.exists(env_path):
                with open(env_path, "r") as f:
                    lines = f.readlines()
            else:
                lines = []
            out = []
            found = False
            for line in lines:
                if line.startswith(f"{key}="):
                    out.append(f"{key}={value}\n")
                    found = True
                else:
                    out.append(line)
            if not found:
                out.append(f"{key}={value}\n")
            with open(env_path, "w") as f:
                f.writelines(out)
        except Exception as e:
            print(f"_write_env_var failed: {e}")

    def _choose_auto_profile(self) -> str:
        # Simple, deterministic selector using live regime mix.
        regimes = {"trending": 0, "sideways": 0, "high_vol": 0, "unknown": 0}
        for symbol in self.states:
            r = self.strategist.entry_conditions.get(symbol, {}).get("regime", "unknown")
            regimes[r] = regimes.get(r, 0) + 1
        total = max(1, sum(regimes.values()))
        trending_ratio = regimes.get("trending", 0) / total
        high_vol_ratio = regimes.get("high_vol", 0) / total
        sideways_ratio = regimes.get("sideways", 0) / total

        if high_vol_ratio >= 0.30:
            return "conservative"
        if trending_ratio >= 0.50:
            return "trend_only"
        if sideways_ratio >= 0.60:
            # Prefer scalper when market is mostly sideways (more turns).
            return "scalper"
        return "standard"

    async def _auto_profile_loop(self):
        if not self.auto_profile_enabled:
            return
        await asyncio.sleep(20)
        while True:
            try:
                await asyncio.sleep(max(60, self.auto_profile_interval))
                if self._kill_in_progress:
                    return
                if self.auto_profile_flat_only:
                    has_exposure = any(
                        st.is_active or st.trend_active or st.trend_entry_pending
                        for st in self.states.values()
                    )
                    if has_exposure:
                        continue
                now = asyncio.get_event_loop().time()
                if self._auto_profile_last and (now - self._auto_profile_last) < self.auto_profile_min_hold:
                    continue
                current = self.config.get("active_profile", "standard")
                chosen = self._choose_auto_profile()
                if chosen == current:
                    continue
                self._auto_profile_last = now
                await self.notifier.send_message(f"🔁 Auto-profile switching {current} → {chosen} (restart)")
                self._write_env_var("ACTIVE_PROFILE", chosen)
                await self.trigger_kill_switch()
                return
            except Exception as e:
                print(f"_auto_profile_loop error: {e}")

    def _client_order_id(self, symbol: str, role: str) -> str:
        clean_symbol = symbol.replace("/", "").replace("-", "").lower()[:10]
        clean_role = "".join(ch for ch in role.lower() if ch.isalnum())[:6]
        millis = int(datetime.now(timezone.utc).timestamp() * 1000) % 100000000
        return f"{self.client_id_prefix}{clean_symbol}{clean_role}{millis}{next(self._order_seq)}"[:36]

    def _order_client_id(self, order: dict) -> str:
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        return str(order.get("clientOrderId") or info.get("clientOrderId") or info.get("origClientOrderId") or "")

    def _is_bot_order(self, order: dict) -> bool:
        if not self.manage_only_bot_orders:
            return True
        return self._order_client_id(order).startswith(self.client_id_prefix)

    def _order_key(self, order: dict) -> str:
        return str(order.get("id") or self._order_client_id(order) or f"{order.get('timestamp')}:{order.get('side')}:{order.get('filled')}")

    def _order_avg_price(self, order: dict) -> float:
        avg = order.get("average")
        if avg:
            return float(avg)
        filled = float(order.get("filled") or order.get("amount") or 0)
        cost = float(order.get("cost") or 0)
        if filled > 0 and cost > 0:
            return cost / filled
        price = order.get("price")
        return float(price or 0)

    async def _connect_redis(self):
        if self.redis:
            try:
                await self.redis.ping()
                return
            except Exception:
                pass
        try:
            rc = self.config["redis"]
            url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
            self.redis = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
            await self.redis.ping()
        except Exception as e:
            print(f"_connect_redis: {e}")

    async def _save_snapshot(self, state: GridState, decision: str):
        await self._connect_redis()
        if not self.redis:
            return
        try:
            ec = self.strategist.entry_conditions.get(state.symbol, {})
            snap = {
                "ts": str(datetime.now(timezone.utc)),
                "symbol": state.symbol,
                "decision": decision,
                "regime": ec.get("regime", ""),
                "adx": round(ec.get("adx", 0), 2),
                "atr": round(ec.get("atr", 0), 2),
                "rsi": round(ec.get("rsi", 0), 1),
                "ema_20": round(ec.get("ema_20", 0), 2),
                "ema_50": round(ec.get("ema_50", 0), 2),
                "trend_pullback": ec.get("trend_pullback", False),
                "price_at_lower_bb": ec.get("price_at_lower_bb", False),
                "price_above_200_ema": ec.get("price_above_200_ema", False),
                "analyst_verdict": state.last_analyst_verdict.get("verdict", "") if state.last_analyst_verdict else "",
                "grid_type": state.grid_type,
                "grid_width": round(state.width * 100, 2),
                "grid_count": state.count,
                "atr_pct": ec.get("atr_pct", 0),
                "nudge": {
                    "width_mult": self.config["strategy"]["entry"].get("nudge", {}).get("low_vol_width_multiplier") 
                        if ec.get("regime") == "sideways" and ec.get("atr_pct", 0) > 0 
                        and ec.get("atr_pct", 0) < self.config["strategy"]["entry"].get("nudge", {}).get("low_vol_atr_pct_threshold", 0.003) 
                        else None,
                    "tp_mode": "tight" if ec.get("rsi", 50) > self.config["strategy"]["entry"].get("nudge", {}).get("rsi_extreme_threshold", 80) else None,
                },
            }
            key = f"vortex:snapshot:{state.symbol.replace('/', '_')}"
            await self.redis.setex(key, 604800, json.dumps(snap))
        except Exception:
            pass

    def _calc_fee(self, order: dict, filled: float, price: float, is_maker: bool = True) -> float:
        symbol = order.get("symbol", "") if order else ""
        quote = symbol.split("/")[-1] if "/" in symbol else "USDT"
        base = symbol.split("/")[0] if "/" in symbol else ""
        fees = []
        if order:
            if order.get("fees"):
                fees.extend(order.get("fees") or [])
            elif order.get("fee"):
                fees.append(order["fee"])
        if fees:
            total = 0.0
            usable = False
            for fee in fees:
                if not fee or fee.get("cost") is None:
                    continue
                currency = fee.get("currency")
                cost = float(fee["cost"])
                if currency == quote or not currency:
                    total += cost
                    usable = True
                elif currency == base:
                    total += cost * price
                    usable = True
            if usable and total > 0:
                return total
        rate = self.config["fees"]["maker"] if is_maker else self.config["fees"]["taker"]
        return filled * price * rate

    def _add_inventory_lot(self, state: GridState, qty: float, cost: float):
        if qty <= 0 or cost <= 0:
            return
        state.fill_lots.append({"qty": qty, "cost": cost})
        state.filled_qty += qty
        state.filled_cost += cost

    def _consume_inventory(self, state: GridState, qty: float, fallback_price: float) -> float:
        remaining = max(0.0, qty)
        consumed_cost = 0.0
        while remaining > 1e-12 and state.fill_lots:
            lot = state.fill_lots[0]
            lot_qty = float(lot["qty"])
            take = min(remaining, lot_qty)
            unit_cost = float(lot["cost"]) / lot_qty if lot_qty > 0 else fallback_price
            consumed_cost += take * unit_cost
            lot["qty"] = lot_qty - take
            lot["cost"] = max(0.0, float(lot["cost"]) - take * unit_cost)
            remaining -= take
            if lot["qty"] <= 1e-12:
                state.fill_lots.pop(0)
        if remaining > 1e-12:
            avg_cost = (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else fallback_price
            consumed_cost += remaining * avg_cost
        state.filled_qty = max(0.0, state.filled_qty - qty)
        state.filled_cost = max(0.0, state.filled_cost - consumed_cost)
        return consumed_cost

    def _dynamic_depth(self, state: GridState) -> int:
        ec = self.strategist.entry_conditions.get(state.symbol, {})
        profile = self.config.get("active_profile", "standard")
        p = self.config.get("profiles", {}).get(profile, {})
        profile_max = p.get("grid", {}).get("profile_max_levels", state.count)

        regime = ec.get("regime", "unknown")
        adx = ec.get("adx", 0)
        adx_slope = ec.get("adx_slope", 0)
        rvol = ec.get("rvol", 1.0)
        candle_eff = ec.get("candle_eff", 0.5)

        if regime == "unknown":
            return 0
        if regime == "high_vol":
            return 0
        # Skip rvol/candle_eff check when price is at lower BB (strong entry signal)
        bb_lower = ec.get("price_at_lower_bb", False)
        if not bb_lower and (rvol < 0.5 or candle_eff < 0.3):
            return 0

        if regime == "trending" and adx >= 30:
            depth = 1
        elif regime == "trending" and adx >= 25 and adx_slope > 0:
            depth = max(1, profile_max // 4)
        elif regime == "trending":
            depth = max(1, profile_max // 2)
        elif regime == "sideways" and (adx > 18 or candle_eff < 0.4):
            depth = max(1, profile_max // 2)
        else:
            depth = profile_max

        budget_buys = int(state.pair_budget / 10) if state.pair_budget > 0 else 0
        budget_cap = budget_buys * 2
        return min(depth, budget_cap, profile_max)

    async def _check_auto_regime(self):
        """Auto-switch between normal and countertrend mode based on market conditions."""
        mode_cfg = self.config.get("safety", {}).get("regime_mode", "auto")
        if mode_cfg != "auto":
            self._regime_mode = mode_cfg
            self.config["safety"]["panic_revert_to_safe_mode"] = (mode_cfg == "normal")
            return

        now = asyncio.get_event_loop().time()
        has_position = any(st.is_active or st.trend_active for st in self.states.values())
        any_entry_attempted = any(st.last_entry_attempt > 0 for st in self.states.values())

        # Check if any pair has a good sideways signal (ADX < 20 + at lower BB)
        sideways_signal = False
        for symbol, st in self.states.items():
            ec = self.strategist.entry_conditions.get(symbol, {})
            regime = ec.get("regime", "")
            adx = ec.get("adx", 0)
            bb = ec.get("price_at_lower_bb", False)
            if regime == "sideways" and adx < 20 and bb and ec.get("rvol", 1) > 0.3:
                sideways_signal = True
                break

        if self._regime_mode == "normal":
            # Switch to countertrend if no trade in 60 minutes
            idle = any(now - st.last_entry_attempt > 3600 for st in self.states.values())
            if idle and not has_position:
                self._regime_mode = "countertrend"
                self.config["safety"]["panic_revert_to_safe_mode"] = False
                print(f"  ⏰ Auto: switching to countertrend (no entry for 1h)")
                await push_activity("⏰ Auto: switching to countertrend (no entry for 1h)", "info")

        elif self._regime_mode == "countertrend":
            # Switch back to normal when strong sideways signal appears
            if sideways_signal:
                if has_position:
                    self._pending_mode = "normal"  # wait for position to close
                else:
                    self._regime_mode = "normal"
                    self.config["safety"]["panic_revert_to_safe_mode"] = True
                    print(f"  📊 Auto: switching to normal (sideways setup)")
                    await push_activity("📊 Auto: switching to normal (sideways setup)", "info")

        # Process pending mode switch when position closes
        if self._pending_mode and not has_position:
            self._regime_mode = self._pending_mode
            self._pending_mode = None
            self.config["safety"]["panic_revert_to_safe_mode"] = (self._regime_mode == "normal")
            print(f"  Auto: position closed, switching to {self._regime_mode}")
            await push_activity(f"Auto: position closed, switching to {self._regime_mode}", "info")

    async def _check_filter_override(self, filter_name: str) -> bool:
        await self._connect_redis()
        if not self.redis:
            return False
        try:
            return await self.redis.exists(f"vortex:filter:override:{filter_name}") == 1
        except Exception:
            return False

    async def _publish_conditions(self):
        await self._connect_redis()
        if not self.redis:
            return
        try:
            data = {}
            for symbol, st in self.states.items():
                ec = self.strategist.entry_conditions.get(symbol, {})
                tf = self.strategist.timeframes["entry"]
                df = self.strategist.data.get(symbol, {}).get(tf)
                change = 0.0
                if df is not None and len(df) > 288:
                    close_now = float(df.iloc[-1]["close"])
                    close_24h = float(df.iloc[-288]["close"])
                    change = round(((close_now - close_24h) / close_24h) * 100, 2)
                data[symbol] = {
                    "regime": ec.get("regime", "unknown"),
                    "adx": ec.get("adx", 0),
                    "adx_slope": ec.get("adx_slope", 0),
                    "rsi": ec.get("rsi", 0),
                    "atr": ec.get("atr", 0),
                    "rvol": ec.get("rvol", 1),
                    "candle_eff": ec.get("candle_eff", 0.5),
                    "ema_20": ec.get("ema_20", 0),
                    "ema_50": ec.get("ema_50", 0),
                    "trend_uptrend": bool(ec.get("trend_uptrend", False)),
                    "trend_pullback": ec.get("trend_pullback", False),
                    "bb_lower": float(ec.get("bb_lower", 0)),
                    "above_ema200": bool(ec.get("price_above_200_ema", False)),
                    "trend_active": st.trend_active,
                    "trend_entry_pending": st.trend_entry_pending,
                    "trend_entry": st.trend_entry_price,
                    "trend_stop": st.trend_stop,
                    "trend_target": st.trend_target,
                    "trend_pnl": round((ec.get("atr", 0) * 0), 2),
                    "analyst_verdict": st.last_analyst_verdict.get("verdict", "") if st.last_analyst_verdict else "",
                    "analyst_confidence": st.last_analyst_verdict.get("confidence", 0) if st.last_analyst_verdict else 0,
                    "analyst_reason": st.last_analyst_verdict.get("reason", "") if st.last_analyst_verdict else "",
                    "countertrend_mode": not self.config.get("safety", {}).get("panic_revert_to_safe_mode", False) and st.symbol in getattr(self.strategist, 'PILOT_PAIRS', []) and self.strategist.should_exit_trend_inversion(st.symbol),
                    "countertrend_active": not self.config.get("safety", {}).get("panic_revert_to_safe_mode", False) and st._ct_risk is not None and st.trend_active,
                    "entry_type": st.entry_type,
                    "change": change,
                }
            cleaned = json.loads(json.dumps(data, default=lambda x: float(x) if hasattr(x, 'item') else str(x)))
            cleaned["_meta"] = {
                "regime_mode": self._regime_mode,
                "breakout": self.strategist.allow_breakout_override is True,
                "trading_mode": self.trading_mode.value,
            }
            cleaned["_stats"] = {
                "cycles": self._cycle_count,
                "signals": self._signal_count,
                "rejected": self._reject_count,
                "executed": self._exec_count,
                "ai_would_have_blocked": self._ai_would_have_blocked,
                "ai_would_have_resized": self._ai_would_have_resized,
            }
            await self.redis.set("vortex:conditions", json.dumps(cleaned))
            await self.redis.expire("vortex:conditions", 30)
        except Exception as e:
            print(f"_publish_conditions error: {e}")

    async def _fetch_fear_greed(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.alternative.me/fng/?limit=1", timeout=5) as resp:
                    data = await resp.json()
                    if data and "data" in data and len(data["data"]) > 0:
                        await self._connect_redis()
                        if self.redis:
                            await self.redis.setex("vortex:fear_greed", 3600, json.dumps({
                                "value": int(data["data"][0]["value"]),
                                "classification": data["data"][0]["value_classification"],
                            }))
        except Exception as e:
            print(f"_fetch_fear_greed: {e}")

    async def _record_balance(self):
        await self._connect_redis()
        if not self.redis:
            return
        try:
            simulated = os.getenv("SIMULATED_BALANCE")
            if simulated:
                total_usd = float(simulated)
                try:
                    with self.db.conn.cursor() as cur:
                        cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
                        total_usd += float(cur.fetchone()[0])
                except Exception:
                    pass
                total_usd = round(total_usd, 2)
                usdt_free = total_usd
                usdt_used = 0
                holdings = []
                try:
                    bal = await self.exchange.fetch_balance()
                    tracked_bases = {p["name"].split("/")[0] for p in self.config["pairs"] if p.get("enabled", True)}
                    for key, val in bal.items():
                        if key in ("USDT", "info", "free", "used", "total", "timestamp"):
                            continue
                        if not isinstance(val, dict):
                            continue
                        if key not in tracked_bases:
                            continue
                        qty = float(val.get("free", 0)) + float(val.get("used", 0))
                        if qty <= 0:
                            continue
                        try:
                            ticker = await asyncio.wait_for(self.exchange.fetch_ticker(f"{key}/USDT"), timeout=5)
                            price = float(ticker["last"])
                            value = round(qty * price, 2)
                            holdings.append({"asset": key, "qty": round(qty, 6), "price": price, "value": value})
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                bal = await self.exchange.fetch_balance()
                usdt_free = float(bal["USDT"]["free"]) if "USDT" in bal else 0
                usdt_used = float(bal["USDT"].get("used", 0)) if isinstance(bal.get("USDT"), dict) else 0
                total_usd = usdt_free + usdt_used
                holdings = []
                tracked_bases = {p["name"].split("/")[0] for p in self.config["pairs"] if p.get("enabled", True)}
                for key, val in bal.items():
                    if key in ("USDT", "info", "free", "used", "total", "timestamp"):
                        continue
                    if not isinstance(val, dict):
                        continue
                    if key not in tracked_bases:
                        continue
                    qty = float(val.get("free", 0)) + float(val.get("used", 0))
                    if qty <= 0:
                        continue
                    try:
                        ticker = await asyncio.wait_for(self.exchange.fetch_ticker(f"{key}/USDT"), timeout=5)
                        price = float(ticker["last"])
                        value = round(qty * price, 2)
                        holdings.append({"asset": key, "qty": round(qty, 6), "price": price, "value": value})
                        total_usd += value
                    except Exception:
                        pass
            total_usd = round(total_usd, 2)
            if simulated:
                init_from_sim = round(float(simulated), 2)
                await self.redis.set("vortex:balance:initial", str(init_from_sim))
                await self.redis.set("vortex:balance:initial_time", str(datetime.now(timezone.utc)))
            elif not await self.redis.exists("vortex:balance:initial"):
                await self.redis.set("vortex:balance:initial", str(total_usd))
                await self.redis.set("vortex:balance:initial_time", str(datetime.now(timezone.utc)))
            await self.redis.set("vortex:balance:current", str(total_usd))
            await self.redis.set("vortex:balance:holdings", json.dumps(holdings))
            await self.redis.set("vortex:balance:usdt_free", str(round(usdt_free, 2)))
            await self.redis.set("vortex:balance:usdt_used", str(round(usdt_used, 2)))
            await self.redis.set("vortex:balance:time", str(datetime.now(timezone.utc)))
            self.db.log_balance_snapshot(round(total_usd, 2), round(total_usd, 2))
        except Exception as e:
            print(f"_record_balance error: {e}")

    async def _sweep_leftover_coins(self):
        CHUNK_USD = 5.0
        try:
            balance = await self.exchange.fetch_balance()
            for symbol in self.states:
                base = symbol.split("/")[0]
                free = float(balance.get(base, {}).get("free", 0))
                if free <= 0:
                    continue
                st = self.states.get(symbol)
                if st and (st.trend_active or st.is_active or st.trend_entry_pending):
                    continue
                try:
                    ticker = await asyncio.wait_for(self.exchange.fetch_ticker(symbol), timeout=5)
                    price = float(ticker["last"])
                    min_val = self.exchange.get_min_notional(symbol)
                    total_val = free * price
                    if total_val < min_val:
                        print(f"  Sweep skip {base}: ${total_val:.2f} < min ${min_val:.2f}")
                        await push_activity(f"Sweep skip {base}: ${total_val:.2f} below min")
                        continue

                    async def _sell(qty) -> bool:
                        try:
                            # Normalize amount early so market sells won't be rejected on precision.
                            qty_norm = float(self.exchange.amount_to_precision(symbol, qty))
                            order = await self.exchange.create_market_sell_order(symbol, qty_norm)
                            # Confirm the order actually closed; testnet can return "open" briefly.
                            oid = order.get("id")
                            status = (order.get("status") or "").lower()
                            if oid and status not in ("closed", "filled"):
                                for _ in range(10):
                                    await asyncio.sleep(0.5)
                                    try:
                                        o2 = await self.exchange.fetch_order(oid, symbol)
                                        status = (o2.get("status") or "").lower()
                                        if status in ("closed", "filled"):
                                            order = o2
                                            break
                                    except Exception:
                                        pass
                            fee = self._calc_fee(order, qty_norm, price, is_maker=False)
                            self.db.log_trade({
                                "timestamp": datetime.now(timezone.utc), "pair": symbol,
                                "side": "sell", "price": price, "quantity": qty_norm,
                                "order_id": order.get("id"), "status": "closed",
                                "grid_level": None, "realized_pnl": round(-fee, 2), "fee_cost": fee,
                            })
                            print(f"  Swept {qty_norm:.4f} {base} @ ${price:.2f}")
                            return True
                        except Exception as e:
                            print(f"  Sweep sell failed {base} {qty:.4f}: {e}")
                            await push_activity(f"Sweep sell failed: {base} {e}", "error")
                            return False

                    if await _sell(free):
                        continue

                    remaining = free
                    chunk_qty = CHUNK_USD / price
                    while remaining > chunk_qty * 0.5:
                        qty = min(remaining, chunk_qty)
                        remaining -= qty
                        if not await _sell(qty):
                            remaining += qty
                            break
                    if remaining > 1e-8:
                        await _sell(remaining)
                except Exception as e:
                    print(f"  Sweep sell failed {symbol}: {e}")
                    await push_activity(f"Sweep error: {symbol} {e}", "error")
        except Exception as e:
            print(f"_sweep_leftover_coins error: {e}")
        # Refresh dashboard balance immediately after sweeping so "Coins held" doesn't lag for an hour.
        try:
            await self._record_balance()
        except Exception:
            pass

    async def _publish_orders(self):
        await self._connect_redis()
        if not self.redis:
            return
        data = {}
        for symbol, st in self.states.items():
            orders = []
            for level in st.levels:
                orders.append({"side": level["type"], "price": level["price"], "level": level["level"]})
            dyn = self._dynamic_depth(st)
            data[symbol] = {
                "is_active": st.is_active,
                "orders": orders,
                "dynamic_levels": dyn,
                "trend_active": st.trend_active,
                "trend_entry_pending": getattr(st, "trend_entry_pending", False),
                "trend_entry": getattr(st, "trend_entry_price", 0),
                "trend_stop": getattr(st, "trend_stop", 0),
                "trend_target": getattr(st, "trend_target", 0),
                "trend_size": getattr(st, "trend_size", 0),
                "entry_type": st.entry_type,
            }
        try:
            await self.redis.set("vortex:grid_state", json.dumps(data))
        except Exception:
            pass
        if self.allocator:
            try:
                holders = [sym for sym, st in self.states.items() if st.slot_acquired]
                # Prevent allocator drift: keep used in sync with actual slot holders.
                await self.allocator.reconcile_used(len(holders))
                await self.redis.setex("vortex:allocator", 3600, json.dumps({
                    "slots": self.allocator.slots,
                    "used": self.allocator.used,
                    "budget_per_slot": self.allocator.budget_per_slot,
                    "reserve": self.allocator.reserve,
                    "holders": holders,
                }))
            except Exception:
                pass

    async def _check_daily_loss(self) -> bool:
        if self._kill_in_progress:
            return True
        await self._connect_redis()
        daily_pnl = self.db.get_daily_pnl()
        effective_daily_pnl = daily_pnl
        if self.redis:
            try:
                if await self.redis.exists("vortex:loss_limit_hit"):
                    max_loss_pct = self.config["risk"].get("max_daily_loss_percent", 5)
                    initial = float(os.getenv("SIMULATED_BALANCE", "250"))
                    max_loss_val = initial * (max_loss_pct / 100) if initial > 0 else 0
                    reset_at = await self.redis.get("vortex:daily_loss_reset_at")
                    reset_pnl_raw = await self.redis.get("vortex:daily_loss_reset_pnl")
                    today_utc = datetime.now(timezone.utc).date().isoformat()
                    if reset_at == today_utc and reset_pnl_raw is not None:
                        try:
                            reset_pnl = float(reset_pnl_raw)
                            effective_daily_pnl = daily_pnl - reset_pnl
                        except Exception:
                            effective_daily_pnl = daily_pnl
                    if max_loss_val > 0 and effective_daily_pnl < 0 and abs(effective_daily_pnl) >= max_loss_val:
                        return True
                    await self.redis.delete("vortex:loss_limit_hit")
            except Exception:
                pass
        max_loss_override = await self.redis.get("vortex:max_daily_loss") if self.redis else None
        reset_at = await self.redis.get("vortex:daily_loss_reset_at") if self.redis else None
        if max_loss_override and reset_at == datetime.now(timezone.utc).date().isoformat():
            max_loss = float(max_loss_override)
            limit_label = f"${max_loss:.0f}"
        else:
            max_loss_pct = self.config["risk"].get("max_daily_loss_percent", 5)
            initial = float(os.getenv("SIMULATED_BALANCE", "250"))
            max_loss = initial * (max_loss_pct / 100) if initial > 0 else 0
            limit_label = f"{max_loss_pct}%"
        reset_at = await self.redis.get("vortex:daily_loss_reset_at") if self.redis else None
        reset_pnl_raw = await self.redis.get("vortex:daily_loss_reset_pnl") if self.redis else None
        if reset_at == datetime.now(timezone.utc).date().isoformat() and reset_pnl_raw is not None:
            try:
                effective_daily_pnl = daily_pnl - float(reset_pnl_raw)
            except Exception:
                effective_daily_pnl = daily_pnl
        if max_loss > 0 and effective_daily_pnl < 0 and abs(effective_daily_pnl) >= max_loss:
            if not self._daily_loss_notified:
                await self.notifier.send_message(f"🚨 Daily loss limit ({limit_label}) hit: ${effective_daily_pnl:.2f}")
                self._daily_loss_notified = True
            await self.trigger_kill_switch()
            return True
        return False

    async def _check_budget_depleted(self):
        try:
            sim = float(os.getenv("SIMULATED_BALANCE", "250"))
            remaining = await self.redis.get("vortex:budget_remaining") if self.redis else None
            if remaining is None:
                return
            remaining = float(remaining)
            pct = remaining / sim * 100 if sim > 0 else 0
            if remaining < 10:
                await self.notifier.send_message(f"🚨 Budget depleted (${remaining:.2f}). Send /refill to continue")
                await push_activity(f"🚨 Budget depleted — /refill to continue", "error")
            elif pct < 30:
                await push_activity(f"⚠️ Budget: ${remaining:.2f} / ${sim:.2f} ({pct:.0f}%) — /refill to refill", "warn")
        except Exception as e:
            print(f"_check_budget_depleted: {e}")

    async def _ai_veto(self, symbol: str, strategy: str, ec: dict, regime: str) -> str:
        """Call Groq AI to veto/reduce/approve a trade signal. Returns APPROVE, REDUCE, or VETO."""
        groq_key = os.getenv("GROQ_API_KEY", "")
        if not groq_key:
            return "APPROVE"
        try:
            enabled = await self.redis.get("vortex:feature:ai_veto") if self.redis else b"1"
            if enabled == b"0":
                return "APPROVE"
        except Exception:
            pass
        recent = []
        try:
            rows = self.db.get_recent_decisions(symbol, limit=10)
            counts = {"WIN": 0, "LOSS": 0}
            for r in rows[:5]:
                outcome = "WIN" if r.get("outcome", 0) > 0 else "LOSS" if r.get("outcome", 0) < 0 else "SCRATCH"
                recent.append(outcome)
                if outcome in counts:
                    counts[outcome] += 1
        except Exception:
            pass
        streak = ""
        for i, r in enumerate(recent):
            if i > 0 and r != recent[0]:
                streak = f"{len([x for x in recent if x == recent[0]])}x {recent[0]} streak"
                break
        adx = ec.get("adx", 0)
        rsi = ec.get("rsi", 50)
        hour = datetime.now(timezone.utc).hour
        recent_seq = ' '.join(recent) if recent else 'N/A'
        streak_txt = f"\nStreak: {streak}" if streak else ""
        fg_str = ""
        try:
            fg_raw = await self.redis.get("vortex:fear_greed") if self.redis else None
            if fg_raw:
                fg = json.loads(fg_raw)
                fg_str = f"\nFear & Greed: {fg['value']} ({fg['classification']})"
        except Exception:
            pass
        prompt = (
            "You are a Risk Management Oracle for a quantitative crypto execution engine.\n"
            "Your objective is to filter a proposed algorithmic trade to maximize expectancy and protect capital.\n\n"
            f"[TRADE THESIS]\n"
            f"Pair: {symbol}\n"
            f"Direction: LONG\n"
            f"Strategy: {strategy}\n"
            f"Regime: {regime}\n"
            f"Macro Time: {hour}:00 UTC\n\n"
            f"[CURRENT MARKET STATE]\n"
             f"ADX (14): {adx:.1f}\n"
             f"RSI (14): {rsi:.1f}\n"
             f"Recent Trade Sequence: {recent_seq}{streak_txt}{fg_str}\n\n"
             f"[EXECUTION RULES]\n"
             f"1. Momentum Alignment: ADX > 25 confirms a trend. However, ensure RSI aligns with the trade direction — RSI < 45 is poor for long breakouts.\n"
             f"2. Drawdown Protection: A low recent win rate indicates micro-structural chop or strategy desync. Capital preservation is the highest priority.\n"
             f"3. Volume Context: The current hour may have unique liquidity characteristics.\n"
             f"4. Quality Filter: Only approve trades with strong confluence — weak signals (BB near 2% entry with RSI barely under 35) should be reduced or vetoed.\n\n"
            f"[OUTPUT INSTRUCTIONS]\n"
            f"Step 1: Perform a brief quantitative analysis inside <scratchpad> tags.\n"
            f"Step 2: The very last line of your response must be EXACTLY ONE WORD.\n"
            f"Choose from: APPROVE, REDUCE, VETO"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": [
                            {"role": "system", "content": "You are a quantitative risk management filter for algorithmic trading. Your decisions are final and binding."},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.1,
                        "max_tokens": 300,
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    text = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip().upper()
                    lines = [l.strip() for l in text.split('\n') if l.strip()]
                    final_word = lines[-1] if lines else ""
                    for word in ("VETO", "REDUCE", "APPROVE"):
                        if word in final_word:
                            await push_activity(
                                f"🤖 AI {word} {symbol.split('/')[0]} {strategy}: ADX {adx:.0f} RSI {rsi:.0f} {regime}",
                                "ai"
                            )
                            return word
        except Exception as e:
            self._log("ERROR", f"{symbol} AI veto failed: {e}")
        return "APPROVE"

    async def _get_initial_balance(self) -> float:
        await self._connect_redis()
        if self.redis:
            try:
                v = await self.redis.get("vortex:balance:initial")
                return float(v) if v else 0
            except Exception:
                pass
        return 0

    async def _log_balance(self):
        try:
            bal = await self.exchange.fetch_balance()
            usdt = float(bal["USDT"]["free"]) if "USDT" in bal else 0
            self.db.log_balance_snapshot(usdt, usdt)
        except Exception:
            pass

    async def _check_sideway_entry(self, symbol: str, ec: dict, ep_cfg: dict) -> str:
        ep = ep_cfg if ep_cfg else {}
        rsi = ec.get("rsi", 50)
        rvol = ec.get("rvol", 0)
        atr_pct = ec.get("atr_pct", 0)
        adx = ec.get("adx", 0)
        last_close = ec.get("close", 0)
        previous_close = ec.get("close_prev", 0) if ec.get("close_prev") else last_close
        if not last_close: return ""
        tf = self.strategist.timeframes.get("entry", "4h")
        df = self.strategist.data.get(symbol, {}).get(tf)
        if df is None or len(df) < 25: return ""

        # BB squeeze + confluence
        if ep.get("bb_squeeze", False):
            has_bb = all(c in df.columns for c in ("bb_upper", "bb_lower", "bb_middle"))
            if has_bb:
                bb_w = (df["bb_upper"] - df["bb_lower"]) / df["bb_middle"].clip(lower=1)
                cur_w = float(bb_w.iloc[-1])
                min_w = float(bb_w.iloc[-20:-1].min())
                expanding = cur_w > min_w * 1.12
                bb_u = float(df["bb_upper"].iloc[-1])
                bb_l = float(df["bb_lower"].iloc[-1])
                near_upper = last_close >= bb_u * 0.99
                near_lower = last_close <= bb_l * 1.01
                rsi_rising = rsi > float(df["rsi"].iloc[-3]) if "rsi" in df.columns and len(df) >= 3 else False
                price_falling = last_close < float(df["close"].iloc[-3]) if len(df) >= 3 else False
                bullish_div = price_falling and rsi_rising

                # Moderate path: BB squeeze expansion + volume + trend
                exit_1h = self.strategist.exit_conditions.get(symbol, {})
                bearish_1h = exit_1h.get("price_below_200_ema_1h", False)

                if expanding and rvol > 1.5:
                    if near_upper: return "bb_squeeze"
                    if near_lower or not bearish_1h: return "bb_squeeze"

                if expanding and rvol > 1.2 and adx > 25 and not bearish_1h:
                    return "bb_squeeze"

        # trend_bounce: buy pullback to lower BB within uptrend
        if ep.get("trend_bounce", False):
            above_200 = ec.get("price_above_200_ema", False)
            if above_200:
                near_lower = last_close <= bb_l * 1.02 and last_close >= bb_l * 0.97
                if near_lower and rsi < 45 and rvol > 0.3:
                    return "trend_bounce"

        # scalping_5m: quick oversold bounces on 5m timeframe
        if ep.get("scalping_5m", False):
            df_5m = self.strategist.data.get(symbol, {}).get("5m")
            if df_5m is not None and len(df_5m) >= 50 and "bb_lower" in df_5m.columns and "rsi" in df_5m.columns:
                try:
                    c5 = float(df_5m["close"].iloc[-1])
                    bl = float(df_5m["bb_lower"].iloc[-1])
                    rsi5 = float(df_5m["rsi"].iloc[-1])
                    rsi5_prev = float(df_5m["rsi"].iloc[-2]) if len(df_5m) >= 2 else rsi5
                    near_bb = c5 <= bl * 1.02
                    oversold = rsi5 < 35
                    recovering = rsi5 > rsi5_prev
                    # Original tight conditions: RSI<35, BB 0.5% (for pairs like IMX)
                    if ep.get("scalp_original", False):
                        if c5 <= bl * 1.005 and rsi5 < 35 and recovering:
                            return "scalp_original"
                    # Current widened conditions
                    if near_bb and oversold and recovering:
                        return "scalping_5m"
                    print(f"[scalp] {symbol}: c5={c5:.4f} bl={bl:.4f} near_bb={near_bb} rsi5={rsi5:.1f} oversold={oversold} prev={rsi5_prev:.1f} recovering={recovering}")
                except (IndexError, ValueError, TypeError):
                    pass

        # lowvol_scalp: ATR < 0.5%, near EMA50, RSI 25-65 (unchanged)
        if ep.get("lowvol_scalp", False):
            ema50 = float(df.iloc[-1].get("ema_50", 0)) if "ema_50" in df.columns else 0
            if ema50 > 0:
                near_ema = abs(last_close - ema50) / ema50 * 100 < 1.0
                if near_ema and atr_pct and atr_pct < 0.5 and 25 <= rsi <= 65 and rvol > 0.1:
                    return "lowvol_scalp"

        # lowvol_momentum: low volatility + uptrend + green candle (69% WR on 4h)
        if ep.get("lowvol_momentum", False):
            ema50 = float(df.iloc[-1].get("ema_50", 0)) if "ema_50" in df.columns else 0
            if ema50 > 0:
                above_50 = last_close > ema50
                low_vol = atr_pct and atr_pct < 0.3
                green = last_close > previous_close
                if above_50 and low_vol and green:
                    return "lowvol_momentum"

        # supertrend: ATR-based trend flip in bull market
        if ep.get("supertrend", False):
            if "supertrend" in df.columns:
                st_val = float(df["supertrend"].iloc[-1])
                st_prev = float(df["supertrend"].iloc[-2]) if len(df) >= 2 else st_val
                above_200 = ec.get("price_above_200_ema", False)
                if st_val == 1 and st_prev == -1 and above_200:
                    return "supertrend"

        # vwap_revert: buy when price below VWAP + RSI oversold (sideways mean reversion)
        if ep.get("vwap_revert", False):
            hour = datetime.now(timezone.utc).hour
            if 4 <= hour <= 6:
                return ""
            if "vwap" in df.columns and "vwap_lower" in df.columns:
                vwap = float(df["vwap"].iloc[-1])
                vwap_lower = float(df["vwap_lower"].iloc[-1])
                if vwap > 0 and last_close <= vwap and rsi < 40:
                    return "vwap_revert"

        return ""

    async def _reset_simulation(self):
        print("  🛡️ _reset_simulation called — blocked to preserve trade history")
        await push_activity("_reset_simulation blocked — trade history preserved")
        try:
            await self.notifier.send_message(f"🔄 Simulation balance changed — resetting state: {msg}")
        except Exception:
            pass

    def set_analyst(self, analyst: Analyst):
        analyst.db = self.db
        self.analyst = analyst

    async def calculate_grid_levels(self, state: GridState, center_price: float) -> List[Dict]:
        dynamic_count = self._dynamic_depth(state)
        if dynamic_count < 2:
            return []
        buy_count = dynamic_count // 2
        sell_count = dynamic_count // 2
        width = state.width
        ec = self.strategist.entry_conditions.get(state.symbol, {})
        if ec.get("regime") == "sideways":
            atr_pct = ec.get("atr_pct", 0)
            threshold = self.config["strategy"]["entry"].get("nudge", {}).get("low_vol_atr_pct_threshold", 0.003)
            mult = self.config["strategy"]["entry"].get("nudge", {}).get("low_vol_width_multiplier", 1.25)
            if atr_pct > 0 and atr_pct < threshold:
                width = round(width * mult, 4)
        atr = ec.get("atr", 0)
        if atr > 0 and center_price > 0:
            atr_pct_grid = atr / center_price
            min_width = round(atr_pct_grid * 0.5, 6)
            if width < min_width:
                width = min_width
        levels = []
        if state.grid_type == "arithmetic":
            step = center_price * width
            for i in range(1, buy_count + 1):
                levels.append({"type": "buy", "price": round(center_price - step * i, 4), "level": -i, "placed": False})
            for i in range(1, sell_count + 1):
                levels.append({"type": "sell", "price": round(center_price + step * i, 4), "level": i, "placed": False})
        else:
            for i in range(1, buy_count + 1):
                levels.append({"type": "buy", "price": round(center_price * ((1 - width) ** i), 4), "level": -i, "placed": False})
            for i in range(1, sell_count + 1):
                levels.append({"type": "sell", "price": round(center_price * ((1 + width) ** i), 4), "level": i, "placed": False})
        return sorted(levels, key=lambda x: x["price"])

    async def place_grid_orders(self, state: GridState, levels: List[Dict]):
        maker_fee = self.config["fees"]["maker"]
        gross_per_flip = state.width
        net_per_flip = gross_per_flip - 2 * maker_fee
        profile_name = self.config.get("active_profile", "standard")
        profile_risk = self.config.get("profiles", {}).get(profile_name, {}).get("risk", {})
        min_net = float(profile_risk.get("min_net_profit_percent", self.config.get("risk", {}).get("min_net_profit_percent", 0.25))) / 100
        if net_per_flip < min_net:
            await self.notifier.send_message(
                f"⛔ {state.symbol} blocked: {gross_per_flip*100:.2f}% width "
                f"- {2*maker_fee*100:.2f}% fees = {net_per_flip*100:.2f}% net "
                f"< {min_net*100:.2f}% minimum"
            )
            if state.slot_acquired and self.allocator:
                await self.allocator.release(state.symbol)
                state.slot_acquired = False
            return
        balance = await self.exchange.fetch_balance()
        base = state.symbol.split("/")[0]
        usdt_balance = state.pair_budget
        base_balance = balance.get(base, {}).get("free", 0)
        buys = sorted([l for l in levels if l["type"] == "buy"], key=lambda x: x["price"], reverse=True)
        min_per_level = max(10, state.min_notional)
        max_levels = int(usdt_balance / min_per_level) if usdt_balance > 0 else 0
        affordable = min(max_levels, len(buys))
        if affordable < 1:
            print(f"  {state.symbol}: ${usdt_balance:.2f} < ${min_per_level}, skipping grid")
            await push_activity(f"{state.symbol}: insufficient balance ${usdt_balance:.2f} for grid", "warn")
            if state.slot_acquired and self.allocator:
                await self.allocator.release(state.symbol)
                state.slot_acquired = False
            return
        buy_levels = buys[:affordable]
        equity_per_level = usdt_balance / affordable
        placed = []
        skipped = []
        for level in buy_levels:
            side = "buy"
            price = level["price"]
            amount = equity_per_level / price
            ticker = await self.exchange.watch_ticker(state.symbol)
            spread = (ticker["ask"] - ticker["bid"]) / ticker["last"]
            if spread > self.config["risk"]["slippage_max_percent"] / 100:
                skipped.append(f"{side.upper()} @ {price} (spread {spread:.4f})")
                continue
            client_id = self._client_order_id(state.symbol, f"grid{level['level']}buy")
            try:
                if self.post_only_grid:
                    order = await self.exchange.create_post_only_limit_order(state.symbol, side, amount, price, client_id)
                else:
                    order = await self.exchange.create_limit_order(state.symbol, side, amount, price, client_id)
            except Exception as e:
                skipped.append(f"{side.upper()} @ {price} ({str(e)[:80]})")
                continue
            level["placed"] = True
            level["order_id"] = order.get("id")
            level["client_order_id"] = self._order_client_id(order) or client_id
            self.db.log_trade({
                "timestamp": order["timestamp"], "pair": state.symbol,
                "side": side, "price": order["price"], "quantity": order["amount"],
                "order_id": order.get("id"), "status": order["status"],
                "grid_level": level["level"], "realized_pnl": None
            })
            placed.append(f"{side.upper()} @ {order['price']} x{order['amount']}")
        summary = f"📋 {state.symbol} grid: {len(placed)}/{len(levels)} levels ({affordable} buys)"
        if placed:
            summary += f"\n" + "\n".join(placed[:5])
            if len(placed) > 5:
                summary += f"\n...and {len(placed)-5} more"
        if skipped:
            summary += f"\n⚠️ Skipped: {len(skipped)} — " + "; ".join(skipped[:3])
            if len(skipped) > 3:
                summary += f" (+{len(skipped)-3})"
        state.levels = [l for l in state.levels if l.get("placed")]
        if not placed and state.slot_acquired and self.allocator:
            await self.allocator.release(state.symbol)
            state.slot_acquired = False
        print(summary)
        await self.notifier.send_message(summary)
        await self._publish_orders()

    async def recenter_grid(self, state: GridState):
        now = asyncio.get_event_loop().time()
        min_cooldown = 1800
        if (now - state.last_rebalance) < min_cooldown:
            return
        state.is_active = False
        await asyncio.sleep(0)
        try:
            if self.manage_only_bot_orders:
                await self.exchange.cancel_bot_orders(state.symbol, self.client_id_prefix)
            else:
                await self.exchange.cancel_all_orders(state.symbol)
        except Exception:
            pass
        balance = await self.exchange.fetch_balance()
        base = state.symbol.split("/")[0]
        bal = balance.get(base, {}).get("free", 0)
        sell_qty = min(float(bal), state.filled_qty) if self.manage_only_bot_orders else float(bal)
        if sell_qty > 0:
            try:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                market_price = float(ticker["last"])
                avg_entry = (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else 0
                client_id = self._client_order_id(state.symbol, "recenter")
                order = await self.exchange.create_market_sell_order(state.symbol, sell_qty, client_id)
                if avg_entry > 0:
                    actual_price = self._order_avg_price(order) or market_price
                    fee = self._calc_fee(order, sell_qty, actual_price, is_maker=False)
                    cost_basis = self._consume_inventory(state, min(sell_qty, state.filled_qty), avg_entry)
                    pnl = round((actual_price * sell_qty) - cost_basis - fee, 2)
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                        "side": "sell", "price": market_price, "quantity": sell_qty,
                        "order_id": order.get("id"), "status": "closed",
                        "grid_level": None, "realized_pnl": pnl, "fee_cost": fee,
                    })
                state.filled_cost = 0.0
                state.filled_qty = 0.0
                state.fill_lots = []
            except Exception:
                pass
        ticker = await self.exchange.watch_ticker(state.symbol)
        state.levels = await self.calculate_grid_levels(state, ticker["last"])
        await self.place_grid_orders(state, state.levels)
        state.last_rebalance = now
        state.fill_counts = {"buy": 0, "sell": 0}
        state.is_active = True
        asyncio.create_task(self.watch_order_fills(state))
        asyncio.create_task(self.check_exit_conditions(state))
        await self.notifier.send_message(f"🔄 {state.symbol} grid re-centered at ${ticker['last']}")

    async def watch_order_fills(self, state: GridState):
        while state.is_active:
            try:
                orders = await asyncio.wait_for(self.exchange.watch_orders(state.symbol), timeout=10)
                for order in orders:
                    status = str(order.get("status", "")).lower()
                    if status == "closed":
                        if not self._is_bot_order(order):
                            continue
                        order_key = self._order_key(order)
                        if order_key in state.processed_order_ids:
                            continue
                        state.processed_order_ids.add(order_key)
                        side = order["side"]
                        fill_price = self._order_avg_price(order)
                        amount = float(order["filled"])
                        if amount <= 0 or fill_price <= 0:
                            continue
                        state.fill_counts[side] += 1
                        if side == "buy":
                            buy_fee = self._calc_fee(order, amount, fill_price, is_maker=self.post_only_grid)
                            self._add_inventory_lot(state, amount, fill_price * amount + buy_fee)
                            sell_price = round(fill_price * (1 + state.width), 4)
                            client_id = self._client_order_id(state.symbol, "gridsell")
                            try:
                                if self.post_only_grid:
                                    sell_order = await self.exchange.create_post_only_limit_order(state.symbol, "sell", amount, sell_price, client_id)
                                else:
                                    sell_order = await self.exchange.create_limit_order(state.symbol, "sell", amount, sell_price, client_id)
                            except Exception:
                                sell_order = await self.exchange.create_limit_order(state.symbol, "sell", amount, sell_price, client_id)
                            est_sell_fee = self._calc_fee(None, amount, sell_price, is_maker=self.post_only_grid)
                            profit = round((sell_price - fill_price) * amount - buy_fee - est_sell_fee, 2)
                            self.db.log_trade({
                                "timestamp": sell_order["timestamp"], "pair": state.symbol,
                                "side": "sell", "price": sell_order["price"],
                                "quantity": sell_order["amount"], "order_id": sell_order.get("id"),
                                "status": sell_order["status"], "grid_level": None, "realized_pnl": None,
                            })
                            await self.notifier.send_message(f"✅ {state.symbol} Buy→Sell | Buy: {fill_price} → Sell: {sell_price} | est net +${profit}")
                        elif side == "sell":
                            sell_fee = self._calc_fee(order, amount, fill_price, is_maker=self.post_only_grid)
                            cost_basis = self._consume_inventory(state, amount, fill_price / (1 + state.width))
                            profit = round((fill_price * amount) - cost_basis - sell_fee, 2)
                            buy_price = round(fill_price * (1 - state.width), 4)
                            client_id = self._client_order_id(state.symbol, "gridbuy")
                            try:
                                if self.post_only_grid:
                                    buy_order = await self.exchange.create_post_only_limit_order(state.symbol, "buy", amount, buy_price, client_id)
                                else:
                                    buy_order = await self.exchange.create_limit_order(state.symbol, "buy", amount, buy_price, client_id)
                            except Exception:
                                buy_order = await self.exchange.create_limit_order(state.symbol, "buy", amount, buy_price, client_id)
                            self.db.log_trade({
                                "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                                "side": "sell", "price": fill_price,
                                "quantity": amount, "order_id": order.get("id"),
                                "status": "closed", "grid_level": None, "realized_pnl": profit,
                                "fee_cost": sell_fee,
                            })
                            self.db.log_trade({
                                "timestamp": buy_order["timestamp"], "pair": state.symbol,
                                "side": "buy", "price": buy_order["price"],
                                "quantity": buy_order["amount"], "order_id": buy_order.get("id"),
                                "status": buy_order["status"], "grid_level": None, "realized_pnl": None,
                            })
                            await self.notifier.send_message(f"✅ {state.symbol} Sell→Buy | Sell: {fill_price} → Buy: {buy_price} | net ${profit:+.2f} (fee ${sell_fee:.4f})")
                            if profit < 0:
                                state.consecutive_losses += 1
                                if state.consecutive_losses >= 3:
                                    state.cooldown_until = asyncio.get_event_loop().time() + 3600
                                    await self.notifier.send_message(f"🛑 {state.symbol} 3 losses in a row. Cooling down 1h")
                            else:
                                state.consecutive_losses = 0
                            await self._log_balance()
                        total = state.fill_counts["buy"] + state.fill_counts["sell"]
                        if total >= 2:
                            buy_pct = state.fill_counts["buy"] / total
                            sell_pct = state.fill_counts["sell"] / total
                            skewed_side = "buy" if buy_pct >= sell_pct else "sell"
                            skew_pct = max(buy_pct, sell_pct)
                            regime = self.strategist.get_regime(state.symbol)
                            if skew_pct >= 0.80 and regime != "trending":
                                direction = "up" if sell_pct >= 0.75 else "down"
                                await self.recenter_grid(state)
                                await self.notifier.send_message(f"📐 {state.symbol} re-centered due to {direction} skew ({state.fill_counts})")
                            elif skew_pct >= 0.60:
                                await self.notifier.send_message(
                                    f"⏳ {state.symbol} skew {round(skew_pct*100)}% ({skewed_side}) — "
                                    f"holding for natural reversion (regime: {regime})"
                                )
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"Order watch ({state.symbol}): {e}")
                await asyncio.sleep(1)

    async def check_exit_conditions(self, state: GridState):
        await asyncio.sleep(30)
        while state.is_active:
            try:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                price = float(ticker.get("last") or ticker.get("bid") or 0)
            except Exception:
                await asyncio.sleep(10)
                continue
            if price <= 0:
                await asyncio.sleep(10)
                continue
            if self.strategist.should_exit_take_profit(state.symbol):
                avg_entry = (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else 0
                if avg_entry > 0:
                    gross_profit_pct = (price - avg_entry) / avg_entry
                    roundtrip_fee_pct = float(self.config["fees"].get("maker", 0)) + float(self.config["fees"].get("taker", 0))
                    profile_name = self.config.get("active_profile", "standard")
                    profile_risk = self.config.get("profiles", {}).get(profile_name, {}).get("risk", {})
                    min_net_profit_pct = float(
                        profile_risk.get(
                            "min_net_profit_percent",
                            self.config.get("risk", {}).get("min_net_profit_percent", 0.25),
                        )
                    ) / 100
                    if gross_profit_pct - roundtrip_fee_pct < min_net_profit_pct:
                        await asyncio.sleep(10)
                        continue
                balance = await self.exchange.fetch_balance()
                base = state.symbol.split("/")[0]
                coin_bal = balance.get(base, {}).get("free", 0)
                state.cooldown_until = asyncio.get_event_loop().time() + 300
                if coin_bal > 0 and state.filled_qty > 0:
                    await self.cancel_all(state)
                else:
                    try:
                        if self.manage_only_bot_orders:
                            await self.exchange.cancel_bot_orders(state.symbol, self.client_id_prefix)
                        else:
                            await self.exchange.cancel_all_orders(state.symbol)
                    except Exception:
                        pass
                    state.is_active = False
                    if state.slot_acquired and self.allocator:
                        await self.allocator.release(state.symbol)
                        state.slot_acquired = False
                break
            if state.levels:
                lowest = min(l["price"] for l in state.levels if l["type"] == "buy")
                state_atr = self.strategist.entry_conditions.get(state.symbol, {}).get("atr", 0)
                atr_mult = self.config["strategy"]["exit"]["stop_loss"].get("atr_multiplier", 1.5)
                pct_stop = lowest * (1 - self.config["strategy"]["exit"]["stop_loss"]["percent_below_lowest_grid"] / 100)
                if state_atr > 0:
                    stop = max(pct_stop, lowest - (state_atr * atr_mult))
                else:
                    stop = pct_stop
                if price < stop:
                    await self.notifier.send_message(f"🛑 {state.symbol} SL triggered: {price} < {stop}")
                    state.cooldown_until = asyncio.get_event_loop().time() + 3600
                    await self.cancel_all(state)
                    await asyncio.sleep(4 * 3600)
                    break
            if self.strategist.should_exit_trend_inversion(state.symbol):
                    await self.notifier.send_message(f"📉 {state.symbol} Trend inversion (1h below 200 EMA)")
                    state.cooldown_until = asyncio.get_event_loop().time() + 3600
                    await self.cancel_all(state)
                    break
            if state._ct_risk and state._ct_risk.get("force_exit_on_timeout") and state.trend_entry_started > 0:
                elapsed = (asyncio.get_event_loop().time() - state.trend_entry_started) / 60
                if elapsed >= state._ct_risk["time_limit_minutes"]:
                    await self.notifier.send_message(f"⏰ {state.symbol} Countertrend time limit ({state._ct_risk['time_limit_minutes']}m) — exiting")
                    state._ct_risk = None
                    await self.cancel_all(state)
                    break
            await asyncio.sleep(10)

    async def cancel_all(self, state: GridState):
        state.is_active = False
        state._ct_risk = None
        state._analyst_size_mult = 1.0
        state._news_size_mult = 1.0
        try:
            if self.manage_only_bot_orders:
                await self.exchange.cancel_bot_orders(state.symbol, self.client_id_prefix)
            else:
                await self.exchange.cancel_all_orders(state.symbol)
        except Exception:
            pass
        balance = await self.exchange.fetch_balance()
        base = state.symbol.split("/")[0]
        bal = balance.get(base, {}).get("free", 0)
        bot_qty = state.filled_qty + (state.trend_size if state.trend_active else 0)
        sell_qty = min(float(bal), bot_qty) if self.manage_only_bot_orders else float(bal)
        if sell_qty > 0:
            try:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                market_price = float(ticker["last"])
                avg_entry = (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else self.db.get_avg_entry_price(state.symbol)
                client_id = self._client_order_id(state.symbol, "cancel")
                order = await self.exchange.create_market_sell_order(state.symbol, sell_qty, client_id)
                if avg_entry > 0:
                    actual_price = self._order_avg_price(order) or market_price
                    fee = self._calc_fee(order, sell_qty, actual_price, is_maker=False)
                    cost_basis = self._consume_inventory(state, min(sell_qty, state.filled_qty), avg_entry)
                    remaining_trend_qty = max(0.0, sell_qty - min(sell_qty, state.filled_qty))
                    cost_basis += remaining_trend_qty * (state.trend_entry_price or actual_price)
                    pnl = round((actual_price * sell_qty) - cost_basis - fee, 2) if avg_entry > 0 else 0
                else:
                    fee = 0
                    pnl = 0
                exit_status = getattr(state, 'status_reason', None) or "closed"
                self.db.log_trade({
                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                    "side": "sell", "price": market_price, "quantity": sell_qty,
                    "order_id": order.get("id"), "status": exit_status,
                    "grid_level": None, "realized_pnl": pnl, "fee_cost": fee,
                })
            except Exception as e:
                exit_status = getattr(state, 'status_reason', None) or "closed"
                self.db.log_trade({
                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                    "side": "sell", "price": state.trend_entry_price if state.trend_entry_price else 0,
                    "quantity": sell_qty, "order_id": None, "status": exit_status,
                    "grid_level": None, "realized_pnl": 0, "fee_cost": 0,
                })
            self.db.log_decision(state.symbol, "CANCEL_FAIL",
                f"sell: {e}")
            await push_activity(f"Cancel sell failed ({state.symbol}): {e}", "error")
        self.db.mark_cancelled(state.symbol)
        state.trend_active = False
        state.trend_entry_pending = False
        state.trend_size = 0.0
        await self.notifier.send_message(f"🔴 {state.symbol} Grid cancelled")
        if state.slot_acquired and self.allocator:
            await self.allocator.release(state.symbol)
            state.slot_acquired = False
        await self._publish_orders()

    async def cancel_open_orders(self):
        for symbol in self.states:
            try:
                if self.manage_only_bot_orders:
                    await self.exchange.cancel_bot_orders(symbol, self.client_id_prefix)
                else:
                    await self.exchange.cancel_all_orders(symbol)
            except Exception:
                pass

    async def add_pair(self, symbol: str):
        if symbol in self.states:
            return
        st = GridState(symbol, self.config)
        st.pair_budget = self.pair_budget
        try:
            st.min_notional = self.exchange.get_min_notional(symbol)
        except Exception:
            st.min_notional = 10.0
        self.states[symbol] = st
        self._pair_tasks[symbol] = asyncio.create_task(self.manage_pair(st))

    async def remove_pair(self, symbol: str):
        if symbol not in self.states:
            return
        if symbol in self._pair_tasks:
            self._pair_tasks[symbol].cancel()
            del self._pair_tasks[symbol]
        st = self.states[symbol]
        if st.is_active:
            await self.cancel_all(st)
        del self.states[symbol]
        self.allocator.remove_pair(symbol)

    async def enter_trend_position(self, state: GridState, fixed_tp: float = 0, fixed_sl: float = 0):
        state.filled_cost = 0.0
        state.filled_qty = 0.0
        balance = await self.exchange.fetch_balance()
        usdt = float(balance["USDT"]["free"])
        simulated = os.getenv("SIMULATED_BALANCE")
        if simulated:
            usdt = min(usdt, float(simulated))
        trend_cfg = self.config["strategy"].get("trend", {})
        risk_pct = trend_cfg.get("risk_percent", 2.0) / 100
        state.atr = self.strategist.entry_conditions.get(state.symbol, {}).get("atr", 0)
        ec = self.strategist.entry_conditions.get(state.symbol, {})
        entry_price = self.strategist.get_trend_price(state.symbol)
        if not state.entry_type:
            ttype = "continuation"
            if ec.get("trend_breakout"):
                ttype = "breakout"
            elif state._ct_risk is not None:
                ttype = "countertrend"
            elif ec.get("regime") == "sideways":
                ttype = "sideways"
            state.entry_type = ttype
        else:
            ttype = state.entry_type
        try:
            ticker = await asyncio.wait_for(self.exchange.watch_ticker(state.symbol), timeout=5)
            ticker_ok = True
        except Exception:
            ticker_ok = False
        if ticker_ok:
            bid = float(ticker.get("bid", 0))
            ask = float(ticker.get("ask", 0))
            last = float(ticker.get("last", 0))
            spread = (ask - bid) / last if last > 0 and bid > 0 and ask > 0 else 0

            if ttype in ("continuation", "breakout") and spread > self.max_spread_pct:
                self.db.log_decision(state.symbol, "SKIP",
                    f"spread_{spread*10000:.0f}bps_exceeds_max_for_{ttype}",
                    ec.get("regime", ""), ec.get("adx", 0), ec.get("atr", 0),
                    ec.get("rsi", 0), last or entry_price, 0)
                await push_activity(f"{state.symbol} {ttype} skipped: spread {spread*10000:.0f}bps", "warn")
                return

            if ttype == "breakout" or ttype == "sideways":
                entry_price = ask if ask > 0 else entry_price
            else:
                best_bid = bid if bid > 0 else last
                offset_pct = self.offset.get(ttype, 0.0003)
                effective_offset = min(offset_pct, spread * 0.5) if spread > 0 else offset_pct
                entry_price = round(best_bid * (1 + effective_offset), 4)
                ema_20 = ec.get("ema_20", 0)
                if ema_20 > 0 and entry_price > ema_20 * 1.01:
                    if state._ct_risk is not None:
                        entry_price = round(float(ema_20), 4)
                        self.db.log_decision(state.symbol, "INFO",
                            f"countertrend_cap_entry_to_ema20:{entry_price}",
                            ec.get("regime", ""), ec.get("adx", 0), ec.get("atr", 0),
                            ec.get("rsi", 0), entry_price, 0)
                    else:
                        entry_price = 0
        if not ticker_ok:
            try:
                rest_ticker = await asyncio.wait_for(
                    self.exchange.fetch_ticker(state.symbol), timeout=5)
                if rest_ticker:
                    entry_price = float(
                        rest_ticker.get("last", 0) or
                        rest_ticker.get("ask", 0) or 0)
            except Exception:
                pass
            if entry_price <= 0:
                entry_price = float(ec.get("last_price", 0) or ec.get("close", 0) or 0)
        tp_atr = trend_cfg.get("tp_atr", 1.5)
        trail_atr = trend_cfg.get("trail_atr", 2.0)
        profile_params = self.strategist.get_profile_params(state.symbol)
        tp_atr = profile_params["tp_atr"]
        trail_atr = profile_params["sl_atr"]
        rsi = ec.get("rsi", 50)
        rsi_threshold = self.config["strategy"]["entry"].get("nudge", {}).get("rsi_extreme_threshold", 80)
        if rsi > rsi_threshold:
            tp_atr = self.config["strategy"]["entry"].get("nudge", {}).get("rsi_extreme_tp_atr", 0.5)
        if entry_price <= 0 or state.atr <= 0:
            reason = f"invalid_entry: price={entry_price} atr={state.atr}"
            self.db.log_decision(state.symbol, "SKIP", reason, ec.get("regime", ""),
                ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), entry_price, 0)
            await push_activity(f"{state.symbol} trend entry skipped: {reason}", "warn")
            return
        risk_amount = min(usdt * risk_pct, state.pair_budget * 0.5)
        size = round(risk_amount / (state.atr * trail_atr), 4)
        max_size = (usdt * 0.95) / entry_price
        size = min(size, max_size)
        # Check base viability BEFORE multipliers
        base_notional = round(size * entry_price, 2)
        # Cap to slot budget to prevent oversized positions
        max_notional = state.pair_budget * 0.9
        if base_notional > max_notional:
            size = round(max_notional / entry_price, 8)
            base_notional = round(size * entry_price, 2)
        ct_mult = state._ct_risk["size_multiplier"] if state._ct_risk else 1.0
        analyst_mult = state._analyst_size_mult
        news_mult = state._news_size_mult
        combined_mult = max(0.05, ct_mult * analyst_mult * news_mult)
        final_notional = round(size * combined_mult * entry_price, 2)
        if final_notional < 5 or base_notional < 5:
            reason = f"entry_too_small: base_${base_notional}_final_${final_notional}_x{combined_mult:.2f}"
            self.db.log_decision(state.symbol, "SKIP", reason, ec.get("regime", ""),
                ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), entry_price, 0)
            await push_activity(f"{state.symbol} entry skipped: {reason}", "warn")
            state._ct_risk = None
            state._analyst_size_mult = 1.0
            state._news_size_mult = 1.0
            return
        # Apply multipliers
        if state._ct_risk:
            size *= state._ct_risk["size_multiplier"]
            trail_atr *= state._ct_risk["stop_atr_multiplier"]
            print(f"  Countertrend entry: size x{state._ct_risk['size_multiplier']:.2f}, stop x{state._ct_risk['stop_atr_multiplier']:.2f}")
        size *= state._analyst_size_mult * state._news_size_mult
        size *= state._ai_size_mult
        state._ai_size_mult = 1.0
        try:
            fg_raw = await self.redis.get("vortex:fear_greed") if self.redis else None
            if fg_raw:
                fg = json.loads(fg_raw)
                fg_val = int(fg["value"])
                if fg_val < 25:
                    size *= 1.5
                elif fg_val < 45:
                    size *= 1.2
                elif fg_val >= 75:
                    size *= 0.4
                elif fg_val >= 55:
                    size *= 0.7
        except Exception:
            pass
        state._analyst_size_mult = 1.0
        state._news_size_mult = 1.0
        size = round(size, 6)
        if size * entry_price < 5:
            self.db.log_decision(state.symbol, "SKIP", "size_after_mult_too_small",
                "", 0, 0, 0, entry_price, 0)
            await push_activity(f"{state.symbol} entry skipped post-mult", "warn")
            return
        try:
            client_id = self._client_order_id(state.symbol, "trendbuy")
            adx = ec.get("adx", 0)
            if adx > 30 or self.config.get("execution", {}).get("force_market_entries", False):
                order = await self.exchange.create_market_buy_order(state.symbol, size, client_id)
                fill_price = self._order_avg_price(order) or float(order.get("price") or 0)
                if fill_price <= 0:
                    fill_price = entry_price
                if fill_price > 0:
                    entry_price = fill_price
                    state.trend_entry_pending = False
                    state.trend_active = True
                    state.entry_adx = ec.get("adx", 0)
                    state.entry_rsi = ec.get("rsi", 0)
                    state.entry_regime = _regime_with_dir(ec)
                    state.trend_entry_price = fill_price
                    state.trend_size = size
                    if fixed_tp and fixed_sl:
                        state.trend_stop = fill_price * (1 - fixed_sl)
                        state.trend_target = fill_price * (1 + fixed_tp)
                    else:
                        state.trend_stop = fill_price - (state.atr * trail_atr)
                        state.trend_target = fill_price + (state.atr * tp_atr)
                    state.trend_high = fill_price
                    fee = self._calc_fee(order, size, fill_price, is_maker=False)
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                        "side": "buy", "price": fill_price, "quantity": size,
                        "order_id": order.get("id"), "status": "closed",
                        "grid_level": None, "realized_pnl": None, "fee_cost": fee,
                    })
                    if fixed_tp and fixed_sl:
                        sl_pct = (state.trend_stop / fill_price - 1) * 100
                        tp_pct = (state.trend_target / fill_price - 1) * 100
                        tp_usd = round(size * fill_price * (tp_pct / 100), 2)
                        sl_usd = round(size * fill_price * abs(sl_pct / 100), 2)
                        await self.notifier.send_message(
                            f"🔥 {state.symbol} market buy @ ${fill_price:.4f} | "
                            f"SL: ${state.trend_stop:.4f} (Expected SL: {sl_pct:.2f}%, -${sl_usd:.2f}) | "
                            f"TP: ${state.trend_target:.4f} (Expected TP: {tp_pct:+.2f}%, +${tp_usd:.2f})"
                        )
                    else:
                        await self.notifier.send_message(f"🔥 {state.symbol} market trend buy @ ${fill_price:.4f} | Trail SL: ${state.trend_stop:.4f}")
                    state.bullets_fired = 1
                    if fixed_tp and fixed_sl:
                        asyncio.create_task(self._position_monitor(state))
                    else:
                        asyncio.create_task(self.trail_trend_position(state))
                    return
            if self.post_only_trend:
                order = await self.exchange.create_post_only_limit_order(state.symbol, "buy", size, entry_price, client_id)
            else:
                order = await self.exchange.create_limit_order(state.symbol, "buy", size, entry_price, client_id)
            state.trend_entry_pending = True
            state.trend_entry_order_id = str(order.get("id") or "")
            state.trend_entry_client_id = self._order_client_id(order) or client_id
            state.trend_entry_started = asyncio.get_event_loop().time()
            state.trend_entry_price = float(order["price"])
            state.trend_size = float(order["amount"])
            if fixed_tp and fixed_sl:
                state.trend_stop = entry_price * (1 - fixed_sl)
                state.trend_target = entry_price * (1 + fixed_tp)
            else:
                state.trend_stop = entry_price - (state.atr * trail_atr)
                state.trend_target = entry_price + (state.atr * tp_atr)
            state.trend_high = entry_price
            self.db.log_trade({
                "timestamp": order["timestamp"], "pair": state.symbol,
                "side": "buy", "price": order["price"], "quantity": order["amount"],
                "order_id": order.get("id"), "status": order["status"],
                "grid_level": None, "realized_pnl": None,
            })
            if fixed_tp and fixed_sl:
                sl_pct = (state.trend_stop / entry_price - 1) * 100
                tp_pct = (state.trend_target / entry_price - 1) * 100
                tp_usd = round(state.trend_size * entry_price * (tp_pct / 100), 2)
                sl_usd = round(state.trend_size * entry_price * abs(sl_pct / 100), 2)
                await self.notifier.send_message(
                    f"📈 {state.symbol} entry placed @ ${entry_price:.2f} | "
                    f"SL: ${state.trend_stop:.2f} (Expected SL: {sl_pct:.2f}%, -${sl_usd:.2f}) | "
                    f"TP: ${state.trend_target:.2f} (Expected TP: {tp_pct:+.2f}%, +${tp_usd:.2f})"
                )
            else:
                await self.notifier.send_message(f"📈 {state.symbol} trend entry placed @ ${entry_price} | Trail SL after fill: ${state.trend_stop:.2f}")
            asyncio.create_task(self.watch_trend_entry_fill(state))
        except Exception as e:
            self.db.log_decision(state.symbol, "SKIP", f"trend_order_failed:{str(e)[:160]}",
                ec.get("regime", ""), ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), entry_price, 0)
            await push_activity(f"{state.symbol} trend order failed: {e}", "error")
            await self.notifier.send_message(f"⛔ {state.symbol} trend entry failed: {e}")

    async def watch_trend_entry_fill(self, state: GridState):
        while state.trend_entry_pending and not state.trend_active:
            try:
                ec = self.strategist.entry_conditions.get(state.symbol, {})
                ttype = state.entry_type or "continuation"
                timeout_seconds = self.timeout.get(ttype, 90)
                if timeout_seconds and (asyncio.get_event_loop().time() - state.trend_entry_started) > timeout_seconds:
                    if ec.get("trend_breakout"):
                        try:
                            await self.exchange.cancel_order(state.trend_entry_order_id, state.symbol)
                        except Exception:
                            pass
                        try:
                            client_id = self._client_order_id(state.symbol, "trendbuy")
                            buy_order = await self.exchange.create_market_buy_order(state.symbol, state.trend_size, client_id)
                            fill_price = self._order_avg_price(buy_order)
                            amount = float(buy_order.get("filled") or state.trend_size)
                            if fill_price > 0 and amount > 0:
                                state.trend_entry_pending = False
                                state.trend_active = True
                                state.entry_adx = ec.get("adx", 0)
                                state.entry_rsi = ec.get("rsi", 0)
                                state.entry_regime = _regime_with_dir(ec)
                                state.trend_entry_price = fill_price
                                state.trend_size = amount
                                state.trend_stop = fill_price - (state.atr * self.config["strategy"].get("trend", {}).get("trail_atr", 2.0))
                                state.trend_target = fill_price + (state.atr * self.config["strategy"].get("trend", {}).get("tp_atr", 1.5))
                                state.trend_high = fill_price
                                fee = self._calc_fee(buy_order, amount, fill_price, is_maker=False)
                                self.db.log_trade({
                                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                                    "side": "buy", "price": fill_price, "quantity": amount,
                                    "order_id": buy_order.get("id"), "status": "closed",
                                    "grid_level": None, "realized_pnl": None, "fee_cost": fee,
                                })
                                await self.notifier.send_message(f"⚡ {state.symbol} breakout entry filled (market) @ ${fill_price:.4f} | Trail SL: ${state.trend_stop:.4f}")
                                state.bullets_fired = 1
                                asyncio.create_task(self.trail_trend_position(state))
                                return
                        except Exception as e:
                            await self.notifier.send_message(f"⛔ {state.symbol} breakout market entry failed: {e}")
                    if state.trend_entry_order_id:
                        try:
                            await self.exchange.cancel_order(state.trend_entry_order_id, state.symbol)
                        except Exception:
                            pass
                    state.trend_entry_pending = False
                    state.trend_size = 0.0
                    if state.slot_acquired and self.allocator:
                        await self.allocator.release(state.symbol)
                        state.slot_acquired = False
                    await self.notifier.send_message(f"⌛ {state.symbol} trend entry timed out; slot released")
                    waited = int((asyncio.get_event_loop().time() - state.trend_entry_started) / 60)
                    ep = state.trend_entry_price or 0
                    offset_pct = self.offset.get(ttype, 0.0003) * 100
                    reason = f"{ttype.capitalize()} — entry at ${ep:.2f} (+{offset_pct:.2f}% offset) expired unfilled after {waited}min"
                    try:
                        since_ms = int(state.trend_entry_started * 1000)
                        ohlcv = await self.exchange.fetch_ohlcv(state.symbol, "1m", limit=5, since=since_ms)
                        if ohlcv and len(ohlcv) > 1 and ep > 0:
                            highs = [c[2] for c in ohlcv if c[2]]
                            lows = [c[3] for c in ohlcv if c[3]]
                            high_after = max(highs)
                            low_after = min(lows)
                            max_upside = (high_after - ep) / ep * 100
                            max_drawdown = (ep - low_after) / ep * 100
                            if max_upside > offset_pct:
                                reason += f" — price ran +{max_upside:.2f}% during that period"
                            elif max_drawdown > 0.05:
                                reason += f" — price touched -{max_drawdown:.2f}% below entry"
                            else:
                                reason += f" — price moved +{max_upside:.2f}%/-{max_drawdown:.2f}%"
                    except Exception:
                        pass
                    self.db.log_decision(state.symbol, "PENDING_EXPIRED", reason,
                        ec.get("regime", ""), ec.get("adx", 0), ec.get("atr", 0),
                        ec.get("rsi", 0), ep, 0)
                    return
                orders = await asyncio.wait_for(self.exchange.watch_orders(state.symbol), timeout=10)
                for order in orders:
                    if not self._is_bot_order(order):
                        continue
                    cid = self._order_client_id(order)
                    oid = str(order.get("id") or "")
                    if oid != state.trend_entry_order_id and cid != state.trend_entry_client_id:
                        continue
                    status = str(order.get("status", "")).lower()
                    if status == "closed":
                        fill_price = self._order_avg_price(order)
                        amount = float(order.get("filled") or 0)
                        if fill_price <= 0 or amount <= 0:
                            continue
                        state.trend_entry_pending = False
                        state.trend_active = True
                        state.entry_adx = ec.get("adx", 0)
                        state.entry_rsi = ec.get("rsi", 0)
                        state.entry_regime = _regime_with_dir(ec)
                        state.trend_entry_price = fill_price
                        state.trend_size = amount
                        state.trend_stop = fill_price - (state.atr * self.config["strategy"].get("trend", {}).get("trail_atr", 2.0))
                        state.trend_target = fill_price + (state.atr * self.config["strategy"].get("trend", {}).get("tp_atr", 1.5))
                        state.trend_high = fill_price
                        fee = self._calc_fee(order, amount, fill_price, is_maker=self.post_only_trend)
                        self.db.log_trade({
                            "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                            "side": "buy", "price": fill_price, "quantity": amount,
                            "order_id": order.get("id"), "status": "closed",
                            "grid_level": None, "realized_pnl": None, "fee_cost": fee,
                        })
                        await self.notifier.send_message(f"✅ {state.symbol} trend filled @ ${fill_price:.4f} | Trail SL: ${state.trend_stop:.4f}")
                        state.bullets_fired = 1
                        asyncio.create_task(self.trail_trend_position(state))
                        return
                    if status in {"canceled", "expired", "rejected"}:
                        state.trend_entry_pending = False
                        state.trend_size = 0.0
                        await self.notifier.send_message(f"⚪ {state.symbol} trend entry {status}")
                        self.db.log_decision(state.symbol, "PENDING_CANCELLED",
                            f"@${state.trend_entry_price:.2f}: {status}",
                            "", 0, 0, 0, state.trend_entry_price, 0)
                        return
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"watch_trend_entry_fill ({state.symbol}): {e}")
                await push_activity(f"Trend entry fill error ({state.symbol}): {e}", "error")
            await asyncio.sleep(2)

    async def exit_trend_position(self, state: GridState, reason: str):
        if not state.trend_active or state.trend_size <= 0:
            state.trend_active = False
            return
        try:
            balance = await self.exchange.fetch_balance()
            base = state.symbol.split("/")[0]
            free = float(balance.get(base, {}).get("free", 0))
            qty = min(free, state.trend_size)
            if qty <= 0:
                await self.notifier.send_message(f"⚠️ {state.symbol} trend exit skipped: no free {base}")
                self.db.log_trade({
                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                    "side": "sell", "price": state.trend_entry_price, "quantity": 0,
                    "order_id": None, "status": "closed",
                    "grid_level": None, "realized_pnl": 0, "fee_cost": 0,
                })
                self.db.log_decision(state.symbol, "EXIT_SKIP",
                    "no free coins to sell",
                    state.entry_regime, state.entry_adx, 0, state.entry_rsi, state.trend_entry_price, 0)
                state.trend_active = False
                state.trend_entry_pending = False
                await self._release_slot(state, "exit_skip")
                return
            client_id = self._client_order_id(state.symbol, f"trend{reason}")
            order = await self.exchange.create_market_sell_order(state.symbol, qty, client_id)
            exit_price = self._order_avg_price(order)
            if exit_price <= 0:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                exit_price = float(ticker["last"])
            entry_fee = self._calc_fee(None, qty, state.trend_entry_price, is_maker=self.post_only_trend)
            exit_fee = self._calc_fee(order, qty, exit_price, is_maker=False)
            total_fee = entry_fee + exit_fee
            pnl = round((exit_price - state.trend_entry_price) * qty - total_fee, 2)
            self.db.log_trade({
                "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                "side": "sell", "price": exit_price, "quantity": qty,
                "order_id": order.get("id"), "status": "closed",
                "grid_level": None, "realized_pnl": pnl, "fee_cost": exit_fee,
            })
            self.db.log_decision(state.symbol, f"EXIT_{reason.upper()}",
                f"PnL ${pnl:+.2f} @ ${exit_price:.4f}",
                state.entry_regime, state.entry_adx, 0, state.entry_rsi, exit_price, 0)
            await self.notifier.send_message(f"{'✅' if pnl >= 0 else '🛑'} {state.symbol} trend {reason.upper()} exit @ ${exit_price:.4f}: ${pnl:+.2f} (fee ${total_fee:.4f})")
            # Deduct loss from budget
            if pnl < 0:
                try:
                    remaining = await self.redis.get("vortex:budget_remaining") if self.redis else None
                    if remaining:
                        new_remaining = max(0, float(remaining) + pnl)
                        await self.redis.set("vortex:budget_remaining", str(round(new_remaining, 2)))
                except Exception:
                    pass
            if state.entry_type == "continuation":
                if pnl < 0:
                    state.continuation_losses += 1
                    cfg = self.config.get("anti_churn", {}).get("continuation", {})
                    max_losses = cfg.get("max_consecutive_losses", 2)
                    if state.continuation_losses >= max_losses:
                        cooldown = cfg.get("cooldown_minutes", 45) * 60
                        state.continuation_cooldown = asyncio.get_event_loop().time() + cooldown
                        self._log("RISK", f"{state.symbol} anti-churn: {state.continuation_losses}/{max_losses} losses → cooldown {cooldown//60}m")
                else:
                    state.continuation_losses = 0
        except Exception as e:
            await self.notifier.send_message(f"⚠️ {state.symbol} trend exit failed: {e}")
            return
        state.trend_active = False
        state.trend_entry_pending = False
        state.trend_size = 0.0
        state.breakeven_activated = False
        if state.slot_acquired and self.allocator:
            await self.allocator.release(state.symbol)
            state.slot_acquired = False
        await self._publish_orders()

    async def _partial_exit(self, state: GridState, qty: float, reason: str):
        if qty <= 0: return
        try:
            balance = await self.exchange.fetch_balance()
            base = state.symbol.split("/")[0]
            free = float(balance.get(base, {}).get("free", 0))
            sell_qty = min(free, qty)
            if sell_qty <= 0: return
            client_id = self._client_order_id(state.symbol, f"trend{reason}")
            order = await self.exchange.create_market_sell_order(state.symbol, sell_qty, client_id)
            exit_price = self._order_avg_price(order)
            if exit_price <= 0:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                exit_price = float(ticker["last"])
            entry_fee = self._calc_fee(None, sell_qty, state.trend_entry_price, is_maker=self.post_only_trend)
            exit_fee = self._calc_fee(order, sell_qty, exit_price, is_maker=False)
            total_fee = entry_fee + exit_fee
            pnl = round((exit_price - state.trend_entry_price) * sell_qty - total_fee, 2)
            self.db.log_trade({
                "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                "side": "sell", "price": exit_price, "quantity": sell_qty,
                "order_id": order.get("id"), "status": "closed",
                "grid_level": None, "realized_pnl": pnl, "fee_cost": exit_fee,
            })
            self.db.log_decision(state.symbol, f"PARTIAL_{reason.upper()}",
                f"sold_{sell_qty:.4f}_@_${exit_price:.4f}_pnl=${pnl:+.2f}", "", 0, 0, 0, exit_price, 0)
            await self.notifier.send_message(
                f"📊 {state.symbol} {reason.upper()}: sold {sell_qty:.4f} @ ${exit_price:.4f} | ${pnl:+.2f}")
            state.trend_size = round(state.trend_size - sell_qty, 8)
        except Exception as e:
            await self.notifier.send_message(f"⚠️ {state.symbol} partial exit failed: {e}")

    async def _position_monitor(self, state: GridState):
        """Monitor fixed TP/SL positions with breakeven lock and 5-min SL cooldown."""
        be_pct = self.strategist.get_breakeven_pct(0.2)
        await asyncio.sleep(10)
        try:
            while state.trend_active:
                try:
                    ticker = await self.exchange.watch_ticker(state.symbol)
                    price = float(ticker.get("bid") or ticker["last"])
                    ticker_ts = ticker.get("timestamp", 0)
                    if ticker_ts and time.time() * 1000 - ticker_ts > 30000:
                        continue

                    # Breakeven lock
                    if not state.breakeven_activated and be_pct > 0 and price >= state.trend_entry_price * (1 + be_pct):
                        state.breakeven_activated = True
                        be_stop = round(state.trend_entry_price * 1.001, 8)
                        state.trend_stop = max(state.trend_stop, be_stop)
                        self._log("TRADE", f"{state.symbol} breakeven lock @ ${be_stop:.2f} (trigger ${price:.4f})")
                        self.db.log_decision(state.symbol, "BREAKEVEN_LOCK",
                            f"stop→${be_stop:.2f}_trigger=${price:.4f}",
                            state.entry_regime, state.entry_adx, 0, state.entry_rsi, price, 0)

                    # Take profit at trend_target (fixed TP, 100% at +0.8%)
                    if state.trend_target > 0 and price >= state.trend_target:
                        self._log("TRADE", f"{state.symbol} take profit @ ${price:.2f}")
                        state.sideway_wins += 1
                        state.sideway_losses = 0
                        await self.exit_trend_position(state, "tp")
                        break
                    if price < state.trend_stop:
                        self._log("RISK", f"{state.symbol} SL triggered @ ${price:.2f} — 5min cooldown")
                        trigger_time = asyncio.get_event_loop().time()
                        recovered = False
                        while asyncio.get_event_loop().time() - trigger_time < 300:
                            try:
                                t = await self.exchange.watch_ticker(state.symbol)
                                p = float(t.get("last"))
                                if p >= state.trend_stop:
                                    self._log("RISK", f"{state.symbol} SL recovered @ ${p:.2f}")
                                    recovered = True
                                    break
                            except Exception:
                                pass
                            await asyncio.sleep(10)
                        if recovered:
                            continue
                        # SL hit — track consecutive losses for anti-churn
                        state.sideway_losses += 1
                        state.sideway_wins = 0
                        if state.sideway_losses >= 3:
                            state.cooldown_until = asyncio.get_event_loop().time() + 7200
                            self._log("RISK", f"{state.symbol} 3 consecutive sideway losses — 2h cooldown")
                            await self.notifier.send_message(f"🛑 {state.symbol} 3 sideway losses. Cooling down 2h")
                        await self.exit_trend_position(state, "sl")
                        break
                except Exception as e:
                    print(f"_position_monitor ({state.symbol}): {e}")
                await asyncio.sleep(5)
        finally:
            if state.trend_active:
                await self.exit_trend_position(state, "cleanup")

    async def trail_trend_position(self, state: GridState):
        await asyncio.sleep(10)
        profile_params = self.strategist.get_profile_params(state.symbol)
        trail_mult = profile_params.get("sl_atr", 1.5)
        be_pct = self.strategist.get_breakeven_pct(0.2)
        try:
            while state.trend_active:
                try:
                    ticker = await self.exchange.watch_ticker(state.symbol)
                    price = float(ticker.get("bid") or ticker["last"])
                    ticker_ts = ticker.get("timestamp", 0)
                    if ticker_ts and time.time() * 1000 - ticker_ts > 30000:
                        continue
                    if price > state.trend_high:
                        state.trend_high = price
                        state.trend_stop = max(state.trend_stop, price - (state.atr * trail_mult))
                    if not state.breakeven_activated and be_pct > 0 and price >= state.trend_entry_price * (1 + be_pct):
                        if state.entry_type in ("continuation", "breakout"):
                            state.breakeven_activated = True
                            be_stop = round(state.trend_entry_price * 1.001, 8)
                            state.trend_stop = max(state.trend_stop, be_stop)
                            self._log("TRADE", f"{state.symbol} breakeven lock @ ${be_stop:.2f}")
                            self.db.log_decision(state.symbol, "BREAKEVEN_LOCK",
                                f"stop→${be_stop:.2f}_trigger=${price:.4f}",
                                state.entry_regime, state.entry_adx, 0, state.entry_rsi, price, 0)
                    if state.bullets_fired == 1:
                        profile_params = self.strategist.get_profile_params(state.symbol)
                        if profile_params.get("thesis_add", True):
                            pos_state = {
                                "avg_entry_price": (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else state.trend_entry_price,
                                "last_entry_attempt": state.last_entry_attempt,
                            }
                            if self.strategist.evaluate_thesis_add(state.symbol, pos_state):
                                try:
                                    trend_cfg = self.config["strategy"].get("trend", {})
                                    trail_atr = trend_cfg.get("trail_atr", 2.0)
                                    risk_pct = trend_cfg.get("risk_percent", 2.0) / 100
                                    balance = await self.exchange.fetch_balance()
                                    usdt = float(balance["USDT"]["free"])
                                    simulated = os.getenv("SIMULATED_BALANCE")
                                    if simulated:
                                        usdt = min(usdt, float(simulated))
                                    risk_amount = min(usdt * risk_pct, state.pair_budget * 0.5)
                                    add_size = round(risk_amount / (state.atr * trail_atr), 4)
                                    client_id = self._client_order_id(state.symbol, "thesisadd")
                                    add_order = await self.exchange.create_market_buy_order(state.symbol, add_size, client_id)
                                    add_price = self._order_avg_price(add_order) or float(add_order.get("price") or price)
                                    add_qty = float(add_order.get("filled", add_size))
                                    old_cost = state.filled_cost
                                    old_qty = state.filled_qty
                                    total_qty = old_qty + add_qty
                                    total_cost = old_cost + (add_qty * add_price)
                                    state.filled_cost = total_cost
                                    state.filled_qty = total_qty
                                    state.bullets_fired = 2
                                    state.avg_entry_price = round(total_cost / total_qty, 4) if total_qty > 0 else 0
                                    state.trend_stop = state.avg_entry_price - (state.atr * trail_atr)
                                    self.db.log_trade({
                                        "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                                        "side": "buy", "price": add_price, "quantity": add_qty,
                                        "order_id": add_order.get("id"), "status": "closed",
                                        "grid_level": None, "realized_pnl": None,
                                    })
                                    await self.notifier.send_message(
                                        f"✅ THESIS ADD: {state.symbol}\n"
                                        f"PRICE: ${add_price:.4f}\n"
                                        f"NEW AVG: ${state.avg_entry_price:.4f}\n"
                                        f"BULLET: 2/2"
                                    )
                                except Exception as e:
                                    await self.notifier.send_message(f"⚠️ {state.symbol} thesis add failed: {e}")
                    emergency_pct = self.config.get("risk", {}).get("emergency_stop_pct", 3)
                    if emergency_pct > 0 and price < state.trend_entry_price * (1 - emergency_pct / 100):
                        self._log("RISK", f"{state.symbol} emergency stop @ ${price:.2f} (entry ${state.trend_entry_price:.2f})")
                        self.db.log_decision(state.symbol, "EXIT_EMERGENCY",
                            f"entry=${state.trend_entry_price:.4f}_exit=${price:.4f}",
                            state.entry_regime, state.entry_adx, 0, state.entry_rsi, price, 0)
                        await self.exit_trend_position(state, "emergency")
                        break
                    if state.trend_target > 0 and price >= state.trend_target:
                        self._log("TRADE", f"{state.symbol} take profit @ ${price:.2f}")
                        await self.exit_trend_position(state, "tp")
                        break
                    if price < state.trend_stop:
                        await self.exit_trend_position(state, "trail")
                        break
                except Exception as e:
                    print(f"trail_trend ({state.symbol}): {e}")
                    await push_activity(f"Trail trend error ({state.symbol}): {e}", "error")
                await asyncio.sleep(5)
        finally:
            if state.trend_active:
                await self.exit_trend_position(state, "cleanup")

    async def graceful_exit_pair(self, symbol: str, reason: str = "manual_tp"):
        state = self.states.get(symbol)
        if not state:
            await self.notifier.send_message(f"⚠️ Manual exit: {symbol} not found.")
            return
        await push_activity(f"Manual exit triggered for {symbol} (reason: {reason})", "warn")
        state.status_reason = reason
        await self.cancel_all(state)
        now = asyncio.get_event_loop().time()
        cooldown_secs = self.config.get("risk", {}).get("manual_exit_cooldown_secs", 3600)
        state.cooldown_until = now + cooldown_secs
        self.db.log_decision(symbol, "MANUAL_EXIT", reason)
        await self.notifier.send_message(
            f"🛑 *{symbol}* manually closed ({reason.replace('_', ' ').upper()})\n"
            f"Cooldown: {cooldown_secs // 60} min before re-entry."
        )

    async def trigger_kill_switch(self):
        if self._kill_in_progress:
            return
        self._kill_in_progress = True
        for state in list(self.states.values()):
            try:
                if state.trend_active and state.trend_size > 0:
                    await self.exit_trend_position(state, "kill")
                else:
                    await self.cancel_all(state)
            except Exception:
                pass
        await self.notifier.send_message("❌ Kill switch activated")
        await self._connect_redis()
        if self.redis:
            try:
                await self.redis.setex("vortex:loss_limit_hit", 3600, "1")
            except Exception:
                pass
        try:
            await asyncio.wait_for(self.exchange.close(), timeout=5)
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass
        os._exit(0)

    async def manage_pair(self, state: GridState):
        while True:
            if self._kill_in_progress:
                return
            try:
                if state.trend_active or state.trend_entry_pending:
                    await asyncio.sleep(30)
                    continue
                if not state.is_active:
                    now = asyncio.get_event_loop().time()
                    self._cycle_count += 1
                    if await self._check_daily_loss():
                        return
                    if self._cycle_count % 6 == 0:
                        await self._check_budget_depleted()
                    if state.cooldown_until > now:
                        cooldown_secs = int(state.cooldown_until - now)
                        self._log("RISK", f"{state.symbol} COOLDOWN {cooldown_secs}s remaining")
                        await asyncio.sleep(30)
                        continue
                    throttle_remaining = 120 - (now - state.last_entry_attempt)
                    if throttle_remaining > 0:
                        self._log("RISK", f"{state.symbol} ENTRY_THROTTLE {throttle_remaining:.0f}s remaining")
                        await asyncio.sleep(10)
                        continue
                    regime = self.strategist.get_regime(state.symbol)
                    ec = self.strategist.entry_conditions.get(state.symbol, {})
                    if regime == "trending":
                        regime = "trending↑" if ec.get("trend_uptrend") else "trending↓"
                    panic = self.strategist._market_panic()
                    prev_regime = self._prev_regime.get(state.symbol, "")
                    if regime != prev_regime:
                        self._log("STATE", f"{state.symbol}: {prev_regime or 'INIT'} → {regime} (ADX {ec.get('adx',0):.1f}, RSI {ec.get('rsi',0):.1f})")
                        self._prev_regime[state.symbol] = regime
                    price = 0
                    try:
                        ticker = await asyncio.wait_for(
                            self.exchange.watch_ticker(state.symbol), timeout=5)
                        price = float(ticker["last"])
                    except (asyncio.TimeoutError, Exception):
                        pass
                    bal = 0
                    try:
                        b = await self.exchange.fetch_balance()
                        bal = float(b["USDT"]["free"])
                    except Exception:
                        pass

                    def _log_rejection(decision, reason, vetos):
                        self._reject_count += 1
                        tag = f"{reason} | vetos: {','.join(vetos)}" if vetos else reason
                        self.db.log_decision(state.symbol, decision, tag, regime,
                            ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), price, bal)
                        primary = vetos[0] if vetos else reason
                        reject_key = f"{state.symbol}:{primary}"
                        now_t = asyncio.get_event_loop().time()
                        last_key, last_t = self._last_rejection.get(reject_key, ("", 0))
                        if primary == last_key and (now_t - last_t) < 600:
                            self._rejection_agg[reject_key] = self._rejection_agg.get(reject_key, 1) + 1
                        else:
                            if reject_key in self._rejection_agg and self._rejection_agg[reject_key] > 1:
                                self._log("REJECTED", f"{state.symbol} [{primary}] repeated {self._rejection_agg[reject_key]}x (suppressed)")
                                self._rejection_agg.pop(reject_key, None)
                            self._last_rejection[reject_key] = (primary, now_t)
                            self._rejection_agg.pop(reject_key, None)
                            self._log("REJECTED", f"{state.symbol}: {reason}" + (f" | {','.join(vetos)}" if vetos else ""))

                    def log_dec(decision, reason, vetos=None):
                        if vetos:
                            _log_rejection(decision, reason, vetos)
                        else:
                            self.db.log_decision(state.symbol, decision, reason, regime,
                                ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), price, bal,
                                ec.get("trend_uptrend"))
                    perf_ok, perf_reason = await self._pair_performance_gate(state.symbol)
                    if not perf_ok:
                        pause_secs = int(self.performance_guard_pause_hours * 3600)
                        state.cooldown_until = now + pause_secs
                        self.db.log_decision(state.symbol, "SKIP", perf_reason, regime,
                            ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), price, bal,
                            ec.get("trend_uptrend"))
                        self._log("RISK", f"{state.symbol} performance gate: {perf_reason}")
                        await asyncio.sleep(30)
                        continue
                    # ── NewsFilter (risk scaler, disabled in TECHNICAL_ONLY) ──
                    news_size_mult = 1.0
                    if self.news_filter and self.trading_mode != TradingMode.TECHNICAL_ONLY:
                        try:
                            news_mult = await asyncio.wait_for(
                                self.news_filter.get_risk_multiplier(state.symbol), timeout=10)
                            news_size_mult = news_mult
                            if news_mult < 1.0:
                                self._log("NEWS", f"{state.symbol} risk multiplier {news_mult}")
                        except (asyncio.TimeoutError, Exception) as e:
                            self._log("ERROR", f"{state.symbol} NewsFilter: {e}")
                    # ── Analyst + ct_score (global) ──
                    analyst_signal = "NEUTRAL"
                    analyst_conf = 0
                    if self.trading_mode != TradingMode.TECHNICAL_ONLY:
                        if self.analyst:
                            last_regime = self._last_analyst_regime.get(state.symbol)
                            last_analyst_time = self._last_analyst_time.get(state.symbol, 0)
                            regime_changed = last_regime != regime
                            cache_expired = (asyncio.get_event_loop().time() - last_analyst_time) > 7200
                            if regime_changed or cache_expired or state.last_analyst_verdict is None:
                                try:
                                    verdict = await asyncio.wait_for(
                                        self.analyst.should_enter(state.symbol,
                                            self.strategist.data.get(state.symbol, {}).get(self.strategist.timeframes["entry"]),
                                            ec), timeout=8)
                                    state.last_analyst_verdict = verdict
                                    analyst_signal = verdict.get("verdict", "NEUTRAL")
                                    analyst_conf = verdict.get("confidence", 0)
                                    self.strategist.entry_conditions.setdefault(state.symbol, {})["analyst_signal"] = analyst_signal
                                    self._last_analyst_regime[state.symbol] = regime
                                    self._last_analyst_time[state.symbol] = asyncio.get_event_loop().time()
                                    if self.trading_mode == TradingMode.AI_OBSERVE_ONLY:
                                        self._log("AI", f"{state.symbol} verdict={analyst_signal} conf={analyst_conf} (observe)")
                                except (asyncio.TimeoutError, Exception) as e:
                                    if self.trading_mode == TradingMode.TECHNICAL_PLUS_AI:
                                        self._log("ERROR", f"{state.symbol} Analyst: {e}")
                    if self.trading_mode == TradingMode.TECHNICAL_PLUS_AI:
                        analyst_size_mult, analyst_stop_mult = {
                            "BULLISH": (1.2, 1.1),
                            "NEUTRAL": (1.0, 1.0),
                            "WEAK_BEARISH": (0.8, 0.9),
                            "STRONG_DOWNTREND": (0.6, 0.8),
                            "HIGH_VOLATILITY": (0.4, 0.5),
                        }.get(analyst_signal, (1.0, 1.0))
                        if analyst_signal != "NEUTRAL":
                            self._ai_would_have_resized += 1
                    else:
                        analyst_size_mult = 1.0
                        analyst_stop_mult = 1.0
                    ct_signal = analyst_signal if self.trading_mode == TradingMode.TECHNICAL_PLUS_AI else "NEUTRAL"
                    ct_score = self.strategist.evaluate_countertrend_scalp(state.symbol, ct_signal)
                    ct_conf = analyst_conf if self.trading_mode == TradingMode.TECHNICAL_PLUS_AI else 100
                    if regime.startswith("trending"):
                        now_ts = asyncio.get_event_loop().time()
                        if state.continuation_cooldown > now_ts:
                            remaining = int(state.continuation_cooldown - now_ts)
                            self._log("RISK", f"{state.symbol} anti-churn: continuation blocked {remaining}s remaining")
                            await asyncio.sleep(10)
                            continue
                        ep_cfg = self.config.get("entry_paths", {}).get(state.symbol, {})
                        has_ep = any(ep_cfg.values()) if ep_cfg else False
                        if self.strategist.should_enter(state.symbol) and (not has_ep or ep_cfg.get("continuation", False)):
                            self._signal_count += 1
                            self._log("SIGNAL", f"{state.symbol} continuation: ADX {ec.get('adx',0):.1f}, RSI {ec.get('rsi',0):.1f}, >50EMA={ec.get('price_above_50_ema',False)}, >200EMA={ec.get('price_above_200_ema',False)}")
                            ok, why = await self._trend_preflight(state, "trend_continuation")
                            if not ok:
                                log_dec("SKIP", why, vetos=[why])
                                await asyncio.sleep(60)
                                continue
                            ai_v = await self._ai_veto(state.symbol, "continuation", ec, regime)
                            if ai_v == "VETO":
                                log_dec("AI_VETO", "ai_veto_continuation")
                                await asyncio.sleep(60)
                                continue
                            if ai_v == "REDUCE":
                                state._ai_size_mult = 0.5
                                log_dec("AI_REDUCE", "ai_reduce_continuation")
                            if not await self._acquire_slot(state, "trend_continuation"):
                                log_dec("BLOCKED", "no_budget_slot", vetos=["SLOT_FULL"])
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            try:
                                log_dec("ENTER_TREND_ATTEMPT", "trend_continuation")
                                self._exec_count += 1
                                await self._save_snapshot(state, "ENTER_TREND_CONTINUATION")
                                self._log("TRADE", f"{state.symbol} trend continuation entry")
                                await self.enter_trend_position(state)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", "trend_continuation")
                                else:
                                    log_dec("SKIP", "continuation_not_placed", vetos=["ENTRY_FAILED"])
                            except Exception as e:
                                self._log("ERROR", f"{state.symbol} continuation entry failed: {e}")
                                await self._release_slot(state, "trend_exception")
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "continuation_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(120)
                            continue
                        pb = ec.get("trend_pullback", False)
                        bo = ec.get("trend_breakout", False)
                        if bo:
                            rsi_bo = ec.get("rsi", 50)
                            adx_bo = ec.get("adx", 0)
                            _rsi_caps = {"BTC/USDT": 65, "ETH/USDT": 0}
                            rsi_cap = _rsi_caps.get(state.symbol, 62)
                            if rsi_cap == 0:
                                self._log("SIGNAL", f"{state.symbol} breakout blocked: disabled")
                                log_dec("SKIP", "breakout_disabled", vetos=["BREAKOUT_DISABLED"])
                                bo = False
                            elif rsi_bo > rsi_cap and adx_bo < 40:
                                self._log("SIGNAL", f"{state.symbol} breakout blocked: rsi_{rsi_bo:.0f}>{rsi_cap}_adx_{adx_bo:.0f}<40")
                                log_dec("SKIP", f"breakout_rsi{rsi_bo:.0f}_cap{rsi_cap}", vetos=["BREAKOUT_RSI_CAP"])
                                bo = False
                        if pb and has_ep and not ep_cfg.get("pullback", False):
                            pb = False
                        if bo and has_ep and not ep_cfg.get("breakout", False):
                            bo = False
                        if pb or bo:
                            ok, why = await self._trend_preflight(state, "trend_entry")
                            if not ok:
                                log_dec("SKIP", why, vetos=[why])
                                await asyncio.sleep(60)
                                continue
                            pb_reason = "trend_breakout" if bo else "trend_pullback"
                            ai_v = await self._ai_veto(state.symbol, pb_reason, ec, regime)
                            if ai_v == "VETO":
                                log_dec("AI_VETO", f"ai_veto_{pb_reason}")
                                await asyncio.sleep(60)
                                continue
                            if ai_v == "REDUCE":
                                state._ai_size_mult = 0.5
                                log_dec("AI_REDUCE", f"ai_reduce_{pb_reason}")
                            if not await self._acquire_slot(state, "trend_entry"):
                                log_dec("BLOCKED", "no_budget_slot", vetos=["SLOT_FULL"])
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            try:
                                reason = "trend_breakout" if bo else "trend_pullback"
                                log_dec("ENTER_TREND_ATTEMPT", reason)
                                self._exec_count += 1
                                await self._save_snapshot(state, "ENTER_TREND")
                                self._log("TRADE", f"{state.symbol} {reason} entry")
                                await self.enter_trend_position(state)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", reason)
                                else:
                                    log_dec("SKIP", f"{reason}_not_placed", vetos=["ENTRY_FAILED"])
                            except Exception as e:
                                self._log("ERROR", f"{state.symbol} {reason} entry failed: {e}")
                                await push_activity(f"{state.symbol} {reason} entry failed: {e}", "error")
                                await self._release_slot(state, "trend_exception")
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "trend_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(120)
                            continue
                        # Try sideway strategies in trending regime too
                        ep_cfg = self.config.get("entry_paths", {}).get(state.symbol, {})
                        sw_entry = await self._check_sideway_entry(state.symbol, ec, ep_cfg)
                        if sw_entry:
                            log_dec("ENTER_TREND_ATTEMPT", "sideway_strategy")
                            ok, why = await self._trend_preflight(state, "sideway_entry")
                            if not ok:
                                log_dec("SKIP", why, vetos=[why])
                                await asyncio.sleep(60)
                                continue
                            ai_v = await self._ai_veto(state.symbol, sw_entry, ec, regime)
                            if ai_v == "VETO":
                                log_dec("AI_VETO", f"ai_veto_{sw_entry}")
                                await asyncio.sleep(60)
                                continue
                            if ai_v == "REDUCE":
                                state._ai_size_mult = 0.5
                                log_dec("AI_REDUCE", f"ai_reduce_{sw_entry}")
                            if not await self._acquire_slot(state, "sideway_entry"):
                                log_dec("BLOCKED", "no_budget_slot", vetos=["SLOT_FULL"])
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            try:
                                state.entry_type = sw_entry
                                self._exec_count += 1
                                await self._save_snapshot(state, "ENTER_SIDEWAY")
                                self._log("TRADE", f"{state.symbol} {sw_entry} entry")
                                tp_pct, sl_pct = {"bb_squeeze": (0.008, 0.004), "trend_bounce": (0.005, 0.004), "scalping_5m": (0.006, 0.004), "scalp_original": (0.006, 0.004), "ema50_bounce": (0.008, 0.004), "lowvol_scalp": (0.004, 0.002), "lowvol_momentum": (0.004, 0.002), "supertrend": (0.012, 0.006), "vwap_revert": (0.008, 0.003)}.get(sw_entry, (0.008, 0.004))
                                await self.enter_trend_position(state, fixed_tp=tp_pct, fixed_sl=sl_pct)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", f"{sw_entry}_placed")
                                else:
                                    log_dec("SKIP", f"{sw_entry}_not_placed", vetos=["ENTRY_FAILED"])
                            except Exception as e:
                                self._log("ERROR", f"{state.symbol} {sw_entry} entry failed: {e}")
                                await self._release_slot(state, "sideway_exception")
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "sideway_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        vetos = []
                        if not pb and not bo:
                            vetos.append("NO_PULLBACK_BREAKOUT")
                        if ec.get("rsi", 50) > 65:
                            vetos.append("RSI_OVERHEATED")
                        if ec.get("rsi", 50) < 35:
                            vetos.append("RSI_OVERSOLD")
                        if not ec.get("price_above_50_ema"):
                            vetos.append("BELOW_50_EMA")
                        if not ec.get("price_above_200_ema"):
                            vetos.append("BELOW_200_EMA")
                        adx_val = ec.get("adx", 0)
                        if adx_val <= 35:
                            vetos.append(f"ADX_WEAK({adx_val:.0f})")
                        log_dec("CASH", "trending_no_setup", vetos=vetos if vetos else None)
                        await asyncio.sleep(60)
                        continue
                    elif regime == "high_vol":
                        if await self._check_filter_override("HIGH_VOLATILITY"):
                            self._log("RISK", f"{state.symbol} HIGH_VOL overridden by /filter")
                        else:
                            log_dec("BLOCKED", "regime_high_volatility", vetos=["HIGH_VOLATILITY"])
                            await asyncio.sleep(120)
                    elif regime == "sideways":
                        # Check sideway strategies (bb_squeeze, trend_bounce)
                        ep_cfg = self.config.get("entry_paths", {}).get(state.symbol, {})
                        sw_entry = await self._check_sideway_entry(state.symbol, ec, ep_cfg)
                        if sw_entry:
                            log_dec("ENTER_TREND_ATTEMPT", "sideway_strategy")
                            ok, why = await self._trend_preflight(state, "sideway_entry")
                            if not ok:
                                log_dec("SKIP", why, vetos=[why])
                                await asyncio.sleep(60)
                                continue
                            ai_v = await self._ai_veto(state.symbol, sw_entry, ec, regime)
                            if ai_v == "VETO":
                                log_dec("AI_VETO", f"ai_veto_{sw_entry}")
                                await asyncio.sleep(60)
                                continue
                            if ai_v == "REDUCE":
                                state._ai_size_mult = 0.5
                                log_dec("AI_REDUCE", f"ai_reduce_{sw_entry}")
                            if not await self._acquire_slot(state, "sideway_entry"):
                                log_dec("BLOCKED", "no_budget_slot", vetos=["SLOT_FULL"])
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            try:
                                state.entry_type = sw_entry
                                self._exec_count += 1
                                await self._save_snapshot(state, "ENTER_SIDEWAY")
                                self._log("TRADE", f"{state.symbol} {sw_entry} entry")
                                tp_pct, sl_pct = {"bb_squeeze": (0.008, 0.004), "trend_bounce": (0.005, 0.004), "scalping_5m": (0.006, 0.004), "scalp_original": (0.006, 0.004), "ema50_bounce": (0.008, 0.004), "lowvol_scalp": (0.004, 0.002), "lowvol_momentum": (0.004, 0.002), "supertrend": (0.012, 0.006), "vwap_revert": (0.008, 0.003)}.get(sw_entry, (0.008, 0.004))
                                await self.enter_trend_position(state, fixed_tp=tp_pct, fixed_sl=sl_pct)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", f"{sw_entry}_placed")
                                else:
                                    log_dec("SKIP", f"{sw_entry}_not_placed", vetos=["ENTRY_FAILED"])
                            except Exception as e:
                                self._log("ERROR", f"{state.symbol} {sw_entry} entry failed: {e}")
                                await self._release_slot(state, "sideway_exception")
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "sideway_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        if self._regime_mode in ("countertrend", "auto") and ct_score >= 60:
                            allowed, ct_risk = self.strategist.evaluate_countertrend_entry(state.symbol, ct_score)
                            if not allowed or ct_risk is None:
                                ct_vetos = [f"CT_SCORE_{ct_score}_BLOCKED"]
                                if analyst_signal != "NEUTRAL":
                                    ct_vetos.append(f"ANALYST_{analyst_signal}")
                                if news_size_mult < 1.0:
                                    ct_vetos.append(f"NEWS_x{news_size_mult}")
                                log_dec("BLOCKED", "countertrend_not_allowed", vetos=ct_vetos)
                                if ct_score >= 60:
                                    self._log("SIGNAL", f"{state.symbol} countertrend candidate ct={ct_score} but blocked")
                                await asyncio.sleep(60)
                                continue
                            ok, why = await self._trend_preflight(state, "countertrend_entry")
                            if not ok:
                                log_dec("SKIP", why, vetos=[why])
                                await asyncio.sleep(60)
                                continue
                            ai_v = await self._ai_veto(state.symbol, "countertrend", ec, regime)
                            if ai_v == "VETO":
                                log_dec("AI_VETO", "ai_veto_countertrend")
                                await asyncio.sleep(60)
                                continue
                            if ai_v == "REDUCE":
                                state._ai_size_mult = 0.5
                                log_dec("AI_REDUCE", "ai_reduce_countertrend")
                            if not await self._acquire_slot(state, "countertrend_entry"):
                                log_dec("BLOCKED", "no_budget_slot", vetos=["SLOT_FULL"])
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            state._ct_risk = ct_risk
                            state._analyst_size_mult = analyst_size_mult
                            state._news_size_mult = news_size_mult
                            try:
                                log_dec("ENTER_TREND_ATTEMPT", f"countertrend_score_{ct_score}")
                                self._exec_count += 1
                                await self._save_snapshot(state, "ENTER_COUNTERTREND")
                                self._log("TRADE", f"{state.symbol} countertrend entry ct={ct_score}")
                                await self.enter_trend_position(state)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", f"countertrend_score_{ct_score}")
                                else:
                                    log_dec("SKIP", f"countertrend_not_placed_{ct_score}", vetos=["ENTRY_FAILED"])
                            except Exception as e:
                                self._log("ERROR", f"{state.symbol} countertrend entry failed: {e}")
                                await self._release_slot(state, "countertrend_exception")
                                state._ct_risk = None
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "countertrend_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        elif ct_score >= 60:
                            self._log("SIGNAL", f"{state.symbol} countertrend ct={ct_score} but regime_mode not active")
                        else:
                            side_vetos = [f"CT_LOW({ct_score})"]
                            if not ec.get("price_at_lower_bb"):
                                side_vetos.append("NOT_AT_BB")
                            if analyst_signal != "NEUTRAL":
                                side_vetos.append(f"ANALYST_{analyst_signal}")
                            if news_size_mult < 1.0:
                                side_vetos.append(f"NEWS_x{news_size_mult}")
                            log_dec("CASH", f"sideways_no_entry_cscore_{ct_score}", vetos=side_vetos)
                        await asyncio.sleep(60)
                        continue
                    if self.config.get("grid", {}).get("enabled", True) and self.strategist.should_enter(state.symbol):
                        ep_cfg = self.config.get("entry_paths", {}).get(state.symbol, {})
                        if any(ep_cfg.values()):
                            continue
                        self._signal_count += 1
                        self._log("SIGNAL", f"{state.symbol} grid entry candidate (regime={regime})")
                        ai_v = await self._ai_veto(state.symbol, "grid_entry", ec, regime)
                        if ai_v == "VETO":
                            log_dec("AI_VETO", "ai_veto_grid")
                            await asyncio.sleep(60)
                            continue
                        if ai_v == "REDUCE":
                            state._ai_size_mult = 0.5
                            log_dec("AI_REDUCE", "ai_reduce_grid")
                        if not await self._acquire_slot(state, "grid_entry"):
                            log_dec("BLOCKED", "no_budget_slot", vetos=["SLOT_FULL"])
                            await asyncio.sleep(60)
                            continue
                        state.last_entry_attempt = now
                        try:
                            self._exec_count += 1
                            log_dec("ENTER_GRID", "grid_entry")
                            self._last_normal_trade = now
                            self._log("TRADE", f"{state.symbol} grid entry (regime={regime})")
                            await self._save_snapshot(state, "ENTER_GRID")
                            ticker = await self.exchange.watch_ticker(state.symbol)
                            state.levels = await self.calculate_grid_levels(state, ticker["last"])
                            state.filled_cost = 0.0
                            state.filled_qty = 0.0
                            await self.place_grid_orders(state, state.levels)
                            if state.levels:
                                state.is_active = True
                                state.last_rebalance = asyncio.get_event_loop().time()
                                asyncio.create_task(self.watch_order_fills(state))
                                asyncio.create_task(self.check_exit_conditions(state))
                        except Exception:
                            await self._release_slot(state, "grid_exception")
                else:
                    now = asyncio.get_event_loop().time()
                    if (now - state.last_rebalance) > (self.config["strategy"]["rebalance_interval_hours"] * 3600):
                        try:
                            if self.manage_only_bot_orders:
                                await self.exchange.cancel_bot_orders(state.symbol, self.client_id_prefix)
                            else:
                                await self.exchange.cancel_all_orders(state.symbol)
                        except Exception:
                            pass
                        ticker = await self.exchange.watch_ticker(state.symbol)
                        state.levels = await self.calculate_grid_levels(state, ticker["last"])
                        await self.place_grid_orders(state, state.levels)
                        state.last_rebalance = now
                        await self.notifier.send_message(f"🔄 {state.symbol} rebalanced")
            except Exception as e:
                self._log("ERROR", f"manage_pair ({state.symbol}): {e}")
                await push_activity(f"manage_pair error ({state.symbol}): {e}", "error")
            await asyncio.sleep(10)

    async def run(self):
        print(f"Starting executor for {len(self.all_pairs)} configured pairs")
        await push_activity(f"Starting executor for {len(self.all_pairs)} pairs")
        self._daily_loss_notified = False
        self._kill_in_progress = False
        await self._connect_redis()
        if self.redis:
            init_activity(self.redis)
            await self.redis.set("vortex:trading_mode", self.trading_mode.value)
            await self.redis.set("vortex:plan:deploy_time", datetime.now(timezone.utc).isoformat())
        try:
            balance = await self.exchange.fetch_balance()
            actual_total = float(balance["USDT"]["free"]) + float(balance["USDT"].get("used", 0))
            simulated = os.getenv("SIMULATED_BALANCE")
            if simulated:
                total = float(simulated)
                sim_val = float(simulated)
                pnl_total = 0.0
                with self.db.conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
                    pnl_total = float(cur.fetchone()[0])
                display_total = sim_val + pnl_total
                print(f"  ⚠️ Simulated balance: ${display_total:.2f}")
                # Allocator uses the actual balance (capped by sim) for slot viability
                alloc_total = min(actual_total, sim_val)
                # Initialize budget_remaining if not set
                try:
                    if self.redis:
                        exists = await self.redis.exists("vortex:budget_remaining")
                        if not exists:
                            await self.redis.set("vortex:budget_remaining", str(sim_val))
                except Exception:
                    pass
                prev = await self.redis.get("vortex:simulated_balance:last") if self.redis else None
                now_val = str(float(simulated))
                reset_on_start = self._env_bool("SIM_RESET_ON_START", self.config.get("simulation", {}).get("reset_on_start", False))
                reset_on_change = self._env_bool("SIM_RESET_ON_CHANGE", self.config.get("simulation", {}).get("reset_on_change", False))
                if reset_on_start or (reset_on_change and prev is not None and prev != now_val):
                    print(f"  🔄 Simulation reset requested")
                    await push_activity("Simulation reset triggered")
                    await self._reset_simulation()
                    total = float(simulated)
                if self.redis:
                    await self.redis.set("vortex:simulated_balance:last", now_val)
            else:
                prev = await self.redis.get("vortex:simulated_balance:last") if self.redis else None
                reset_on_disable = self._env_bool("SIM_RESET_ON_DISABLE", self.config.get("simulation", {}).get("reset_on_disable", False))
                if prev is not None and reset_on_disable:
                    print(f"  🔄 Simulation removed, resetting state")
                    await push_activity("Simulation disabled, state reset")
                    await self._reset_simulation()
                    if self.redis:
                        await self.redis.delete("vortex:simulated_balance:last")
                alloc_total = actual_total
            alloc_cfg = self.config.get("allocator", {})
            self.allocator = BudgetAllocator(alloc_total, alloc_cfg, len(self.all_pairs))
            self.pair_budget = self.allocator.budget_per_slot
            print(f"  Balance: ${alloc_total:.2f} | Slots: {self.allocator.slots} | "
                  f"Budget/slot: ${self.pair_budget:.2f} | Reserve: ${self.allocator.reserve:.2f}")
            await push_activity(f"Balance: ${alloc_total:.2f} | {self.allocator.slots} slots @ ${self.pair_budget:.2f}/slot")
            for symbol in self.all_pairs:
                st = GridState(symbol, self.config)
                st.pair_budget = self.pair_budget
                try:
                    st.min_notional = self.exchange.get_min_notional(symbol)
                except Exception:
                    st.min_notional = 10.0
                self.states[symbol] = st
            # Load any active positions from Redis before cancel/publish
            try:
                raw = await self.redis.get("vortex:grid_state") if self.redis else None
                if raw:
                    saved = json.loads(raw)
                    for sym, state_data in saved.items():
                        st = self.states.get(sym)
                        if st and state_data.get("trend_active"):
                            st.trend_active = True
                            st.trend_entry_price = float(state_data.get("trend_entry", 0))
                            st.trend_stop = float(state_data.get("trend_stop", 0))
                            st.trend_target = float(state_data.get("trend_target", 0))
                            st.trend_size = float(state_data.get("trend_size", 0))
                            st.is_active = state_data.get("is_active", False)
                            st.entry_type = state_data.get("entry_type", "")
                            if self.allocator:
                                st.slot_acquired = await self.allocator.acquire(sym)
                            else:
                                st.slot_acquired = True
                            asyncio.create_task(self._position_monitor(st))
            except Exception:
                pass
            if self.cancel_bot_orders_on_start:
                for state in self.states.values():
                    if self.manage_only_bot_orders:
                        await self.exchange.cancel_bot_orders(state.symbol, self.client_id_prefix)
                    else:
                        await self.exchange.cancel_all_orders(state.symbol)
            if self.sweep_on_start or self._env_bool("SIM_SWEEP_ON_START", False):
                await self._sweep_leftover_coins()
            print(f"Monitoring {len(self.all_pairs)} pairs: {', '.join(self.all_pairs)}")
            await push_activity(f"Monitoring {len(self.all_pairs)} pairs: {', '.join(self.all_pairs)}")
        except Exception as e:
            print(f"run init error: {e}")
            await push_activity(f"Run init error: {e}", "error")
            return
        await self._record_balance()
        await self._publish_orders()
        async def publish_loop():
            await asyncio.sleep(5)
            while True:
                try:
                    # Read breakout toggle from Redis
                    try:
                        bo = await self.redis.get("vortex:breakout") if self.redis else None
                        if bo == "true":
                            self.strategist.allow_breakout_override = True
                        elif bo == "false":
                            self.strategist.allow_breakout_override = False
                        else:
                            self.strategist.allow_breakout_override = None
                    except Exception:
                        self.strategist.allow_breakout_override = None
                    # Read trading mode from Redis
                    try:
                        mode_raw = await self.redis.get("vortex:trading_mode") if self.redis else None
                        if mode_raw:
                            try:
                                new_mode = TradingMode(mode_raw)
                                if new_mode != self.trading_mode:
                                    self._log("STATE", f"Trading mode: {self.trading_mode.value} → {new_mode.value}")
                                    self.trading_mode = new_mode
                            except ValueError:
                                pass
                    except Exception:
                        pass
                    await self._check_auto_regime()
                    await self._publish_conditions()
                    await self._publish_orders()
                    await self._fetch_fear_greed()
                except Exception as e:
                    print(f"publish_loop: {e}")
                    await push_activity(f"Publish error: {e}", "error")
                await asyncio.sleep(10)
        async def balance_loop():
            while True:
                await asyncio.sleep(3600)
                await self._record_balance()
        async def analyst_refresh_loop():
            await asyncio.sleep(30)
            while True:
                try:
                    if self.trading_mode == TradingMode.TECHNICAL_ONLY:
                        await asyncio.sleep(300)
                        continue
                    if self.analyst:
                        for symbol, st in self.states.items():
                            if not st.last_analyst_verdict or st.last_analyst_verdict.get("verdict") == "":
                                ec = self.strategist.entry_conditions.get(symbol, {})
                                df = self.strategist.data.get(symbol, {}).get(self.strategist.timeframes["entry"])
                                try:
                                    verdict = await asyncio.wait_for(
                                        self.analyst.should_enter(symbol, df, ec), timeout=10)
                                    st.last_analyst_verdict = verdict
                                except (asyncio.TimeoutError, Exception) as e:
                                    print(f"  Analyst refresh timeout ({symbol}): {e}")
                            await asyncio.sleep(15)
                except Exception as e:
                    print(f"analyst_refresh_loop: {e}")
                    await push_activity(f"Analyst refresh error: {e}", "error")
                await asyncio.sleep(300)
        self._log("STATE", f"auto_profile: {'enabled' if self.auto_profile_enabled else 'disabled'}")
        if self.auto_profile_enabled:
            asyncio.create_task(self._auto_profile_loop())
        asyncio.create_task(balance_loop())
        asyncio.create_task(publish_loop())
        asyncio.create_task(analyst_refresh_loop())
        tasks = []
        for s in self.all_pairs:
            tasks.append(self.manage_pair(self.states[s]))
            await asyncio.sleep(1)
        await asyncio.gather(*tasks, return_exceptions=True)
