import asyncio
import itertools
import json
import os
from datetime import datetime, timezone
from redis import asyncio as aioredis
from exchange_wrapper import ExchangeWrapper
from strategist import Strategist
from notifier import Notifier
from db import TimescaleDB
from analyst import Analyst
from news_filter import NewsFilter
from activity import push_activity, init_activity
from typing import List, Dict, Optional

class BudgetAllocator:
    def __init__(self, total_balance: float, alloc_cfg: dict, pair_count: int):
        reserve_pct = alloc_cfg.get("reserve_pct", 0.20)
        min_per_slot = alloc_cfg.get("min_per_slot", 50)
        max_budget_pct = alloc_cfg.get("max_budget_pct", 0.10)

        deployable = total_balance * (1 - reserve_pct)
        max_budget = max(deployable * max_budget_pct, min_per_slot)
        self.slots = min(pair_count, max(1, int(deployable / min_per_slot)))
        raw_budget = deployable / self.slots if self.slots > 0 else 0
        self.budget_per_slot = round(min(max_budget, raw_budget), 2)
        self.reserve = round(total_balance - (self.budget_per_slot * self.slots), 2)
        self.used = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            if self.used < self.slots:
                self.used += 1
                return True
            return False

    async def release(self):
        async with self._lock:
            self.used = max(0, self.used - 1)

    async def reconcile_used(self, used: int):
        async with self._lock:
            self.used = max(0, min(int(used), int(self.slots)))


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
        self.last_analyst_verdict: Optional[dict] = None
        self.trend_active = False
        self.trend_entry_price = 0.0
        self.trend_stop = 0.0
        self.trend_target = 0.0
        self.trend_size = 0.0
        self.trend_high = 0.0
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
        self.trend_entry_timeout = execution_cfg.get("trend_entry_timeout_seconds", 300)
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
            self.db.log_decision(symbol, event, tag)
        except Exception:
            pass

    async def _acquire_slot(self, state: 'GridState', reason: str) -> bool:
        if not self.allocator:
            return True
        ok = await self.allocator.acquire()
        if ok:
            state.slot_acquired = True
            self._log_slot_event(state.symbol, "SLOT_ACQUIRE", reason)
        return ok

    async def _release_slot(self, state: 'GridState', reason: str):
        if state.slot_acquired and self.allocator:
            try:
                await self.allocator.release()
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
            if usable:
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
        any_entry_attempted = any(self._last_normal_trade > 0 for _ in self.states)

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
                    "trend_uptrend": ec.get("trend_uptrend", False),
                    "trend_pullback": ec.get("trend_pullback", False),
                    "bb_lower": ec.get("price_at_lower_bb", False),
                    "above_ema200": ec.get("price_above_200_ema", False),
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
                    "change": change,
                }
            cleaned = json.loads(json.dumps(data, default=lambda x: float(x) if hasattr(x, 'item') else str(x)))
            cleaned["_meta"] = {"regime_mode": self._regime_mode, "breakout": self.strategist.allow_breakout_override is True}
            await self.redis.set("vortex:conditions", json.dumps(cleaned))
            await self.redis.expire("vortex:conditions", 30)
        except Exception as e:
            print(f"_publish_conditions error: {e}")

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
            if not await self.redis.exists("vortex:balance:initial"):
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
            }
        try:
            await self.redis.set("vortex:grid_state", json.dumps(data))
            await self.redis.expire("vortex:grid_state", 3600)
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
        if self.redis:
            try:
                if await self.redis.exists("vortex:loss_limit_hit"):
                    return True
            except Exception:
                pass
        daily_pnl = self.db.get_daily_pnl()
        max_loss_pct = self.config["risk"].get("max_daily_loss_percent", 5)
        initial = await self._get_initial_balance()
        if initial > 0 and daily_pnl < 0 and abs(daily_pnl) >= initial * (max_loss_pct / 100):
            if not self._daily_loss_notified:
                await self.notifier.send_message(f"🚨 Daily loss limit ({max_loss_pct}%) hit: ${daily_pnl:.2f}")
                self._daily_loss_notified = True
            await self.trigger_kill_switch()
            return True
        return False

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

    async def _reset_simulation(self):
        msg = ""
        if self.redis:
            try:
                keys = await self.redis.keys("vortex:*")
                for key in keys:
                    if key != "vortex:simulated_balance:last":
                        await self.redis.delete(key)
                msg += "Redis state cleared. "
            except Exception as e:
                print(f"_reset_simulation redis: {e}")
        try:
            with self.db.conn.cursor() as cur:
                cur.execute("TRUNCATE trades, balance_snapshots, trade_decisions RESTART IDENTITY CASCADE")
            msg += "Trade history reset."
        except Exception as e:
            print(f"_reset_simulation db: {e}")
        try:
            for pair_name in self.all_pairs:
                try:
                    await self.exchange.cancel_all_orders(pair_name)
                except Exception:
                    pass
            msg += "Orders cancelled. "
            bal = await self.exchange.fetch_balance()
            for pair_name in self.all_pairs:
                base = pair_name.split("/")[0]
                free = float(bal.get(base, {}).get("free", 0))
                if free > 0:
                    try:
                        ticker = await asyncio.wait_for(self.exchange.fetch_ticker(pair_name), timeout=5)
                        await self.exchange.create_market_sell_order(pair_name, free)
                        value = round(free * float(ticker["last"]), 2)
                        msg += f"{base} {value:.2f} sold. "
                    except Exception as e:
                        print(f"  sell {base}: {e}")
        except Exception as e:
            print(f"_reset_simulation sell: {e}")
        print(f"  🧹 Simulation state reset complete")
        await push_activity("Simulation state reset complete")
        if self.redis:
            try:
                await self.redis.setex("vortex:notification", 60, msg)
            except Exception:
                pass
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
        min_net = self.config["risk"].get("min_net_profit_percent", 0.1) / 100
        if net_per_flip < min_net:
            await self.notifier.send_message(
                f"⛔ {state.symbol} blocked: {gross_per_flip*100:.2f}% width "
                f"- {2*maker_fee*100:.2f}% fees = {net_per_flip*100:.2f}% net "
                f"< {min_net*100:.2f}% minimum"
            )
            if state.slot_acquired and self.allocator:
                await self.allocator.release()
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
                await self.allocator.release()
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
            await self.allocator.release()
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
        await asyncio.sleep(300)
        while state.is_active:
            if self.strategist.should_exit_take_profit(state.symbol):
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
                        await self.allocator.release()
                    state.slot_acquired = False
                break
            if state.levels:
                lowest = min(l["price"] for l in state.levels if l["type"] == "buy")
                ticker = await self.exchange.watch_ticker(state.symbol)
                state_atr = self.strategist.entry_conditions.get(state.symbol, {}).get("atr", 0)
                atr_mult = self.config["strategy"]["exit"]["stop_loss"].get("atr_multiplier", 1.5)
                pct_stop = lowest * (1 - self.config["strategy"]["exit"]["stop_loss"]["percent_below_lowest_grid"] / 100)
                if state_atr > 0:
                    stop = max(pct_stop, lowest - (state_atr * atr_mult))
                else:
                    stop = pct_stop
                if ticker["last"] < stop:
                    await self.notifier.send_message(f"🛑 {state.symbol} SL triggered: {ticker['last']} < {stop}")
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
                self.db.log_trade({
                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                    "side": "sell", "price": market_price, "quantity": sell_qty,
                    "order_id": order.get("id"), "status": "closed",
                    "grid_level": None, "realized_pnl": pnl, "fee_cost": fee,
                })
            except Exception as e:
                self.db.log_trade({
                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                    "side": "sell", "price": state.trend_entry_price if state.trend_entry_price else 0,
                    "quantity": sell_qty, "order_id": None, "status": "closed",
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
            await self.allocator.release()
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

    async def enter_trend_position(self, state: GridState):
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
        try:
            ticker = await asyncio.wait_for(self.exchange.watch_ticker(state.symbol), timeout=5)
            ticker_ok = True
        except Exception:
            ticker_ok = False
        if ticker_ok:
            if ec.get("trend_breakout") or ec.get("regime") == "sideways":
                entry_price = float(ticker["ask"])
            else:
                best_bid = float(ticker["bid"])
                entry_price = round(best_bid * 1.001, 4)
                ema_20 = ec.get("ema_20", 0)
                if ema_20 > 0 and entry_price > ema_20 * 1.01:
                    # For countertrend, be patient: park a limit near EMA20 instead of chasing.
                    if state._ct_risk is not None:
                        entry_price = round(float(ema_20), 4)
                        self.db.log_decision(
                            state.symbol,
                            "INFO",
                            f"countertrend_cap_entry_to_ema20:{entry_price}",
                            ec.get("regime", ""),
                            ec.get("adx", 0),
                            ec.get("atr", 0),
                            ec.get("rsi", 0),
                            entry_price,
                            0,
                        )
                    else:
                        entry_price = 0
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
            if adx > 30:
                order = await self.exchange.create_market_buy_order(state.symbol, size, client_id)
                fill_price = self._order_avg_price(order) or float(order.get("price") or 0)
                if fill_price > 0:
                    entry_price = fill_price
                    state.trend_entry_pending = False
                    state.trend_active = True
                    state.trend_entry_price = fill_price
                    state.trend_size = size
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
                    await self.notifier.send_message(f"🔥 {state.symbol} market trend buy @ ${fill_price:.4f} | Trail SL: ${state.trend_stop:.4f}")
                    state.bullets_fired = 1
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
            state.trend_stop = entry_price - (state.atr * trail_atr)
            state.trend_target = entry_price + (state.atr * tp_atr)
            state.trend_high = entry_price
            self.db.log_trade({
                "timestamp": order["timestamp"], "pair": state.symbol,
                "side": "buy", "price": order["price"], "quantity": order["amount"],
                "order_id": order.get("id"), "status": order["status"],
                "grid_level": None, "realized_pnl": None,
            })
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
                if self.trend_entry_timeout and (asyncio.get_event_loop().time() - state.trend_entry_started) > self.trend_entry_timeout:
                    ec = self.strategist.entry_conditions.get(state.symbol, {})
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
                        await self.allocator.release()
                        state.slot_acquired = False
                    await self.notifier.send_message(f"⌛ {state.symbol} trend entry timed out; slot released")
                    waited = int((asyncio.get_event_loop().time() - state.trend_entry_started) / 60)
                    self.db.log_decision(state.symbol, "PENDING_TIMEOUT",
                        f"@${state.trend_entry_price:.2f} waited {waited}min",
                        "", 0, 0, 0, state.trend_entry_price, 0)
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
                    "no free coins to sell", "", 0, 0, 0, state.trend_entry_price, 0)
                state.trend_active = False
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
                f"PnL ${pnl:+.2f} @ ${exit_price:.4f}", "", 0, 0, 0, exit_price, 0)
            await self.notifier.send_message(f"{'✅' if pnl >= 0 else '🛑'} {state.symbol} trend {reason.upper()} exit @ ${exit_price:.4f}: ${pnl:+.2f} (fee ${total_fee:.4f})")
        except Exception as e:
            await self.notifier.send_message(f"⚠️ {state.symbol} trend exit failed: {e}")
            return
        state.trend_active = False
        state.trend_entry_pending = False
        state.trend_size = 0.0
        if state.slot_acquired and self.allocator:
            await self.allocator.release()
            state.slot_acquired = False
        await self._publish_orders()

    async def trail_trend_position(self, state: GridState):
        await asyncio.sleep(10)
        profile_params = self.strategist.get_profile_params(state.symbol)
        trail_mult = profile_params.get("sl_atr", 1.5)
        try:
            while state.trend_active:
                try:
                    ticker = await self.exchange.watch_ticker(state.symbol)
                    price = float(ticker.get("bid") or ticker["last"])
                    if price > state.trend_high:
                        state.trend_high = price
                        state.trend_stop = max(state.trend_stop, price - (state.atr * trail_mult))
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

    async def trigger_kill_switch(self):
        if self._kill_in_progress:
            return
        self._kill_in_progress = True
        for state in list(self.states.values()):
            try:
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
                    print(f"  [{state.symbol}] cycle start now={now:.0f} last_attempt={state.last_entry_attempt:.0f} cooldown={state.cooldown_until:.0f}")
                    if await self._check_daily_loss():
                        return
                    if state.cooldown_until > now:
                        print(f"  [{state.symbol}] gate: cooldown ({state.cooldown_until:.0f} > {now:.0f})")
                        await asyncio.sleep(30)
                        continue
                    if (now - state.last_entry_attempt) < 120:
                        print(f"  [{state.symbol}] gate: last_entry_attempt ({now - state.last_entry_attempt:.0f} < 120)")
                        await asyncio.sleep(10)
                        continue
                    if self.allocator and self.allocator.used >= self.allocator.slots:
                        print(f"  [{state.symbol}] gate: allocator full ({self.allocator.used}/{self.allocator.slots})")
                        await asyncio.sleep(10)
                        continue
                    if self.strategist.should_exit_trend_inversion(state.symbol):
                        if state.symbol not in getattr(self.strategist, 'PILOT_PAIRS', []):
                            state.last_entry_attempt = 0
                            print(f"  [{state.symbol}] gate: trend_inversion + not PILOT")
                            await asyncio.sleep(300)
                            continue
                    regime = self.strategist.get_regime(state.symbol)
                    ec = self.strategist.entry_conditions.get(state.symbol, {})
                    print(f"  [{state.symbol}] regime={regime} adx={ec.get('adx',0):.1f} rsi={ec.get('rsi',0):.1f} ct_score=...")
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
                    def log_dec(decision, reason):
                        self.db.log_decision(state.symbol, decision, reason, regime,
                            ec.get("adx", 0), ec.get("atr", 0), ec.get("rsi", 0), price, bal)
                    # ── NewsFilter (global risk modifier) ──
                    news_size_mult = 1.0
                    if self.news_filter:
                        try:
                            news = await asyncio.wait_for(
                                self.news_filter.should_trade(state.symbol), timeout=10)
                            if not news.allow_trade:
                                news_size_mult = 0.1
                                print(f"  News event: {news.reason} → size x0.1")
                        except (asyncio.TimeoutError, Exception) as e:
                            print(f"  NewsFilter error: {e} → allow full size")
                    # ── Analyst + ct_score (global) ──
                    analyst_signal = "NEUTRAL"
                    analyst_conf = 0
                    if self.analyst:
                        try:
                            verdict = await asyncio.wait_for(
                                self.analyst.should_enter(state.symbol,
                                    self.strategist.data.get(state.symbol, {}).get(self.strategist.timeframes["entry"]),
                                    ec), timeout=8)
                            state.last_analyst_verdict = verdict
                            analyst_signal = verdict.get("verdict", "NEUTRAL")
                            analyst_conf = verdict.get("confidence", 0)
                            self.strategist.entry_conditions.setdefault(state.symbol, {})["analyst_signal"] = analyst_signal
                        except (asyncio.TimeoutError, Exception) as e:
                            print(f"  Analyst timeout/error: {e} → NEUTRAL")
                            analyst_signal = "NEUTRAL"
                    analyst_size_mult, analyst_stop_mult = {
                        "BULLISH": (1.2, 1.1),
                        "NEUTRAL": (1.0, 1.0),
                        "WEAK_BEARISH": (0.8, 0.9),
                        "STRONG_DOWNTREND": (0.5, 0.7),
                        "HIGH_VOLATILITY": (0.1, 0.5),
                    }.get(analyst_signal, (1.0, 1.0))
                    ct_score = self.strategist.evaluate_countertrend_scalp(state.symbol, analyst_signal)
                    if regime == "trending":
                        pb = ec.get("trend_pullback", False)
                        bo = ec.get("trend_breakout", False)
                        if pb or bo:
                            ok, why = await self._trend_preflight(state, "trend_entry")
                            if not ok:
                                log_dec("SKIP", why)
                                await asyncio.sleep(60)
                                continue
                            if not await self._acquire_slot(state, "trend_entry"):
                                log_dec("BLOCKED", "no_budget_slot")
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            try:
                                reason = "trend_breakout" if bo else "trend_pullback"
                                log_dec("ENTER_TREND_ATTEMPT", reason)
                                await self._save_snapshot(state, "ENTER_TREND")
                                await self.notifier.send_message(f"🎯 {state.symbol} {reason} attempt")
                                await self.enter_trend_position(state)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", reason)
                                else:
                                    log_dec("SKIP", f"{reason}_not_placed")
                            except Exception as e:
                                print(f"  {state.symbol} {reason} entry failed: {e}")
                                await push_activity(f"{state.symbol} {reason} entry failed: {e}", "error")
                                await self.notifier.send_message(f"⛔ {state.symbol} {reason} entry failed: {e}")
                                await self._release_slot(state, "trend_exception")
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "trend_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        bo_enabled = self.strategist.allow_breakout_override if self.strategist.allow_breakout_override is not None else self.config.get("strategy", {}).get("trend", {}).get("allow_breakout", False)
                        bo_reason = f"bo={'on' if bo_enabled else 'off'}"
                        log_dec("CASH", f"trending_no_pullback_{bo_reason}")
                        await asyncio.sleep(60)
                        continue
                    elif regime == "high_vol":
                        if await self._check_filter_override("HIGH_VOLATILITY"):
                            await self.notifier.send_message(f"⚠️ {state.symbol} high vol — overridden by /filter")
                        else:
                            log_dec("BLOCKED", "regime_high_volatility")
                            await self.notifier.send_message(f"⚠️ {state.symbol} high volatility — skipping entry")
                            await asyncio.sleep(120)
                    elif regime == "sideways":
                        if self._regime_mode in ("countertrend", "auto") and ct_score >= 45:
                            allowed, ct_risk = self.strategist.evaluate_countertrend_entry(state.symbol, ct_score, analyst_conf)
                            if not allowed or ct_risk is None:
                                log_dec("BLOCKED", "countertrend_not_allowed")
                                if ct_score >= 45:
                                    log_dec("WATCHLIST", f"ct_score_{ct_score}")
                                await asyncio.sleep(60)
                                continue
                            ok, why = await self._trend_preflight(state, "countertrend_entry")
                            if not ok:
                                log_dec("SKIP", why)
                                await asyncio.sleep(60)
                                continue
                            if not await self._acquire_slot(state, "countertrend_entry"):
                                log_dec("BLOCKED", "no_budget_slot")
                                await asyncio.sleep(60)
                                continue
                            state.last_entry_attempt = now
                            state._ct_risk = ct_risk
                            state._analyst_size_mult = analyst_size_mult
                            state._news_size_mult = news_size_mult
                            try:
                                log_dec("ENTER_TREND_ATTEMPT", f"countertrend_score_{ct_score}")
                                await self._save_snapshot(state, "ENTER_COUNTERTREND")
                                await self.notifier.send_message(f"🎯 {state.symbol} countertrend entry attempt ct={ct_score}")
                                await self.enter_trend_position(state)
                                if state.trend_active or state.trend_entry_pending:
                                    log_dec("ENTER_TREND_PLACED", f"countertrend_score_{ct_score}")
                                else:
                                    log_dec("SKIP", f"countertrend_not_placed_{ct_score}")
                            except Exception as e:
                                print(f"  {state.symbol} countertrend entry failed: {e}")
                                await push_activity(f"{state.symbol} countertrend entry failed: {e}", "error")
                                await self.notifier.send_message(f"⛔ {state.symbol} countertrend entry failed: {e}")
                                await self._release_slot(state, "countertrend_exception")
                                state._ct_risk = None
                            if not state.trend_active and not state.trend_entry_pending:
                                await self._release_slot(state, "countertrend_not_placed")
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        elif ct_score >= 45:
                            log_dec("WATCHLIST", f"ct_score_{ct_score}_needs_{45}")
                        else:
                            log_dec("CASH", f"no_entry_cscore_{ct_score}")
                        await asyncio.sleep(60)
                        continue
                    if self.config.get("grid", {}).get("enabled", True) and self.strategist.should_enter(state.symbol):
                        if not await self._acquire_slot(state, "grid_entry"):
                            log_dec("BLOCKED", "no_budget_slot")
                            await asyncio.sleep(60)
                            continue
                        state.last_entry_attempt = now
                        try:
                            await self.notifier.send_message(f"🚀 {state.symbol} entry conditions met, regime: {regime}")
                            log_dec("ENTER_GRID", "grid_entry")
                            self._last_normal_trade = now
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
                print(f"manage_pair ({state.symbol}): {e} — retrying in 10s")
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
            try:
                await self.redis.delete("vortex:allocator", "vortex:grid_state")
            except Exception:
                pass
        try:
            balance = await self.exchange.fetch_balance()
            total = float(balance["USDT"]["free"]) + float(balance["USDT"].get("used", 0))
            simulated = os.getenv("SIMULATED_BALANCE")
            if simulated:
                total = float(simulated)
                with self.db.conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
                    total += float(cur.fetchone()[0])
                print(f"  ⚠️ Simulated balance: ${total:.2f}")
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
            alloc_cfg = self.config.get("allocator", {})
            self.allocator = BudgetAllocator(total, alloc_cfg, len(self.all_pairs))
            self.pair_budget = self.allocator.budget_per_slot
            print(f"  Balance: ${total:.2f} | Slots: {self.allocator.slots} | "
                  f"Budget/slot: ${self.pair_budget:.2f} | Reserve: ${self.allocator.reserve:.2f}")
            await push_activity(f"Balance: ${total:.2f} | {self.allocator.slots} slots @ ${self.pair_budget:.2f}/slot")
            for symbol in self.all_pairs:
                st = GridState(symbol, self.config)
                st.pair_budget = self.pair_budget
                try:
                    st.min_notional = self.exchange.get_min_notional(symbol)
                except Exception:
                    st.min_notional = 10.0
                self.states[symbol] = st
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
                    await self._check_auto_regime()
                    await self._publish_conditions()
                    await self._publish_orders()
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
        if self.auto_profile_enabled:
            asyncio.create_task(self._auto_profile_loop())
        asyncio.create_task(balance_loop())
        asyncio.create_task(publish_loop())
        asyncio.create_task(analyst_refresh_loop())
        tasks = []
        for s in self.all_pairs:
            tasks.append(self.manage_pair(self.states[s]))
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)
