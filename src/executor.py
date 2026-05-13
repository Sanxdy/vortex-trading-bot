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
        self.trend_entry_pending = False
        self.trend_entry_order_id = ""
        self.trend_entry_client_id = ""
        self.trend_entry_started = 0.0

class Executor:
    def __init__(self, config: dict, exchange: ExchangeWrapper, strategist: Strategist, notifier: Notifier):
        self.config = config
        self.exchange = exchange
        self.strategist = strategist
        self.notifier = notifier
        self.analyst: Optional[Analyst] = None
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
        self.trend_entry_timeout = execution_cfg.get("trend_entry_timeout_seconds", 900)
        self._order_seq = itertools.count(1)

    def _env_bool(self, name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

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
            if order.get("fee"):
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
        if regime == "high_vol" or rvol < 0.5 or candle_eff < 0.3:
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
                }
            cleaned = json.loads(json.dumps(data, default=lambda x: float(x) if hasattr(x, 'item') else str(x)))
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
                holdings = []
                usdt_free = total_usd
                usdt_used = 0
                try:
                    with self.db.conn.cursor() as cur:
                        cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
                        total_usd += float(cur.fetchone()[0])
                except Exception:
                    pass
                total_usd = round(total_usd, 2)
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
        except Exception as e:
            print(f"_record_balance error: {e}")

    async def _sweep_leftover_coins(self):
        try:
            balance = await self.exchange.fetch_balance()
            for symbol in self.states:
                base = symbol.split("/")[0]
                bal = balance.get(base, {}).get("free", 0)
                if bal <= 0:
                    continue
                try:
                    ticker = await asyncio.wait_for(self.exchange.fetch_ticker(symbol), timeout=5)
                    price = float(ticker["last"])
                    order = await self.exchange.create_market_sell_order(symbol, bal)
                    fee = self._calc_fee(order, bal, price, is_maker=False)
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": symbol,
                        "side": "sell", "price": price, "quantity": bal,
                        "order_id": order.get("id"), "status": "closed",
                        "grid_level": None, "realized_pnl": round(-fee, 2), "fee_cost": fee,
                    })
                    print(f"  Swept {bal:.4f} {base} @ ${price:.2f}")
                except Exception as e:
                    print(f"  Sweep sell failed for {symbol}: {e}")
        except Exception as e:
            print(f"_sweep_leftover_coins error: {e}")

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
                "trend_entry": getattr(st, "trend_entry_price", 0),
                "trend_stop": getattr(st, "trend_stop", 0),
                "trend_target": getattr(st, "trend_target", 0),
            }
        try:
            await self.redis.set("vortex:grid_state", json.dumps(data))
            await self.redis.expire("vortex:grid_state", 3600)
        except Exception:
            pass
        if self.allocator:
            try:
                holders = [sym for sym, st in self.states.items() if st.slot_acquired]
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
            await asyncio.sleep(10)

    async def cancel_all(self, state: GridState):
        state.is_active = False
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
            except Exception:
                pass
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
        trend_cfg = self.config["strategy"].get("trend", {})
        risk_pct = trend_cfg.get("risk_percent", 2.0) / 100
        state.atr = self.strategist.entry_conditions.get(state.symbol, {}).get("atr", 0)
        entry_price = self.strategist.get_trend_price(state.symbol)
        ec = self.strategist.entry_conditions.get(state.symbol, {})
        if ec.get("trend_breakout"):
            try:
                ticker = await asyncio.wait_for(self.exchange.watch_ticker(state.symbol), timeout=5)
                entry_price = float(ticker["ask"])
            except Exception:
                pass
        tp_atr = trend_cfg.get("tp_atr", 1.5)
        trail_atr = trend_cfg.get("trail_atr", 2.0)
        rsi = ec.get("rsi", 50)
        rsi_threshold = self.config["strategy"]["entry"].get("nudge", {}).get("rsi_extreme_threshold", 80)
        if rsi > rsi_threshold:
            tp_atr = self.config["strategy"]["entry"].get("nudge", {}).get("rsi_extreme_tp_atr", 0.5)
        if entry_price <= 0 or state.atr <= 0:
            return
        risk_amount = min(usdt * risk_pct, state.pair_budget * 0.5)
        size = round(risk_amount / (state.atr * trail_atr), 4)
        max_size = (usdt * 0.95) / entry_price
        size = min(size, max_size)
        size = round(size, 6)
        if size * entry_price < 5:
            await self.notifier.send_message(f"⛔ {state.symbol} trend entry too small")
            return
        try:
            client_id = self._client_order_id(state.symbol, "trendbuy")
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
            await self.notifier.send_message(f"📈 {state.symbol} trend entry placed @ ${entry_price} | SL after fill: ${state.trend_stop:.2f} | TP: ${state.trend_target:.2f}")
            asyncio.create_task(self.watch_trend_entry_fill(state))
        except Exception as e:
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
                                await self.notifier.send_message(f"⚡ {state.symbol} breakout entry filled (market) @ ${fill_price:.4f} | SL: ${state.trend_stop:.4f} | TP: ${state.trend_target:.4f}")
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
                        await self.notifier.send_message(f"✅ {state.symbol} trend filled @ ${fill_price:.4f} | SL: ${state.trend_stop:.4f} | TP: ${state.trend_target:.4f}")
                        asyncio.create_task(self.trail_trend_position(state))
                        return
                    if status in {"canceled", "expired", "rejected"}:
                        state.trend_entry_pending = False
                        state.trend_size = 0.0
                        await self.notifier.send_message(f"⚪ {state.symbol} trend entry {status}")
                        return
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"watch_trend_entry_fill ({state.symbol}): {e}")
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
        while state.trend_active:
            try:
                ticker = await self.exchange.watch_ticker(state.symbol)
                price = float(ticker["last"])
                if price > state.trend_high:
                    state.trend_high = price
                    state.trend_stop = max(state.trend_stop, price - (state.atr * 2.0))
                if price >= state.trend_target:
                    await self.exit_trend_position(state, "tp")
                    break
                if price < state.trend_stop:
                    await self.exit_trend_position(state, "sl")
                    break
            except Exception as e:
                print(f"trail_trend ({state.symbol}): {e}")
            await asyncio.sleep(5)

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
                await self.redis.setex("vortex:loss_limit_hit", 86400, "1")
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
                    if await self._check_daily_loss():
                        return
                    if state.cooldown_until > now:
                        await asyncio.sleep(30)
                        continue
                    if (now - state.last_entry_attempt) < 120:
                        await asyncio.sleep(10)
                        continue
                    if self.strategist.should_exit_trend_inversion(state.symbol):
                        state.last_entry_attempt = 0
                        await asyncio.sleep(300)
                        continue
                    regime = self.strategist.get_regime(state.symbol)
                    ec = self.strategist.entry_conditions.get(state.symbol, {})
                    price = 0
                    try:
                        ticker = await self.exchange.watch_ticker(state.symbol)
                        price = float(ticker["last"])
                    except Exception:
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
                    if regime == "trending":
                        if self.strategist.should_enter_trend(state.symbol):
                            if not await self.allocator.acquire():
                                log_dec("BLOCKED", "no_budget_slot")
                                await asyncio.sleep(60)
                                continue
                            state.slot_acquired = True
                            state.last_entry_attempt = now
                            try:
                                log_dec("ENTER_TREND", "trend_pullback_signal")
                                await self._save_snapshot(state, "ENTER_TREND")
                                await self.enter_trend_position(state)
                            except Exception:
                                if state.slot_acquired and self.allocator:
                                    await self.allocator.release()
                                state.slot_acquired = False
                            if not state.trend_active and not state.trend_entry_pending:
                                if state.slot_acquired and self.allocator:
                                    await self.allocator.release()
                                state.slot_acquired = False
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        log_dec("BLOCKED", "regime_trending_no_signal")
                    elif regime == "high_vol":
                        if await self._check_filter_override("HIGH_VOLATILITY"):
                            await self.notifier.send_message(f"⚠️ {state.symbol} high vol — overridden by /filter")
                        else:
                            log_dec("BLOCKED", "regime_high_volatility")
                            await self.notifier.send_message(f"⚠️ {state.symbol} high volatility — skipping entry")
                            await asyncio.sleep(120)
                    if self.analyst:
                        if self.allocator and self.allocator.used >= self.allocator.slots:
                            log_dec("BLOCKED", "no_available_slot")
                            await asyncio.sleep(60)
                            continue
                        verdict = await self.analyst.should_enter(state.symbol)
                        state.last_analyst_verdict = verdict
                        v = verdict.get("verdict", "")
                        if v == "STRONG_UPTREND":
                            log_dec("ANALYST", "strong_uptrend")
                        elif v in ("STRONG_DOWNTREND", "HIGH_VOLATILITY") or not verdict.get("safe", True):
                            if v in ("HIGH_VOLATILITY", "STRONG_DOWNTREND") and await self._check_filter_override(v):
                                await self.notifier.send_message(f"📈 {state.symbol} {v} — overridden by /filter")
                            else:
                                msg = f"⛔ {state.symbol} blocked: {v} — {verdict.get('reason', '')}"
                                await self.notifier.send_message(msg)
                                log_dec("BLOCKED", f"analyst_{v}")
                                await asyncio.sleep(300)
                                continue
                    if self.analyst:
                        confidence_val = verdict.get("confidence", 0)
                        conf_threshold = 50 if regime == "trending" else 70
                        if isinstance(confidence_val, (int, float)) and confidence_val < conf_threshold:
                            log_dec("BLOCKED", f"confidence_too_low_{int(confidence_val)}")
                            await asyncio.sleep(300)
                            continue
                    if self.strategist.should_enter(state.symbol):
                        if not await self.allocator.acquire():
                            log_dec("BLOCKED", "no_budget_slot")
                            await asyncio.sleep(60)
                            continue
                        state.slot_acquired = True
                        state.last_entry_attempt = now
                        try:
                            await self.notifier.send_message(f"🚀 {state.symbol} entry conditions met, regime: {regime}")
                            log_dec("ENTER_GRID", "grid_entry")
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
                            if state.slot_acquired and self.allocator:
                                await self.allocator.release()
                            state.slot_acquired = False
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
            await asyncio.sleep(10)

    async def run(self):
        print(f"Starting executor for {len(self.all_pairs)} configured pairs")
        self._daily_loss_notified = False
        self._kill_in_progress = False
        await self._connect_redis()
        if self.redis:
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
                    await self._reset_simulation()
                    total = float(simulated)
                if self.redis:
                    await self.redis.set("vortex:simulated_balance:last", now_val)
            else:
                prev = await self.redis.get("vortex:simulated_balance:last") if self.redis else None
                reset_on_disable = self._env_bool("SIM_RESET_ON_DISABLE", self.config.get("simulation", {}).get("reset_on_disable", False))
                if prev is not None and reset_on_disable:
                    print(f"  🔄 Simulation removed, resetting state")
                    await self._reset_simulation()
                    if self.redis:
                        await self.redis.delete("vortex:simulated_balance:last")
            alloc_cfg = self.config.get("allocator", {})
            self.allocator = BudgetAllocator(total, alloc_cfg, len(self.all_pairs))
            self.pair_budget = self.allocator.budget_per_slot
            print(f"  Balance: ${total:.2f} | Slots: {self.allocator.slots} | "
                  f"Budget/slot: ${self.pair_budget:.2f} | Reserve: ${self.allocator.reserve:.2f}")
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
        except Exception as e:
            print(f"run init error: {e}")
            return
        await self._record_balance()
        await self._publish_orders()
        async def publish_loop():
            await asyncio.sleep(5)
            while True:
                try:
                    await self._publish_conditions()
                    await self._publish_orders()
                except Exception as e:
                    print(f"publish_loop: {e}")
                await asyncio.sleep(10)
        async def balance_loop():
            while True:
                await asyncio.sleep(3600)
                await self._record_balance()
        asyncio.create_task(balance_loop())
        asyncio.create_task(publish_loop())
        tasks = []
        for s in self.all_pairs:
            tasks.append(self.manage_pair(self.states[s]))
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)
