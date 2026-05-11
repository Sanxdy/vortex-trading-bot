import asyncio
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
    def __init__(self, total_balance: float, min_per_pair: float):
        self.slots = max(1, int(total_balance / min_per_pair))
        self.budget_per_slot = round(total_balance / self.slots, 2) if self.slots > 0 else 0
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
        self.pair_budget = 0.0
        self.min_notional = 10.0
        self.slot_acquired = False

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
            }
            key = f"vortex:snapshot:{state.symbol.replace('/', '_')}"
            await self.redis.setex(key, 604800, json.dumps(snap))
        except Exception:
            pass

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
                    "rsi": ec.get("rsi", 0),
                    "atr": ec.get("atr", 0),
                    "ema_20": ec.get("ema_20", 0),
                    "ema_50": ec.get("ema_50", 0),
                    "trend_uptrend": ec.get("trend_uptrend", False),
                    "trend_pullback": ec.get("trend_pullback", False),
                    "bb_lower": ec.get("price_at_lower_bb", False),
                    "above_ema200": ec.get("price_above_200_ema", False),
                    "trend_active": st.trend_active,
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
                try:
                    with self.db.conn.cursor() as cur:
                        cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
                        pnl = float(cur.fetchone()[0])
                    await self.redis.set("vortex:balance:initial_pnl", str(pnl))
                except Exception:
                    pass
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
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": symbol,
                        "side": "sell", "price": price, "quantity": bal,
                        "order_id": order.get("id"), "status": "closed",
                        "grid_level": None, "realized_pnl": 0,
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
            data[symbol] = {"is_active": st.is_active, "orders": orders}
        try:
            await self.redis.set("vortex:grid_state", json.dumps(data))
            await self.redis.expire("vortex:grid_state", 3600)
        except Exception:
            pass
        if self.allocator:
            try:
                await self.redis.setex("vortex:allocator", 3600, json.dumps({
                    "slots": self.allocator.slots,
                    "used": self.allocator.used,
                    "budget_per_slot": self.allocator.budget_per_slot,
                }))
            except Exception:
                pass

    async def _check_daily_loss(self) -> bool:
        daily_pnl = self.db.get_daily_pnl()
        max_loss_pct = self.config["risk"].get("max_daily_loss_percent", 5)
        initial = await self._get_initial_balance()
        if initial > 0 and daily_pnl < 0 and abs(daily_pnl) >= initial * (max_loss_pct / 100):
            await self.notifier.send_message(f"🚨 Daily loss limit ({max_loss_pct}%) hit: ${daily_pnl:.2f}")
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
        self.analyst = analyst

    async def calculate_grid_levels(self, state: GridState, center_price: float) -> List[Dict]:
        buy_count = state.count // 2
        sell_count = state.count // 2
        levels = []
        if state.grid_type == "arithmetic":
            step = center_price * state.width
            for i in range(1, buy_count + 1):
                levels.append({"type": "buy", "price": round(center_price - step * i, 4), "level": -i, "placed": False})
            for i in range(1, sell_count + 1):
                levels.append({"type": "sell", "price": round(center_price + step * i, 4), "level": i, "placed": False})
        else:
            for i in range(1, buy_count + 1):
                levels.append({"type": "buy", "price": round(center_price * ((1 - state.width) ** i), 4), "level": -i, "placed": False})
            for i in range(1, sell_count + 1):
                levels.append({"type": "sell", "price": round(center_price * ((1 + state.width) ** i), 4), "level": i, "placed": False})
        return sorted(levels, key=lambda x: x["price"])

    async def place_grid_orders(self, state: GridState, levels: List[Dict]):
        balance = await self.exchange.fetch_balance()
        base = state.symbol.split("/")[0]
        usdt_balance = float(balance["USDT"]["free"])
        base_balance = balance.get(base, {}).get("free", 0)
        buys = sorted([l for l in levels if l["type"] == "buy"], key=lambda x: x["price"], reverse=True)
        min_per_level = 10
        max_levels = int(usdt_balance / min_per_level) if usdt_balance > 0 else 0
        affordable = min(max_levels, len(buys))
        if affordable < 1:
            print(f"  {state.symbol}: ${usdt_balance:.2f} < ${min_per_level}, skipping grid")
            return
        buy_levels = buys[:affordable]
        equity_per_level = usdt_balance / affordable
        placed = []
        skipped = []
        for level in buy_levels:
            side = "buy"
            price = level["price"]
            amount = round(equity_per_level / price, 6)
            ticker = await self.exchange.watch_ticker(state.symbol)
            spread = (ticker["ask"] - ticker["bid"]) / ticker["last"]
            if spread > self.config["risk"]["slippage_max_percent"] / 100:
                skipped.append(f"{side.upper()} @ {price} (spread {spread:.4f})")
                continue
            order = await self.exchange.create_limit_order(state.symbol, side, amount, price)
            level["placed"] = True
            self.db.log_trade({
                "timestamp": order["timestamp"], "pair": state.symbol,
                "side": side, "price": order["price"], "quantity": order["amount"],
                "order_id": order.get("id"), "status": order["status"],
                "grid_level": level["level"], "realized_pnl": None
            })
            placed.append(f"{side.upper()} @ {price} x{amount}")
        summary = f"📋 {state.symbol} grid: {len(placed)}/{len(levels)} levels ({affordable} buys)"
        if placed:
            summary += f"\n" + "\n".join(placed[:5])
            if len(placed) > 5:
                summary += f"\n...and {len(placed)-5} more"
        if skipped:
            summary += f"\n⚠️ Skipped: {len(skipped)} — " + "; ".join(skipped[:3])
            if len(skipped) > 3:
                summary += f" (+{len(skipped)-3})"
        print(summary)
        await self.notifier.send_message(summary)
        await self._publish_orders()

    async def recenter_grid(self, state: GridState):
        now = asyncio.get_event_loop().time()
        min_cooldown = 1800
        if (now - state.last_rebalance) < min_cooldown:
            return
        try:
            await self.exchange.cancel_all_orders(state.symbol)
        except Exception:
            pass
        balance = await self.exchange.fetch_balance()
        base = state.symbol.split("/")[0]
        bal = balance.get(base, {}).get("free", 0)
        if bal > 0:
            try:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                market_price = float(ticker["last"])
                avg_entry = (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else 0
                await self.exchange.create_market_sell_order(state.symbol, bal)
                if avg_entry > 0:
                    pnl = round((market_price - avg_entry) * bal, 2)
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                        "side": "sell", "price": market_price, "quantity": bal,
                        "order_id": None, "status": "closed",
                        "grid_level": None, "realized_pnl": pnl,
                    })
                state.filled_cost = 0.0
                state.filled_qty = 0.0
            except Exception:
                pass
        ticker = await self.exchange.watch_ticker(state.symbol)
        state.levels = await self.calculate_grid_levels(state, ticker["last"])
        await self.place_grid_orders(state, state.levels)
        state.last_rebalance = now
        state.fill_counts = {"buy": 0, "sell": 0}
        await self.notifier.send_message(f"🔄 {state.symbol} grid re-centered at ${ticker['last']}")

    async def watch_order_fills(self, state: GridState):
        while state.is_active:
            try:
                orders = await self.exchange.watch_orders(state.symbol)
                for order in orders:
                    if order["status"] == "closed":
                        side = order["side"]
                        fill_price = float(order["average"])
                        amount = float(order["filled"])
                        state.fill_counts[side] += 1
                        if side == "buy":
                            state.filled_cost += fill_price * amount
                            state.filled_qty += amount
                            sell_price = round(fill_price * (1 + state.width), 4)
                            sell_order = await self.exchange.create_limit_order(state.symbol, "sell", amount, sell_price)
                            profit = round((sell_price - fill_price) * amount, 2)
                            self.db.log_trade({
                                "timestamp": sell_order["timestamp"], "pair": state.symbol,
                                "side": "sell", "price": sell_order["price"],
                                "quantity": sell_order["amount"], "order_id": sell_order.get("id"),
                                "status": sell_order["status"], "grid_level": None, "realized_pnl": None
                            })
                            await self.notifier.send_message(f"✅ {state.symbol} Buy→Sell | Buy: {fill_price} → Sell: {sell_price} | +${profit}")
                        elif side == "sell":
                            buy_price = round(fill_price * (1 - state.width), 4)
                            buy_order = await self.exchange.create_limit_order(state.symbol, "buy", amount, buy_price)
                            profit = round((fill_price - buy_price) * amount, 2)
                            cost_out = (fill_price / (1 + state.width)) * amount
                            state.filled_cost = max(0, state.filled_cost - cost_out)
                            state.filled_qty = max(0, state.filled_qty - amount)
                            self.db.log_trade({
                                "timestamp": buy_order["timestamp"], "pair": state.symbol,
                                "side": "buy", "price": buy_order["price"],
                                "quantity": buy_order["amount"], "order_id": buy_order.get("id"),
                                "status": buy_order["status"], "grid_level": None, "realized_pnl": profit
                            })
                            await self.notifier.send_message(f"✅ {state.symbol} Sell→Buy | Sell: {fill_price} → Buy: {buy_price} | +${profit}")
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
                            if buy_pct >= 0.60 or sell_pct >= 0.60:
                                direction = "up" if sell_pct >= 0.75 else "down"
                                await self.recenter_grid(state)
                                await self.notifier.send_message(f"📐 {state.symbol} re-centered due to {direction} skew ({state.fill_counts})")
            except Exception as e:
                print(f"Order watch ({state.symbol}): {e}")
                await asyncio.sleep(1)

    async def check_exit_conditions(self, state: GridState):
        await asyncio.sleep(300)
        while state.is_active:
            if self.strategist.should_exit_take_profit(state.symbol):
                await self.notifier.send_message(f"🎉 {state.symbol} TP triggered (upper BB)")
                await self.cancel_all(state)
                break
            if state.levels:
                lowest = min(l["price"] for l in state.levels if l["type"] == "buy")
                ticker = await self.exchange.watch_ticker(state.symbol)
                state_atr = self.strategist.entry_conditions.get(state.symbol, {}).get("atr", 0)
                atr_mult = self.config["strategy"]["exit"]["stop_loss"].get("atr_multiplier", 1.5)
                if state_atr > 0:
                    stop = ticker["last"] - (state_atr * atr_mult)
                else:
                    stop = lowest * (1 - self.config["strategy"]["exit"]["stop_loss"]["percent_below_lowest_grid"] / 100)
                if ticker["last"] < stop:
                    await self.notifier.send_message(f"🛑 {state.symbol} SL triggered: {ticker['last']} < {stop}")
                    await self.cancel_all(state)
                    await asyncio.sleep(4 * 3600)
                    break
            if self.strategist.should_exit_trend_inversion(state.symbol):
                await self.notifier.send_message(f"📉 {state.symbol} Trend inversion (1h below 200 EMA)")
                await self.cancel_all(state)
                break
            await asyncio.sleep(10)

    async def cancel_all(self, state: GridState):
        state.is_active = False
        if state.slot_acquired and self.allocator:
            await self.allocator.release()
            state.slot_acquired = False
        try:
            await self.exchange.cancel_all_orders(state.symbol)
        except Exception:
            pass
        balance = await self.exchange.fetch_balance()
        base = state.symbol.split("/")[0]
        bal = balance.get(base, {}).get("free", 0)
        if bal > 0:
            try:
                ticker = await asyncio.wait_for(self.exchange.fetch_ticker(state.symbol), timeout=5)
                market_price = float(ticker["last"])
                avg_entry = (state.filled_cost / state.filled_qty) if state.filled_qty > 0 else self.db.get_avg_entry_price(state.symbol)
                order = await self.exchange.create_market_sell_order(state.symbol, bal)
                pnl = round((market_price - avg_entry) * bal, 2) if avg_entry > 0 else 0
                self.db.log_trade({
                    "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                    "side": "sell", "price": market_price, "quantity": bal,
                    "order_id": order.get("id"), "status": "closed",
                    "grid_level": None, "realized_pnl": pnl,
                })
            except Exception:
                pass
        self.db.mark_cancelled(state.symbol)
        state.trend_active = False
        await self.notifier.send_message(f"🔴 {state.symbol} Grid cancelled")
        await self._publish_orders()

    async def cancel_open_orders(self):
        for symbol in self.states:
            try:
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
        tp_atr = trend_cfg.get("tp_atr", 1.5)
        trail_atr = trend_cfg.get("trail_atr", 2.0)
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
            order = await self.exchange.create_limit_order(state.symbol, "buy", size, entry_price)
            state.trend_active = True
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
            await self.notifier.send_message(f"📈 {state.symbol} trend buy @ ${entry_price} | SL: ${state.trend_stop:.2f} | TP: ${state.trend_target:.2f}")
            asyncio.create_task(self.trail_trend_position(state))
        except Exception as e:
            await self.notifier.send_message(f"⛔ {state.symbol} trend entry failed: {e}")

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
                    await self.cancel_all(state)
                    pnl = round((state.trend_target - state.trend_entry_price) * state.trend_size, 2)
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                        "side": "sell", "price": state.trend_target, "quantity": state.trend_size,
                        "order_id": None, "status": "closed",
                        "grid_level": None, "realized_pnl": pnl,
                    })
                    await self.notifier.send_message(f"✅ {state.symbol} trend TP hit: +${pnl}")
                    state.trend_active = False
                    break
                if price < state.trend_stop:
                    await self.cancel_all(state)
                    pnl = round((price - state.trend_entry_price) * state.trend_size, 2)
                    self.db.log_trade({
                        "timestamp": datetime.now(timezone.utc), "pair": state.symbol,
                        "side": "sell", "price": price, "quantity": state.trend_size,
                        "order_id": None, "status": "closed",
                        "grid_level": None, "realized_pnl": pnl,
                    })
                    await self.notifier.send_message(f"🛑 {state.symbol} trend SL hit: ${pnl:.2f}")
                    state.trend_active = False
                    break
            except Exception as e:
                print(f"trail_trend ({state.symbol}): {e}")
            await asyncio.sleep(5)

    async def trigger_kill_switch(self):
        for state in self.states.values():
            await self.cancel_all(state)
        await self.notifier.send_message("❌ Kill switch activated")
        await self.exchange.close()
        self.db.close()
        os._exit(0)

    async def manage_pair(self, state: GridState):
        while True:
            try:
                if state.trend_active:
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
                            log_dec("ENTER_TREND", "trend_pullback_signal")
                            await self._save_snapshot(state, "ENTER_TREND")
                            await self.enter_trend_position(state)
                            if not state.trend_active:
                                if state.slot_acquired and self.allocator:
                                    await self.allocator.release()
                                state.slot_acquired = False
                                state.cooldown_until = now + 120
                            await asyncio.sleep(300)
                            continue
                        log_dec("BLOCKED", "regime_trending_no_signal")
                        await asyncio.sleep(30)
                        continue
                    elif regime == "high_vol":
                        if await self._check_filter_override("HIGH_VOLATILITY"):
                            await self.notifier.send_message(f"⚠️ {state.symbol} high vol — overridden by /filter")
                        else:
                            log_dec("BLOCKED", "regime_high_volatility")
                            await self.notifier.send_message(f"⚠️ {state.symbol} high volatility — skipping entry")
                            await asyncio.sleep(120)
                            continue
                    if self.analyst:
                        verdict = await self.analyst.should_enter(state.symbol)
                        state.last_analyst_verdict = verdict
                        v = verdict.get("verdict", "")
                        if v == "STRONG_UPTREND":
                            await self.notifier.send_message(f"📈 {state.symbol} uptrend — entering with scalper re-center")
                        elif v in ("STRONG_DOWNTREND", "HIGH_VOLATILITY") or not verdict.get("safe", True):
                            if v in ("HIGH_VOLATILITY", "STRONG_DOWNTREND") and await self._check_filter_override(v):
                                await self.notifier.send_message(f"📈 {state.symbol} {v} — overridden by /filter")
                            else:
                                msg = f"⛔ {state.symbol} blocked: {v} — {verdict.get('reason', '')}"
                                await self.notifier.send_message(msg)
                                log_dec("BLOCKED", f"analyst_{v}")
                                await asyncio.sleep(300)
                                continue
                    if self.strategist.should_enter(state.symbol):
                        state.last_entry_attempt = now
                        await self.notifier.send_message(f"🚀 {state.symbol} entry conditions met, regime: {regime}")
                        log_dec("ENTER_GRID", "grid_entry")
                        await self._save_snapshot(state, "ENTER_GRID")
                        ticker = await self.exchange.watch_ticker(state.symbol)
                        state.levels = await self.calculate_grid_levels(state, ticker["last"])
                        state.filled_cost = 0.0
                        state.filled_qty = 0.0
                        await self.place_grid_orders(state, state.levels)
                        state.is_active = True
                        state.last_rebalance = asyncio.get_event_loop().time()
                        asyncio.create_task(self.watch_order_fills(state))
                        asyncio.create_task(self.check_exit_conditions(state))
                else:
                    now = asyncio.get_event_loop().time()
                    if (now - state.last_rebalance) > (self.config["strategy"]["rebalance_interval_hours"] * 3600):
                        try:
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
        await self._connect_redis()
        if self.redis:
            try:
                await self.redis.delete("vortex:allocator", "vortex:grid_state", "vortex:balance:initial", "vortex:balance:initial_time", "vortex:balance:initial_pnl")
            except Exception:
                pass
        try:
            balance = await self.exchange.fetch_balance()
            total = float(balance["USDT"]["free"]) + float(balance["USDT"].get("used", 0))
            simulated = os.getenv("SIMULATED_BALANCE")
            if simulated:
                total = float(simulated)
                print(f"  ⚠️ Simulated balance: ${total:.2f}")
                prev = await self.redis.get("vortex:simulated_balance:last") if self.redis else None
                now_val = str(total)
                if prev is None:
                    print(f"  🆕 First simulation run, resetting state")
                    await self._reset_simulation()
                elif prev != now_val:
                    print(f"  🔄 Sim balance changed ({prev} \u2192 {now_val}), resetting state")
                    await self._reset_simulation()
                if self.redis:
                    await self.redis.set("vortex:simulated_balance:last", now_val)
            else:
                prev = await self.redis.get("vortex:simulated_balance:last") if self.redis else None
                if prev is not None:
                    print(f"  🔄 Simulation removed, resetting state")
                    await self._reset_simulation()
                    if self.redis:
                        await self.redis.delete("vortex:simulated_balance:last")
            min_per_pair = self.config["risk"].get("min_balance_per_pair", 50)
            slots = max(1, int(total / min_per_pair))
            self.allocator = BudgetAllocator(total, min_per_pair)
            self.pair_budget = self.allocator.budget_per_slot
            print(f"  Balance: ${total:.2f} | Trend slots: {slots} | Budget/trend: ${self.pair_budget:.2f}")
            for symbol in self.all_pairs:
                st = GridState(symbol, self.config)
                st.pair_budget = self.pair_budget
                try:
                    st.min_notional = self.exchange.get_min_notional(symbol)
                except Exception:
                    st.min_notional = 10.0
                self.states[symbol] = st
            for state in self.states.values():
                await self.exchange.cancel_all_orders(state.symbol)
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
