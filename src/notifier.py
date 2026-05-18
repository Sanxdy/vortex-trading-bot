from telegram import Bot, Update, BotCommand, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
from typing import Optional, TYPE_CHECKING
import asyncio
import json
import os
import sys
import psycopg2
from datetime import timezone, timedelta
from redis import asyncio as aioredis
from suggest import get_suggestions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from backtest.run import compare_profiles

if TYPE_CHECKING:
    from executor import Executor

class Notifier:
    def __init__(self, config: dict):
        self.token = config["notifications"]["telegram"]["token"]
        raw = config["notifications"]["telegram"]["chat_id"]
        self.chat_ids = [c.strip() for c in raw.split(",") if c.strip()]
        self.bot: Optional[Bot] = None
        self.app: Optional[Application] = None
        self.executor: Optional['Executor'] = None
        self._last_suggest: list = []
        self._last_backtest_rec: str = ""
        self._last_msg = ""
        self._last_msg_time = 0.0
        self.watchlist_monitor = None

    @staticmethod
    def _to_local(dt, offset_hours):
        return dt.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=offset_hours)))

    def set_executor(self, executor: 'Executor'):
        self.executor = executor

    def set_watchlist_monitor(self, wm):
        self.watchlist_monitor = wm

    async def connect(self):
        self.bot = Bot(self.token)
        me = await self.bot.get_me()
        print(f"Telegram bot @{me.username} connected")

    async def start_polling(self):
        self.app = Application.builder().token(self.token).build()
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("grid", self.cmd_grid))
        self.app.add_handler(CommandHandler("balance", self.cmd_balance))
        self.app.add_handler(CommandHandler("positions", self.cmd_positions))
        self.app.add_handler(CommandHandler("config", self.cmd_config))
        self.app.add_handler(CommandHandler("pnl", self.cmd_pnl))
        self.app.add_handler(CommandHandler("why", self.cmd_why))
        self.app.add_handler(CommandHandler("suggest", self.cmd_suggest))
        self.app.add_handler(CommandHandler("switch", self.cmd_switch))
        self.app.add_handler(CommandHandler("apply", self.cmd_apply))
        self.app.add_handler(CommandHandler("profile", self.cmd_profile))
        self.app.add_handler(CommandHandler("trades", self.cmd_trades))
        self.app.add_handler(CommandHandler("performance", self.cmd_performance))
        self.app.add_handler(CommandHandler("backtest", self.cmd_backtest))
        self.app.add_handler(CommandHandler("filter", self.cmd_filter))
        self.app.add_handler(CommandHandler("debug", self.cmd_debug))
        self.app.add_handler(CommandHandler("report", self.cmd_report))
        self.app.add_handler(CommandHandler("reflect", self.cmd_reflect))
        self.app.add_handler(CommandHandler("revert", self.cmd_revert))
        self.app.add_handler(CommandHandler("kill", self.cmd_kill))
        self.app.add_handler(CommandHandler("sweep", self.cmd_sweep))
        self.app.add_handler(CommandHandler("sim", self.cmd_sim))
        self.app.add_handler(CommandHandler("mode", self.cmd_mode))
        self.app.add_handler(CommandHandler("ai_stats", self.cmd_ai_stats))
        self.app.add_handler(CommandHandler("wl_add", self.cmd_wl_add))
        self.app.add_handler(CommandHandler("wl_remove", self.cmd_wl_remove))
        self.app.add_handler(CommandHandler("wl_list", self.cmd_wl_list))
        await self.bot.set_my_commands([
            BotCommand("start", "Show commands"),
            BotCommand("kill", "Cancel all orders, sell coins, stop bot"),
            BotCommand("sim", "Set simulated balance (e.g. 50) or off to disable"),
            BotCommand("status", "Grid status for all pairs"),
            BotCommand("grid", "Show grid levels (pair optional)"),
            BotCommand("balance", "Account balances"),
            BotCommand("positions", "Open positions"),
            BotCommand("pnl", "Realized profit & loss"),
            BotCommand("config", "Bot configuration"),
            BotCommand("why", "Diagnose why no position is opening"),
            BotCommand("suggest", "Scan & suggest best coins for grid trading"),
            BotCommand("switch", "Switch trading pairs (restarts bot)"),
            BotCommand("apply", "Apply last /suggest recommendations"),
            BotCommand("profile", "Switch trading profile (standard/scalper)"),
            BotCommand("trades", "List recent trades with P&L"),
            BotCommand("performance", "Portfolio growth from start"),
            BotCommand("backtest", "Backtest a pair with DeepSeek analysis"),
            BotCommand("filter", "Manage filter overrides (list/override/remove)"),
            BotCommand("debug", "Show entry snapshot for a pair"),
            BotCommand("report", "AI analysis of recent trade decisions"),
            BotCommand("reflect", "Performance reflection for a pair"),
            BotCommand("revert", "Toggle mode: normal / auto / countertrend"),
            BotCommand("sweep", "Sell leftover coins from exchange wallet"),
            BotCommand("wl_add", "Add pair to watchlist (e.g. /wl_add ADA/USDT)"),
            BotCommand("wl_remove", "Remove pair from watchlist (e.g. /wl_remove ADA/USDT)"),
            BotCommand("wl_list", "List all watched pairs with status"),
            BotCommand("mode", "Set trading mode (technical_only/ai_observe_only/technical_plus_ai)"),
            BotCommand("ai_stats", "Show AI counterfactual stats"),
        ])
        print("Telegram command polling started")
        await self.app.initialize()
        await self.app.start()
        if self.app.updater is not None:
            await self.app.updater.start_polling()
        await asyncio.Event().wait()

    async def stop_polling(self):
        if self.app:
            if self.app.updater is not None:
                await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    async def safe_reply(self, update: Update, text: str, parse_mode: str = "Markdown"):
        if len(text) > 4000:
            text = text[:3997] + "..."
        try:
            await update.message.reply_text(text, parse_mode=parse_mode)
        except Exception:
            await update.message.reply_text(text.replace("*", "").replace("_", "").replace("`", ""))

    async def send_message(self, message: str):
        now = asyncio.get_event_loop().time()
        if message == self._last_msg and (now - self._last_msg_time) < 10:
            return
        self._last_msg = message
        self._last_msg_time = now
        if not self.bot and not self.app:
            await self.connect()
        text = message[:4000] if len(message) > 4000 else message
        for cid in self.chat_ids:
            try:
                if self.app:
                    await self.app.bot.send_message(chat_id=cid, text=text)
                else:
                    await self.bot.send_message(chat_id=cid, text=text)
            except Exception as e:
                print(f"Telegram send error ({cid}): {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            ["/status", "/grid"],
            ["/balance", "/positions"],
            ["/config", "/pnl"],
            ["/why", "/suggest"],
            ["/apply", "/switch"],
            ["/profile", "/performance"],
            ["/backtest", "/trades"],
            ["/debug", "/report"],
            ["/reflect", "/filter"],
            ["/revert", "/sim"],
            ["/sweep", "/kill"],
        ]
        await update.message.reply_text(
            "🤖 *Vortex Grid Bot*\n"
            "Mean reversion grid trading bot\n\n"
            "Commands:\n"
            "/status — Grid & connection status\n"
            "/grid — Current grid levels (all pairs)\n"
            "/grid BTC — Grid levels for a specific pair\n"
            "/balance — Account balance\n"
            "/positions — Open positions\n"
            "/config — Bot configuration\n"
            "/pnl — Realized profit & loss\n"
            "/why — Diagnose per-pair entry blocks\n"
            "/trades — Recent realized P&L trades\n"
            "/performance — Portfolio growth from start\n"
            "/suggest — Scan for best coins to trade\n"
            "/backtest SOL/USDT — Backtest with DeepSeek analysis\n"
            "/debug BTC — Show last entry snapshot for a pair\n"
            "/report — AI analysis of recent decisions\n"
            "/reflect BTC — Performance reflection for a pair\n"
            "/sweep — Sell leftover coins from exchange wallet\n"
            "/revert — Toggle mode: normal / auto / countertrend\n"
            "/kill — Cancel all orders, sell coins, stop bot\n"
            "/sim 50 — Cap sizing as if balance is $50\n"
            "/sim off — Disable simulation, return to real balance\n"
            "/filter — Manage filter overrides (list/override/remove)\n"
            "/apply — Apply last /suggest recommendations\n"
            "/switch BTC,ETH,SOL — Change active pairs\n"
            "/profile — Show/switch trading profile\n"
            "/help — This message\n\n"
            "Keyboard menu refreshed.",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardMarkup(
                keyboard,
                resize_keyboard=True,
                is_persistent=True,
            ),
        )

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.cmd_start(update, context)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        ex = self.executor
        alloc = ex.allocator
        lines = ["*Vortex Status*"]
        if alloc:
            lines.append(f"Slots: {alloc.used}/{alloc.slots} used | Budget/slot: ${alloc.budget_per_slot:.2f}")
        last_decisions = {}
        try:
            conn = psycopg2.connect(
                host=ex.config["timescaledb"]["host"],
                port=ex.config["timescaledb"]["port"],
                dbname=ex.config["timescaledb"]["dbname"],
                user=ex.config["timescaledb"]["user"],
                password=ex.config["timescaledb"]["password"]
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, decision, reason, timestamp
                FROM trade_decisions
                ORDER BY symbol, timestamp DESC
            """)
            tz_hours = ex.config.get("timezone", 7) if ex else 7
            for row in cur.fetchall():
                ts = self._to_local(row[3], tz_hours).strftime("%H:%M") if row[3] else ""
                tag = f"{row[1]}: {row[2]}" if row[2] else row[1]
                last_decisions[row[0]] = f"{ts} {tag}"
            cur.close()
            conn.close()
        except Exception:
            pass
        for symbol, state in ex.states.items():
            active = "🟢" if state.is_active else "🔴"
            levels = len(state.levels)
            slot = " (slot)" if state.slot_acquired else ""
            dec = last_decisions.get(symbol, "")
            dec_tag = f" — {dec}" if dec else ""
            lines.append(f"{active} {symbol} ({levels} levels){slot}{dec_tag}")
        lines.append(f"\nPairs tracked: {len(ex.states)}")
        await self.safe_reply(update, "\n".join(lines))

    async def cmd_grid(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        ex = self.executor
        target = context.args[0].upper() if context.args else None
        lines = []
        for symbol, state in ex.states.items():
            if target and symbol.split("/")[0] != target and symbol != target:
                continue
            if not state.levels:
                lines.append(f"*{symbol}*: No grid deployed")
                continue
            lines.append(f"*{symbol}* ({'Active' if state.is_active else 'Idle'})")
            shown = 0
            for level in state.levels:
                icon = "📈" if level["type"] == "sell" else "📉"
                lines.append(f"{icon} {level['type'].upper()} {level['price']} (lvl {level['level']:+d})")
                shown += 1
                if shown >= 10:
                    remaining = len(state.levels) - shown
                    if remaining > 0:
                        lines.append(f"  ...and {remaining} more levels")
                    break
            lines.append("")
        if not lines:
            lines.append("No grids deployed. Use `/grid BTC` to check a specific pair.")
        msg = "\n".join(lines)
        await self.safe_reply(update, msg)

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        simulated = os.getenv("SIMULATED_BALANCE")
        if simulated:
            await update.message.reply_text(
                f"*Account Balance (SIMULATED)*\n"
                f"USDT: ${float(simulated):.2f}\n\n"
                f"_To see real balance, use /sim off_",
                parse_mode="Markdown"
            )
            return
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        try:
            balance = await self.executor.exchange.fetch_balance()
            usdt = balance["USDT"]["free"]
            sol = balance["SOL"]["free"]
            total_usdt = usdt + (sol * 0)  # Would need current price for accurate total
            await update.message.reply_text(
                f"*Account Balance*\n"
                f"USDT: {usdt:.2f}\n"
                f"SOL: {sol:.4f}",
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"Error fetching balance: {e}")

    async def cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        try:
            balance = await self.executor.exchange.fetch_balance()
            lines = ["*Open Positions*"]
            has_any = False
            for symbol in self.executor.states:
                base = symbol.split("/")[0]
                free = balance.get(base, {}).get("free", 0)
                total = balance.get(base, {}).get("total", 0)
                used = round(total - free, 4) if total else 0
                if total and total > 0:
                    status = "🟢" if self.executor.states[symbol].is_active else "🔴"
                    parts = [f"{status} *{symbol}*"]
                    if free > 0:
                        parts.append(f"  Free: {free:.4f}")
                    if used > 0:
                        parts.append(f"  In orders: {used:.4f}")
                    parts.append(f"  Total: {total:.4f}")
                    lines.append("\n".join(parts))
                    has_any = True
            if not has_any:
                lines.append("No open positions")
            await self.safe_reply(update, "\n".join(lines))
            return
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")

    async def cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        c = self.executor.config
        pairs = [p["name"] for p in c.get("pairs", []) if p.get("enabled", True)]
        pair_display = ", ".join(pairs[:5]) + (" ..." if len(pairs) > 5 else "")
        default_width = c["grid"].get("default_width_percent", c["grid"].get("width_percent", "n/a"))
        default_count = c["grid"].get("default_count", c["grid"].get("count", "n/a"))
        default_equity = c["grid"].get(
            "default_equity_percent_per_level",
            c["grid"].get("equity_percent_per_level", "n/a"),
        )
        profile = c.get("active_profile", "standard")
        msg = (
            f"*Bot Configuration*\n"
            f"Profile: {profile}\n"
            f"Pairs: {pair_display or 'n/a'}\n"
            f"Grid type: {c['grid']['type']}\n"
            f"Default grid width: {default_width}%\n"
            f"Default grid count: {default_count}\n"
            f"Default equity/level: {default_equity}%\n"
            f"Entry timeframe: {c['strategy']['entry']['timeframe']}\n"
            f"Slippage max: {c['risk']['slippage_max_percent']}%\n"
            f"Safety cap: ${c['risk']['safety_cap']}"
        )
        await self.safe_reply(update, msg)

    async def cmd_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            profile = self.executor.config.get("active_profile", "standard") if self.executor else "standard"
            desc = self.executor.config.get("profiles", {}).get(profile, {}).get("description", "") if self.executor else ""
            msg = (
                f"📊 *Current Profile*: {profile}\n"
                f"{desc}\n\n"
                "Available profiles:\n"
            )
            if self.executor:
                for name, p in self.executor.config.get("profiles", {}).items():
                    active = " ✅" if name == profile else ""
                    msg += f"  /profile {name}{active} — {p.get('description', '')}\n"
            msg += "\nUse `/profile <name>` to switch (restarts bot)."
            await self.safe_reply(update, msg)
            return
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        name = context.args[0].lower()
        available = list(self.executor.config.get("profiles", {}).keys())
        if name not in available:
            await update.message.reply_text(f"Unknown profile: {name}. Available: {', '.join(available)}")
            return
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        try:
            with open(env_path, "r") as f:
                lines = f.readlines()
            with open(env_path, "w") as f:
                found = False
                for line in lines:
                    if line.startswith("ACTIVE_PROFILE="):
                        f.write(f"ACTIVE_PROFILE={name}\n")
                        found = True
                    else:
                        f.write(line)
                if not found:
                    f.write(f"ACTIVE_PROFILE={name}\n")
        except Exception as e:
            await update.message.reply_text(f"Failed to update .env: {e}")
            return
        msg = f"✅ Switched to *{name}* profile\n🔄 Restarting..."
        await self.safe_reply(update, msg)
        if self.executor:
            try:
                await self.executor.trigger_kill_switch()
            except Exception:
                pass
        await asyncio.sleep(2)
        os._exit(0)

    async def cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        try:
            conn = psycopg2.connect(
                host=self.executor.config["timescaledb"]["host"],
                port=self.executor.config["timescaledb"]["port"],
                dbname=self.executor.config["timescaledb"]["dbname"],
                user=self.executor.config["timescaledb"]["user"],
                password=self.executor.config["timescaledb"]["password"]
            )
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl IS NOT NULL")
                total_pnl = float(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl IS NOT NULL")
                trade_count = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM trades WHERE realized_pnl > 0")
                wins = float(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl > 0")
                win_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM trades WHERE realized_pnl < 0")
                loss_count = cur.fetchone()[0]
            conn.close()
            emoji = "🟢" if total_pnl >= 0 else "🔴"
            msg = (
                f"{emoji} *Realized P&L*\n"
                f"Total: `${total_pnl:.2f}`\n"
                f"Trades: {trade_count}\n"
                f"Wins: {win_count} | Losses: {loss_count}\n"
                f"Win P&L: +${wins:.2f}"
            )
            await self.safe_reply(update, msg)
        except Exception as e:
            await update.message.reply_text(f"Error fetching P&L: {e}")

    async def cmd_kill(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        await update.message.reply_text("❌ Kill switch activated. Cancelling all orders and selling positions...")
        try:
            await self.executor.trigger_kill_switch()
        except Exception as e:
            await update.message.reply_text(f"Kill switch error: {e}")

    async def cmd_sweep(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        await update.message.reply_text("🧹 Sweeping leftover coins...")
        try:
            await self.executor._sweep_leftover_coins()
            await update.message.reply_text("✅ Sweep complete")
        except Exception as e:
            await update.message.reply_text(f"⚠️ Sweep error: {e}")

    async def cmd_sim(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            current = os.getenv("SIMULATED_BALANCE", "not set")
            await update.message.reply_text(
                f"Current SIMULATED_BALANCE: {current}\n\n"
                "Usage:\n"
                "/sim 50 — Cap sizing as if balance is $50\n"
                "/sim off — Disable simulation, use real balance",
                parse_mode="Markdown"
            )
            return
        val = context.args[0].lower()
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        try:
            with open(env_path, "r") as f:
                lines = f.readlines()
            with open(env_path, "w") as f:
                found = False
                for line in lines:
                    if line.startswith("SIMULATED_BALANCE="):
                        if val == "off":
                            f.write(f"# SIMULATED_BALANCE=\n")
                        else:
                            f.write(f"SIMULATED_BALANCE={val}\n")
                        found = True
                    else:
                        f.write(line)
                if not found and val != "off":
                    f.write(f"SIMULATED_BALANCE={val}\n")
        except Exception as e:
            await update.message.reply_text(f"Failed to update .env: {e}")
            return
        if val == "off":
            msg = "✅ Simulation disabled — bot will use real balance\n🔄 Restarting..."
        else:
            msg = f"✅ Simulated balance set to ${val}\n🔄 Restarting without automatic history reset..."
        await self.safe_reply(update, msg)
        if self.executor:
            try:
                await self.executor.trigger_kill_switch()
            except Exception:
                pass
        await asyncio.sleep(2)
        os._exit(0)

    async def cmd_wl_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.watchlist_monitor:
            await update.message.reply_text("Watchlist monitor not initialized")
            return
        if not context.args:
            await update.message.reply_text("Usage: /wl_add ADA/USDT")
            return
        symbol = " ".join(context.args).upper()
        if not symbol.endswith("/USDT"):
            symbol += "/USDT"
        if symbol in self.watchlist_monitor.watched:
            await update.message.reply_text(f"{symbol} is already in the watchlist.")
            return
        conditions = await self.watchlist_monitor.suggest_conditions(symbol)
        self.watchlist_monitor.watched[symbol] = {"conditions": conditions}
        self.watchlist_monitor._save_config()
        cond_str = ", ".join(c["type"] for c in conditions)
        await update.message.reply_text(
            f"✅ {symbol} added to watchlist\nConditions: {cond_str}"
        )

    async def cmd_wl_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.watchlist_monitor:
            await update.message.reply_text("Watchlist monitor not initialized")
            return
        if not context.args:
            await update.message.reply_text("Usage: /wl_remove ADA/USDT")
            return
        symbol = " ".join(context.args).upper()
        if not symbol.endswith("/USDT"):
            symbol += "/USDT"
        if symbol not in self.watchlist_monitor.watched:
            await update.message.reply_text(f"{symbol} is not in the watchlist.")
            return
        if self.watchlist_monitor._is_pair_active(symbol):
            await self.watchlist_monitor.executor.remove_pair(symbol)
        del self.watchlist_monitor.watched[symbol]
        self.watchlist_monitor._save_config()
        await update.message.reply_text(f"❌ {symbol} removed from watchlist.")

    async def cmd_wl_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.watchlist_monitor:
            await update.message.reply_text("Watchlist monitor not initialized")
            return
        wm = self.watchlist_monitor
        if not wm.watched:
            await update.message.reply_text("Watchlist is empty.")
            return
        lines = ["*Watchlist:*"]
        for sym, cfg in wm.watched.items():
            active = wm._is_pair_active(sym)
            conds = ", ".join(c["type"] for c in cfg["conditions"])
            tag = "🟢 Active" if active else "🔴 Watching"
            lines.append(f"  {tag} {sym} — {conds}")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def cmd_trades(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        try:
            conn = psycopg2.connect(
                host=self.executor.config["timescaledb"]["host"],
                port=self.executor.config["timescaledb"]["port"],
                dbname=self.executor.config["timescaledb"]["dbname"],
                user=self.executor.config["timescaledb"]["user"],
                password=self.executor.config["timescaledb"]["password"]
            )
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT timestamp, pair, side, price, quantity, realized_pnl
                    FROM trades WHERE realized_pnl IS NOT NULL
                    ORDER BY timestamp DESC LIMIT 10
                """)
                rows = cur.fetchall()
            conn.close()
            if not rows:
                await update.message.reply_text("No trades yet.")
                return
            lines = ["📊 *Recent Trades*\n"]
            for r in rows:
                tz_hours = self.executor.config.get("timezone", 7)
                ts = self._to_local(r[0], tz_hours).strftime("%m/%d %H:%M")
                pnl_val = float(r[5]) if r[5] is not None else 0
                side = "🟢" if pnl_val >= 0 else "🔴"
                pnl = f"+${pnl_val:.2f}" if pnl_val >= 0 else f"-${abs(pnl_val):.2f}"
                lines.append(f"{side} {r[1]} {pnl}")
            await self.safe_reply(update, "\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")

    async def cmd_performance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            rc = self.executor.config.get("redis", {}) if self.executor else {}
            if not rc:
                await update.message.reply_text("Redis not configured")
                return
            url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
            r = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
            initial = await r.get("vortex:balance:initial")
            current = await r.get("vortex:balance:current")
            assets_raw = await r.get("vortex:balance:assets")
            start_time = await r.get("vortex:balance:initial_time")
            await r.close()
            if not initial or not current:
                await update.message.reply_text("No balance history yet. Let bot run for a bit.")
                return
            initial_val = float(initial)
            current_val = float(current)
            diff = current_val - initial_val
            pct = (diff / initial_val * 100) if initial_val > 0 else 0
            emoji = "🟢" if diff >= 0 else "🔴"
            lines = [
                f"{emoji} *Portfolio Performance*\n",
                f"Starting: ${initial_val:.2f}",
                f"Current:  ${current_val:.2f}",
                f"Change:   {emoji} ${diff:.2f} ({pct:+.2f}%)",
            ]
            if start_time:
                lines.append(f"\nTracking since: {start_time[:16]}")
            if assets_raw:
                assets = json.loads(assets_raw)
                if assets:
                    lines.append(f"\nAssets:")
                    for a in assets:
                        lines.append(f"  {a['qty']} {a['asset']} @ ${a['price']} = ${a['value']}")
            await self.safe_reply(update, "\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")

    async def cmd_backtest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        symbol = "SOL/USDT"
        days = 14
        if context.args:
            for arg in context.args:
                if arg.startswith("--days="):
                    days = int(arg.split("=")[1])
                elif "/" in arg.upper():
                    symbol = arg.upper()
                else:
                    symbol = f"{arg.upper()}/USDT"
        await update.message.reply_text(f"🔍 Backtesting {symbol} ({days}d) on all profiles... (~1min)")
        try:
            from backtest.run import Backtest
            profiles = ["standard", "scalper", "trend_only", "conservative"]
            results = {}
            for prof in profiles:
                r = await Backtest(symbol, days, prof).run()
                results[prof] = r
            if results.get("standard", {}).get("error"):
                await update.message.reply_text(f"❌ Error: {results['standard']['error']}")
                return
            msg_lines = [f"📊 *Backtest: {symbol} ({days}d)*\n"]
            for prof in profiles:
                r = results.get(prof, {})
                msg_lines.append(
                    f"*{prof.replace('_',' ').title()}:* "
                    f"Grid {r.get('grid_signals',0)} | Trend {r.get('trend_signals',0)} | "
                    f"Density {r.get('signal_density_pct',0)}%"
                )
            msg = "\n".join(msg_lines)
            analyst_key = self.executor.config.get("deepseek", {}).get("api_key", "") if self.executor else ""
            if analyst_key:
                import aiohttp
                prompt_parts = [f"You are a crypto strategy advisor. Compare backtest results for {symbol} over {days}d:\n"]
                for prof in profiles:
                    r = results.get(prof, {})
                    prompt_parts.append(
                        f"{prof.upper()}: Grid={r.get('grid_signals',0)} Trend={r.get('trend_signals',0)} "
                        f"Density={r.get('signal_density_pct',0)}%"
                    )
                prompt_parts.append(
                    "\nPick the BEST profile. Reply with ONLY the profile name: standard, scalper, trend_only, or conservative. "
                    "Then a brief reason (1 sentence)."
                )
                async with aiohttp.ClientSession() as session:
                    resp = await session.post(
                        "https://api.deepseek.com/chat/completions",
                        headers={"Authorization": f"Bearer {analyst_key}", "Content-Type": "application/json"},
                        json={"model": "deepseek-chat", "messages": [{"role": "user", "content": '\n'.join(prompt_parts)}], "temperature": 0.1, "max_tokens": 200},
                        timeout=20
                    )
                    content = (await resp.json())["choices"][0]["message"]["content"]
                    for p in profiles:
                        if p in content.lower():
                            self._last_backtest_rec = p
                            break
                    msg += f"\n\n🤖 *Recommendation:* `{self._last_backtest_rec}`\n{content}"
                    msg += "\n\nReply `/apply` to switch to this profile."
            await update.message.reply_text(msg, parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"❌ Backtest failed: {e}")

    async def cmd_filter(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(
                "Usage:\n/filter list\n/filter override HIGH_VOLATILITY 2h\n/filter remove HIGH_VOLATILITY",
                parse_mode="Markdown"
            )
            return
        action = context.args[0].lower()
        rc = self.executor.config.get("redis", {}) if self.executor else {}
        if not rc:
            await update.message.reply_text("Redis not configured")
            return
        url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
        r = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
        try:
            if action == "list":
                keys = await r.keys("vortex:filter:override:*")
                if not keys:
                    await update.message.reply_text("No active filter overrides.")
                else:
                    msg = "📋 *Active Filter Overrides:*\n"
                    for k in keys:
                        ttl = await r.ttl(k)
                        name = k.split(":")[-1]
                        msg += f"  • {name} ({ttl // 3600}h {ttl % 3600 // 60}m remaining)\n"
                    await update.message.reply_text(msg, parse_mode="Markdown")
            elif action == "override":
                if len(context.args) < 2:
                    await update.message.reply_text("Usage: /filter override FILTER_NAME [duration]")
                    return
                filter_name = context.args[1].upper()
                duration = 7200
                if len(context.args) >= 3:
                    d = context.args[2].lower()
                    if d.endswith("h"): duration = int(d[:-1]) * 3600
                    elif d.endswith("m"): duration = int(d[:-1]) * 60
                    else: duration = int(d)
                await r.setex(f"vortex:filter:override:{filter_name}", duration, "1")
                await update.message.reply_text(f"✅ Override *{filter_name}* for {duration // 3600}h{duration % 3600 // 60}m", parse_mode="Markdown")
            elif action == "remove":
                if len(context.args) < 2:
                    await update.message.reply_text("Usage: /filter remove FILTER_NAME")
                    return
                filter_name = context.args[1].upper()
                await r.delete(f"vortex:filter:override:{filter_name}")
                await update.message.reply_text(f"✅ Removed override *{filter_name}*", parse_mode="Markdown")
            else:
                await update.message.reply_text("Unknown action. Use: list, override, remove")
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
        finally:
            await r.close()

    async def cmd_debug(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        symbol = "SOL/USDT"
        if context.args:
            a = " ".join(context.args).upper()
            symbol = a if "/" in a else f"{a}/USDT"
        rc = self.executor.config.get("redis", {}) if self.executor else {}
        if not rc:
            await update.message.reply_text("Redis not configured")
            return
        url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
        r = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
        try:
            snap_raw = await r.get(f"vortex:snapshot:{symbol.replace('/', '_')}")
            if not snap_raw:
                await update.message.reply_text(f"No snapshot for {symbol}. Bot may not have entered this pair yet.")
                return
            import json
            s = json.loads(snap_raw)
            lines = [
                f"📋 *Debug: {symbol}*",
                f"Decision: {s.get('decision','?')}",
                f"Time: {s.get('ts','?')[:19]}",
                f"",
                f"*Market State:*",
                f"Regime: {s.get('regime','?')} | ADX: {s.get('adx','?')} | ATR: ${s.get('atr','?')}",
                f"RSI: {s.get('rsi','?')} | EMA20: ${s.get('ema_20','?')} | EMA50: ${s.get('ema_50','?')}",
                f"",
                f"*Entry Conditions:*",
                f"Lower BB: {'✅' if s.get('price_at_lower_bb') else '❌'}",
                f"Above 200 EMA: {'✅' if s.get('price_above_200_ema') else '❌'}",
                f"Trend pullback: {'✅' if s.get('trend_pullback') else '❌'}",
                f"",
                f"*Grid Config:*",
                f"Type: {s.get('grid_type','?')} | Width: {s.get('grid_width','?')}% | Levels: {s.get('grid_count','?')}",
            ]
            if s.get("analyst_verdict"):
                lines.append(f"\n*Analyst:* {s['analyst_verdict']}")
            await self.safe_reply(update, "\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
        finally:
            await r.close()

    async def cmd_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("📊 Reading decision log...")
        try:
            conn = psycopg2.connect(
                host=self.executor.config["timescaledb"]["host"],
                port=self.executor.config["timescaledb"]["port"],
                dbname=self.executor.config["timescaledb"]["dbname"],
                user=self.executor.config["timescaledb"]["user"],
                password=self.executor.config["timescaledb"]["password"]
            )
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT decision, reason, regime, adx, rsi, price, timestamp
                    FROM trade_decisions ORDER BY timestamp DESC LIMIT 100
                """)
                rows = cur.fetchall()
            conn.close()
            tz_hours = self.executor.config.get("timezone", 7) if self.executor else 7
            if not rows:
                await update.message.reply_text("No decisions logged yet.")
                return
            lines = []
            for r in rows[:20]:
                ts = self._to_local(r[6], tz_hours).strftime("%m/%d %H:%M")
                lines.append(f"{ts} {r[0]} {r[1] or ''}")
            await self.safe_reply(update, "📊 *Recent Decisions*\n" + "\n".join(lines))
            if len(rows) < 10:
                return
            entered = [r for r in rows if r[0].startswith("ENTER")]
            blocked = [r for r in rows if r[0] == "BLOCKED"]
            summary = (
                f"Decision log has {len(rows)} entries: {len(entered)} entries, {len(blocked)} blocks.\n"
                f"Analyzing patterns..."
            )
            await update.message.reply_text(summary)
            analyst_key = self.executor.config.get("deepseek", {}).get("api_key", "")
            if not analyst_key:
                await update.message.reply_text("DeepSeek key not configured. Skipping AI analysis.")
                return
            import aiohttp
            prompt_parts = [
                "You are a trading bot analyst. Review this decision log and identify patterns.",
                "",
                "RECENT ENTRIES:",
            ]
            for r in rows[:20]:
                d = r[0]; reason = r[1] or ''; regime = r[2] or '?'; adx = r[3] or 0; rsi = r[4] or 0; price = r[5] or 0
                prompt_parts.append(f"  {d} | {reason} | regime={regime} | ADX={adx} | RSI={rsi}")
            prompt_parts.append("")
            prompt_parts.append("What patterns do you see? What's working and what's not?")
            prompt_parts.append("Reply in 3-4 sentences. Be specific about conditions that lead to entries vs blocks.")
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {analyst_key}", "Content-Type": "application/json"},
                    json={"model": "deepseek-chat", "messages": [{"role": "user", "content": '\n'.join(prompt_parts)}], "temperature": 0.1, "max_tokens": 400},
                    timeout=30
                )
                content = (await resp.json())["choices"][0]["message"]["content"]
                await update.message.reply_text(f"🤖 *Decision Analysis:*\n{content}", parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")

    async def cmd_reflect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        target = context.args[0].upper() if context.args else None
        if not target:
            await update.message.reply_text("Usage: /reflect BTC")
            return
        symbol = f"{target}/USDT" if "/" not in target else target
        try:
            perf = self.executor.db.get_performance_by_regime(symbol)
            recent = self.executor.db.get_recent_decisions(symbol, limit=8)
            lines = [f"📊 *Reflection: {symbol}*"]
            if perf:
                lines.append(f"\n*Performance by Regime:*")
                total_pnl = sum(v["pnl"] for v in perf.values())
                for regime, stats in sorted(perf.items(), key=lambda x: x[1]["pnl"]):
                    emoji = "🟢" if stats["pnl"] >= 0 else "🔴"
                    pct = f"({stats['pnl']/max(abs(total_pnl),0.01)*100:.0f}% of total)" if total_pnl != 0 else ""
                    lines.append(f"  {emoji} {regime}: {stats['trades']} trades, ${stats['pnl']:+.2f} {pct}")
            else:
                lines.append("\nNo completed trades found.")
            if recent:
                lines.append(f"\n*Recent decisions:*")
                for d in recent[:5]:
                    outcome = f"PnL ${d['outcome']:+.2f}" if d['outcome'] != 0 else "no fill"
                    lines.append(f"  {d['decision']} | {d['regime']} | ADX {d['adx']} | RSI {d['rsi']} | {outcome}")
                worst_regime = min(perf.keys(), key=lambda r: perf[r]["pnl"]) if perf else None
                if worst_regime and perf[worst_regime]["pnl"] < 0:
                    lines.append(f"\n💡 *Lesson:* Avoid entries in {worst_regime} regime — "
                                 f"lost ${abs(perf[worst_regime]['pnl']):.2f} across {perf[worst_regime]['trades']} trades.")
            await self.safe_reply(update, "\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"Reflect error: {e}")

    async def cmd_why(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        ex = self.executor
        strat = ex.strategist
        grid_enabled = ex.config.get("grid", {}).get("enabled", True)
        lines = ["*Why no position? — Per-pair diagnosis*\n"]
        for symbol in ex.states:
            ec = strat.entry_conditions.get(symbol, {})
            tf = strat.timeframes["entry"]
            df = strat.data.get(symbol, {}).get(tf, None)
            candle_count = len(df) if df is not None else 0
            has_data = "✅" if candle_count >= 200 else "⏳"
            at_lower_bb = ec.get("price_at_lower_bb", False)
            above_200_ema = ec.get("price_above_200_ema", False)
            regime = strat.get_regime(symbol)
            trend_inversion = strat.should_exit_trend_inversion(symbol) if hasattr(strat, 'should_exit_trend_inversion') else False
            if df is not None and len(df) >= 2:
                last = df.iloc[-1]
                close_val = float(last['close'])
                close_fmt = f"${close_val:.2f}" if close_val < 1000 else f"${close_val:.1f}"
                bb_lower_val = float(last['bb_lower']) if 'bb_lower' in df.columns else None
                bb_lower_fmt = f"${bb_lower_val:.2f}" if bb_lower_val and bb_lower_val < 1000 else f"${bb_lower_val:.1f}" if bb_lower_val else "—"
                bb_upper_fmt = f"${float(last['bb_upper']):.2f}" if 'bb_upper' in df.columns and float(last['bb_upper']) < 1000 else f"${float(last['bb_upper']):.1f}" if 'bb_upper' in df.columns else "—"
                ema_val = float(last['ema_200']) if 'ema_200' in df.columns else None
                ema_fmt = f"${ema_val:.2f}" if ema_val and ema_val < 1000 else f"${ema_val:.1f}" if ema_val else "—"
                bb_dist = round((close_val - bb_lower_val) / bb_lower_val * 100, 2) if bb_lower_val and bb_lower_val > 0 else None
                bb_dist_str = f"{bb_dist}%" if bb_dist is not None else "—"
            else:
                close_val = 0; close_fmt = bb_lower_fmt = bb_upper_fmt = ema_fmt = "—"; bb_dist_str = "—"

            state_icon = '🟢' if ex.states[symbol].is_active else '🔴'

            lines.append(f"{state_icon} *{symbol}*  ({regime})")
            lines.append(f"  Price: {close_fmt} | Lower BB: {bb_lower_fmt} ({bb_dist_str} above)")
            lines.append(f"  Upper BB: {bb_upper_fmt} | EMA200: {ema_fmt} | Data: {has_data}")
            lines.append(f"  At lower BB: {'✅' if at_lower_bb else '❌'} | Above EMA200: {'✅' if above_200_ema else '❌'}")

            if regime == "sideways":
                if at_lower_bb:
                    lines.append(f"  ✅ Price at lower BB — grid entry *ready*")
                else:
                    lines.append(f"  ❌ Price {bb_dist_str} above lower BB — waiting for dip")
            elif regime == "trending":
                adx = ec.get("adx", 0)
                rsi = ec.get("rsi", 50)
                trend_signal = "✅ READY" if strat.should_enter_trend(symbol) else "❌"
                lines.append(f"  Trend signal: {trend_signal}")
                if not strat.should_enter_trend(symbol):
                    ct_score = strat.evaluate_countertrend_scalp(symbol, ec.get("analyst_signal", "NEUTRAL"))
                    if adx > 30:
                        rsi_ok = rsi > 60
                        lines.append(f"  ADX {adx:.0f} | RSI {rsi:.0f} {'✅' if rsi_ok else '❌'} (>60)")
                        lines.append(f"  Countertrend score: {ct_score}/100 {'✅' if ct_score >= 65 else '❌'} (needs ≥65)")
                    else:
                        rsi_ok = rsi < 35
                        lines.append(f"  ADX {adx:.0f} | RSI {rsi:.0f} {'✅' if rsi_ok else '❌'} (<35)")
            elif regime == "high_vol":
                lines.append(f"  ⚠️ High volatility — no entries")

            if trend_inversion:
                lines.append(f"  ⛔ *1h trend inversion active* — price below 200 EMA on 1h (blocks all entries)")
            if ex.allocator and ex.allocator.used >= ex.allocator.slots:
                lines.append(f"  💰 Slot full ({ex.allocator.used}/{ex.allocator.slots})")
            av = ex.states[symbol].last_analyst_verdict
            if av:
                lines.append(f"  Analyst: {av.get('verdict', '?')} ({av.get('confidence', 0)}%)")
            lines.append("")
        await self.safe_reply(update, "\n".join(lines))

    async def cmd_suggest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        await update.message.reply_text("🔍 Scanning top-50 pairs for grid-scalping candidates... (15-20s)")
        try:
            exclude = {p["name"] for p in self.executor.config["pairs"] if p.get("enabled", True)}
            suggestions = await get_suggestions(self.executor.exchange, limit=5, exclude_symbols=exclude)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return
        if not suggestions:
            await update.message.reply_text("❌ No safe scalping pairs found. Try again later.")
            return
        config_pairs = [p["name"].split("/")[0] for p in self.executor.config["pairs"] if p.get("enabled", True)]
        chunks = ["⚡ *Grid-Scalper Suggestions*\n"]
        for i, s in enumerate(suggestions, 1):
            base = s["symbol"].split("/")[0]
            tag = " ✅" if base in config_pairs else ""
            score = s["score"]
            score_icon = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"
            entry = (
                f"*#{i} {s['symbol']}* — Score: {score} {score_icon}{tag}\n"
                f"📊 ADX {s['adx']} | RSI {s['rsi']} | Spread {s['spread']}%\n"
                f"📈 ATR {s['atr_pct']}% | RVOL {s['rvol']} | Eff {s['efficiency']} | Vol ${s['quote_volume']:,.0f}\n\n"
            )
            if len(chunks[-1] + entry) > 3800:
                chunks[-1] += "\n_continued..._"
                chunks.append(entry)
            else:
                chunks[-1] += entry
        for msg in chunks:
            await self.safe_reply(update, msg)

    async def cmd_switch(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(
                "Usage: `/switch BTC,ETH,SOL`\n"
                "Pairs are comma-separated. Bot will restart automatically.",
                parse_mode="Markdown"
            )
            return
        raw = " ".join(context.args)
        new_pairs = [p.strip().upper() for p in raw.replace(",", " ").split() if p.strip()]
        if not new_pairs:
            await update.message.reply_text("No valid pairs provided.")
            return
        valid = {t.upper() for t in ["BTC","ETH","BNB","XRP","ADA","SOL","DOGE","AVAX","DOT","LINK","MATIC","UNI","SHIB","LTC","ATOM","XLM","TRX","NEAR","APT","ARB","OP","FIL","ALGO","AAVE","ICP","EGLD","FTM","SAND","MANA","AXS","CHZ","CRV","GRT","ENJ","ZIL","IOTA","COMP","YFI","SUSHI","SNX","BAT","ZEC","DASH","EOS","VET","THETA"]}
        invalid = [p for p in new_pairs if p not in valid]
        if invalid:
            await update.message.reply_text(f"Invalid pairs: {', '.join(invalid)}")
            return
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        try:
            with open(env_path, "r") as f:
                lines = f.readlines()
            with open(env_path, "w") as f:
                found = False
                for line in lines:
                    if line.startswith("TRADE_PAIRS="):
                        f.write(f"TRADE_PAIRS={','.join(new_pairs)}\n")
                        found = True
                    else:
                        f.write(line)
                if not found:
                    f.write(f"TRADE_PAIRS={','.join(new_pairs)}\n")
        except Exception as e:
            await update.message.reply_text(f"Failed to update .env: {e}")
            return
        msg = f"✅ Switched to: {', '.join(new_pairs)}\n🔄 Restarting..."
        await self.safe_reply(update, msg)
        if self.executor:
            try:
                await self.executor.trigger_kill_switch()
            except Exception as e:
                print(f"Kill switch error (ignored): {e}")
        await asyncio.sleep(2)
        os._exit(0)

    async def cmd_revert(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Toggle panic revert — disable all countertrend entries."""
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
        try:
            with open(config_path, "r") as f:
                content = f.read()
            if "panic_revert_to_safe_mode: true" in content:
                content = content.replace("panic_revert_to_safe_mode: true", "panic_revert_to_safe_mode: false")
                msg = "🟢 Countertrend entries *re-enabled* — adaptive mode active"
            else:
                content = content.replace("panic_revert_to_safe_mode: false", "panic_revert_to_safe_mode: true")
                msg = "🔴 *Panic revert activated* — all countertrend entries blocked (safe mode)"
            with open(config_path, "w") as f:
                f.write(content)
            msg += "\n🔄 Restarting..."
            await self.safe_reply(update, msg)
            if self.executor:
                try:
                    await self.executor.trigger_kill_switch()
                except Exception as e:
                    print(f"Kill switch error (ignored): {e}")
            await asyncio.sleep(2)
            os._exit(0)
        except Exception as e:
            await update.message.reply_text(f"Failed to update config: {e}")

    async def cmd_apply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.executor:
            await update.message.reply_text("Executor not initialized")
            return
        if self._last_backtest_rec and not self._last_suggest:
            name = self._last_backtest_rec
            env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
            try:
                with open(env_path, "r") as f:
                    lines = f.readlines()
                with open(env_path, "w") as f:
                    found = False
                    for line in lines:
                        if line.startswith("ACTIVE_PROFILE="):
                            f.write(f"ACTIVE_PROFILE={name}\n")
                            found = True
                        else:
                            f.write(line)
                    if not found:
                        f.write(f"ACTIVE_PROFILE={name}\n")
            except Exception as e:
                await update.message.reply_text(f"Failed to update .env: {e}")
                return
            msg = f"✅ Switched to *{name}* profile\n🔄 Restarting..."
            await self.safe_reply(update, msg)
            if self.executor:
                try:
                    await self.executor.trigger_kill_switch()
                except Exception:
                    pass
            await asyncio.sleep(2)
            os._exit(0)
            return
        if not self._last_suggest:
            await update.message.reply_text("No suggestions or backtest results to apply. Run `/suggest` or `/backtest` first.")
            return
        new_tickers = [s["ticker"] for s in self._last_suggest if s.get("ticker") and s["ticker"] != "N/A"]
        if not new_tickers:
            await update.message.reply_text("Invalid suggestions stored.")
            return
        new_tickers = list(dict.fromkeys(new_tickers))
        old_tickers = [p["name"].split("/")[0] for p in self.executor.config["pairs"] if p.get("enabled", True)]
        removed = [t for t in old_tickers if t not in new_tickers]
        kept = [t for t in old_tickers if t in new_tickers]
        added = [t for t in new_tickers if t not in old_tickers]
        reports = []
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        try:
            with open(env_path, "r") as f:
                lines = f.readlines()
            with open(env_path, "w") as f:
                found = False
                for line in lines:
                    if line.startswith("TRADE_PAIRS="):
                        f.write(f"TRADE_PAIRS={','.join(new_tickers)}\n")
                        found = True
                    else:
                        f.write(line)
                if not found:
                    f.write(f"TRADE_PAIRS={','.join(new_tickers)}\n")
        except Exception as e:
            await update.message.reply_text(f"❌ .env write failed: {e}")
            return
        active_profile = "standard"
        try:
            with open(env_path) as f:
                for line in f:
                    if line.startswith("ACTIVE_PROFILE="):
                        active_profile = line.split("=", 1)[1].strip()
                        break
        except Exception:
            pass
        reports.append(f"✅ Config saved: {', '.join(new_tickers)}")
        reports.append(f"📋 Profile: {active_profile}")
        for ticker in removed:
            symbol = f"{ticker}/USDT"
            state = self.executor.states.get(symbol)
            if state:
                try:
                    await self.executor.cancel_all(state)
                    reports.append(f"🔴 {ticker} sold (removed)")
                except Exception as e:
                    reports.append(f"⚠️ {ticker} cancel error: {e}")
        for ticker in kept:
            symbol = f"{ticker}/USDT"
            try:
                if getattr(self.executor, "manage_only_bot_orders", True):
                    await self.executor.exchange.cancel_bot_orders(symbol, self.executor.client_id_prefix)
                else:
                    await self.executor.exchange.cancel_all_orders(symbol)
                reports.append(f"📋 {ticker} orders cancelled (kept)")
            except Exception as e:
                reports.append(f"⚠️ {ticker} cancel error: {e}")
        if added:
            reports.append(f"🆕 {', '.join(added)} will start after restart")
        reports.append("\n🔄 Restarting...")
        await self.safe_reply(update, "\n".join(reports))
        await asyncio.sleep(2)
        os._exit(0)

    async def cmd_mode(self, update, context):
        if not context.args:
            current = self.executor.trading_mode.value if self.executor else "?"
            await update.message.reply_text(
                f"Current: *{current}*\n\n"
                f"Usage: `/mode <mode>`\n\n"
                f"• `technical_only` — AI completely off\n"
                f"• `ai_observe_only` — AI runs, logs, no effect\n"
                f"• `technical_plus_ai` — Full AI integration",
                parse_mode="Markdown"
            )
            return
        mode = context.args[0].lower()
        valid = ["technical_only", "ai_observe_only", "technical_plus_ai"]
        if mode not in valid:
            await update.message.reply_text(f"Invalid mode. Choose: {', '.join(valid)}")
            return
        rc = self.executor.config.get("redis", {}) if self.executor else {}
        if not rc:
            await update.message.reply_text("Redis not configured")
            return
        url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
        r = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
        try:
            await r.setex("vortex:trading_mode", 86400, mode)
            await update.message.reply_text(f"✅ Mode → *{mode}*", parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
        finally:
            await r.close()

    async def cmd_ai_stats(self, update, context):
        rc = self.executor.config.get("redis", {}) if self.executor else {}
        if not rc:
            await update.message.reply_text("Redis not configured")
            return
        url = f"redis://:{rc['password']}@{rc['host']}:{rc['port']}" if rc['password'] else f"redis://{rc['host']}:{rc['port']}"
        r = await aioredis.from_url(url, db=rc.get("db", 0), decode_responses=True)
        try:
            stats_raw = await r.get("vortex:conditions")
            if not stats_raw:
                await update.message.reply_text("No stats available yet. Wait for next cycle.")
                return
            import json
            data = json.loads(stats_raw)
            stats = data.get("_stats", {})
            mode = data.get("_meta", {}).get("trading_mode", "?")
            msg = (
                f"🤖 AI Counterfactual Stats\n"
                f"Mode: *{mode}*\n\n"
                f"Cycles: {stats.get('cycles', 0)}\n"
                f"Signals: {stats.get('signals', 0)}\n"
                f"Rejected: {stats.get('rejected', 0)}\n"
                f"Executed: {stats.get('executed', 0)}\n\n"
                f"AI would have blocked: {stats.get('ai_would_have_blocked', 0)}\n"
                f"AI would have resized: {stats.get('ai_would_have_resized', 0)}"
            )
            await update.message.reply_text(msg, parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
        finally:
            await r.close()

    async def close(self):
        await self.stop_polling()
        if self.bot:
            await self.bot.close()
