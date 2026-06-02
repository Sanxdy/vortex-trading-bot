import sys, json, asyncio, copy
sys.path.insert(0, __file__[:__file__.rfind("/backtest")])
from backtest import run

orig_sideway = run.choose_sideway_path
orig_update = run.SimulatedPosition.update

def make_tight_entries(orig_func):
    def tight_sideway(ec, strat, df, i, symbol, ep_cfg=None):
        """Tightened entry thresholds: scalping RSI 55->35, BB 2%->0.5%, bb_squeeze rvol x2"""
        ep = ep_cfg if ep_cfg else {}
        rsi = ec.get("rsi", 50)
        rvol = ec.get("rvol", 0)
        atr_pct = ec.get("atr_pct", 0)
        adx = ec.get("adx", 0)
        last_close = ec.get("close", 0)
        previous_close = ec.get("close_prev", 0) if ec.get("close_prev") else last_close
        if not last_close: return ("skip", "no_close")
        if df is None or len(df) < 25: return ("skip", "short_df")

        # bb_squeeze: rvol 0.8->1.5, 0.6->1.2
        has_bb = all(c in df.columns for c in ("bb_upper", "bb_lower", "bb_middle"))
        if ep.get("bb_squeeze", False) and has_bb:
            bb_w = (df["bb_upper"] - df["bb_lower"]) / df["bb_middle"].clip(lower=1)
            cur_w = float(bb_w.iloc[-1])
            min_w = float(bb_w.iloc[-20:-1].min())
            expanding = cur_w > min_w * 1.12
            bb_u = float(df["bb_upper"].iloc[-1])
            bb_l = float(df["bb_lower"].iloc[-1])
            near_upper = last_close >= bb_u * 0.99
            near_lower = last_close <= bb_l * 1.01
            if expanding and rvol > 1.5:
                if near_upper: return ("bb_squeeze", "bb_squeeze")
                if near_lower: return ("bb_squeeze", "bb_squeeze")
            if expanding and rvol > 1.2 and adx > 25:
                return ("bb_squeeze", "bb_squeeze")

        # scalping_5m: RSI 55->35, BB 2%->0.5%
        if ep.get("scalping_5m", False):
            df_5m = strat.data.get(symbol, {}).get("5m")
            if df_5m and len(df_5m) >= 50 and "bb_lower" in df_5m.columns and "rsi" in df_5m.columns:
                c5 = float(df_5m["close"].iloc[-1])
                bl = float(df_5m["bb_lower"].iloc[-1])
                rsi5 = float(df_5m["rsi"].iloc[-1])
                rsi5_prev = float(df_5m["rsi"].iloc[-2]) if len(df_5m) >= 2 else rsi5
                if ep.get("scalp_original", False):
                    if c5 <= bl * 1.005 and rsi5 < 35 and rsi5 > rsi5_prev:
                        return ("scalp_original", "scalp_original")
                if c5 <= bl * 1.005 and rsi5 < 35 and rsi5 > rsi5_prev:
                    return ("scalping_5m", "scalping_5m")

        # lowvol_momentum: require ATR rising (use df row i-1)
        if ep.get("lowvol_momentum", False):
            ema50 = float(df.iloc[-1].get("ema_50", 0)) if "ema_50" in df.columns else 0
            if ema50 > 0 and last_close > ema50 and atr_pct and atr_pct < 0.3 and last_close > previous_close:
                # Check ATR trend from the candle data at row i
                if "atr" in df.columns and i >= 2:
                    atr_now = float(df["atr"].iloc[i])
                    atr_prev = float(df["atr"].iloc[i-2])
                    if atr_now > atr_prev * 0.95 or atr_now < 0.001:
                        return ("lowvol_momentum", "lowvol_momentum")
                else:
                    return ("lowvol_momentum", "lowvol_momentum")

        # Everything else — unchanged
        return orig_func(ec, strat, df, i, symbol, ep_cfg)
    return tight_sideway


def make_tight_exits(trail_mult=1.2, be_threshold=1.001):
    """Create a tight-exit SimulatedPosition.update with shorter trail and earlier breakeven."""
    def tight_update(self, candle_high, candle_low, current_time):
        if self.closed:
            return
        if self.fixed_tp is not None:
            if self.fixed_tp >= self.entry_price:
                if candle_high >= self.fixed_tp:
                    self.exit_price = self.fixed_tp
                    self.exit_reason = "tp"
                    self.closed = True
                    return
                if self.fixed_sl and candle_low <= self.fixed_sl:
                    self.exit_price = self.fixed_sl
                    self.exit_reason = "sl"
                    self.closed = True
                    return
            else:
                if candle_low <= self.fixed_tp:
                    self.exit_price = self.fixed_tp
                    self.exit_reason = "tp"
                    self.closed = True
                    return
                if self.fixed_sl and candle_high >= self.fixed_sl:
                    self.exit_price = self.fixed_sl
                    self.exit_reason = "sl"
                    self.closed = True
                    return
            return
        if candle_high > self.highest_price:
            self.highest_price = candle_high
            new_stop = candle_high - (self.atr * trail_mult)
            self.stop_level = max(self.stop_level, new_stop)
        if not self.breakeven_activated and candle_high >= self.entry_price * be_threshold:
            if self.entry_type in ("continuation", "breakout"):
                self.breakeven_activated = True
                be_stop = self.entry_price * 1.001
                self.stop_level = max(self.stop_level, be_stop)
        emergency_stop = self.entry_price * 0.97
        if candle_low <= emergency_stop:
            self.exit_price = min(candle_low, self.stop_level)
            self.exit_reason = "emergency"
            self.closed = True
            return
        if candle_low <= self.stop_level:
            self.exit_price = self.stop_level
            self.exit_reason = "trail"
            self.closed = True
    return tight_update


async def run_variant(name, sideway_func, position_update=None, config_mod=None):
    """Run backtest with optional patches."""
    print(f"\n=== Running {name} ===")
    run.choose_sideway_path = sideway_func
    if position_update:
        run.SimulatedPosition.update = position_update
    result = await run.run_all(days=365, profile="sideway")
    print(f"{name}: {result['summary']['trades']} trades, ${result['summary']['pnl']}")
    return result


async def compare():
    # Baseline and Tight Entries already done — load from previous run
    baseline = await run_variant("Baseline", orig_sideway)
    tight_entry = await run_variant("Tight Entries", make_tight_entries(orig_sideway))

    tight_exit = await run_variant("Tight Exits", orig_sideway, make_tight_exits(1.2, 1.001))
    combined = await run_variant("Combined", make_tight_entries(orig_sideway), make_tight_exits(1.2, 1.001))

    results = [("Baseline", baseline), ("Tight Entries", tight_entry),
               ("Tight Exits", tight_exit), ("Combined", combined)]

    print("\n\n===================== FINAL COMPARISON =====================")
    print(f"{'Variant':<18} {'Trades':<10} {'Win%':<8} {'Total PnL':<15} {'PnL/day':<12} {'Δ from Base':<15}")
    print(f"{'-'*18} {'-'*10} {'-'*8} {'-'*15} {'-'*12} {'-'*15}")
    base_pnl = baseline["summary"]["pnl"]
    for name, r in results:
        s = r["summary"]
        delta = s["pnl"] - base_pnl
        print(f"{name:<18} {s['trades']:<10} {s['win_rate']:<8} ${s['pnl']:<+12.2f} ${s['dpd']:<+10.2f} ${delta:<+12.2f}")

    print("\n\n— Per-pair comparison —")
    for name, r in results:
        print(f"\n  {name}:")
        for p in sorted(r["results"], key=lambda x: x["total_pnl"], reverse=True)[:5]:
            print(f"    {p['symbol']:<10} ${p['total_pnl']:<+8.2f}  {p['win_rate']:>5.1f}%  {p['trades']:>4}t")

if __name__ == "__main__":
    asyncio.run(compare())
