import psycopg2
from datetime import datetime, timezone

class TimescaleDB:
    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config["timescaledb"]["host"],
            port=self.config["timescaledb"]["port"],
            dbname=self.config["timescaledb"]["dbname"],
            user=self.config["timescaledb"]["user"],
            password=self.config["timescaledb"]["password"]
        )
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            cur.execute(self.config["timescaledb"]["schema"])
        print("TimescaleDB connected and schema initialized")

    def log_trade(self, trade: dict):
        ts = trade.get("timestamp") or datetime.now(timezone.utc)
        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades (timestamp, pair, side, price, quantity, order_id, status, grid_level, realized_pnl, fee_cost)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    ts,
                    trade["pair"],
                    trade["side"],
                    trade["price"],
                    trade["quantity"],
                    trade.get("order_id"),
                    trade["status"],
                    trade.get("grid_level"),
                    trade.get("realized_pnl"),
                    trade.get("fee_cost")
                ))
        except Exception:
            pass

    def log_balance_snapshot(self, usdt_balance: float, total_value: float):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO balance_snapshots (timestamp, usdt_balance, total_value)
                    VALUES (%s, %s, %s)
                """, (datetime.now(timezone.utc), round(usdt_balance, 2), round(total_value, 2)))
        except Exception:
            pass

    def log_decision(self, symbol: str, decision: str, reason: str = "", regime: str = "", adx: float = 0, atr: float = 0, rsi: float = 0, price: float = 0, balance: float = 0):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trade_decisions (timestamp, symbol, decision, reason, regime, adx, atr, rsi, price, balance_usdt)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (datetime.now(timezone.utc), symbol, decision, reason[:200], regime, round(adx, 2), round(atr, 2), round(rsi, 2), round(price, 2), round(balance, 2)))
        except Exception:
            pass

    def mark_cancelled(self, symbol: str):
        try:
            with self.conn.cursor() as cur:
                cur.execute("UPDATE trades SET status = 'cancelled' WHERE pair = %s AND status = 'open'", (symbol,))
        except Exception:
            pass

    def get_avg_entry_price(self, symbol: str) -> float:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(AVG(price), 0) FROM trades
                    WHERE pair = %s AND side = 'buy' AND realized_pnl IS NULL
                """, (symbol,))
                return float(cur.fetchone()[0])
        except Exception:
            return 0.0

    def get_daily_pnl(self) -> float:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(realized_pnl), 0)
                    FROM trades WHERE realized_pnl IS NOT NULL
                    AND timestamp > NOW() - INTERVAL '24 hours'
                """)
                return float(cur.fetchone()[0])
        except Exception:
            return 0.0

    def get_recent_decisions(self, symbol: str, limit: int = 5) -> list:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT d.timestamp, d.decision, d.reason, d.regime, d.adx, d.rsi, d.price,
                           COALESCE(t.pnl, 0) as outcome
                    FROM trade_decisions d
                    LEFT JOIN LATERAL (
                        SELECT SUM(COALESCE(realized_pnl, 0)) as pnl
                        FROM trades
                        WHERE pair = d.symbol
                        AND timestamp > d.timestamp
                        AND timestamp < d.timestamp + INTERVAL '4 hours'
                    ) t ON true
                    WHERE d.symbol = %s
                    ORDER BY d.timestamp DESC
                    LIMIT %s
                """, (symbol, limit))
                return [{"timestamp": str(r[0]), "decision": r[1], "reason": r[2] or "",
                         "regime": r[3] or "", "adx": float(r[4]) if r[4] else 0,
                         "rsi": float(r[5]) if r[5] else 0, "price": float(r[6]) if r[6] else 0,
                         "outcome": float(r[7])} for r in cur.fetchall()]
        except Exception:
            return []

    def get_performance_by_regime(self, symbol: str) -> dict:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT d.regime, COUNT(*) as trades,
                           COALESCE(SUM(t.pnl), 0) as total_pnl
                    FROM trade_decisions d
                    LEFT JOIN LATERAL (
                        SELECT SUM(COALESCE(realized_pnl, 0)) as pnl
                        FROM trades
                        WHERE pair = d.symbol
                        AND timestamp > d.timestamp
                        AND timestamp < d.timestamp + INTERVAL '4 hours'
                    ) t ON true
                    WHERE d.symbol = %s
                    AND d.decision LIKE 'ENTER%%'
                    GROUP BY d.regime
                    ORDER BY total_pnl ASC
                """, (symbol,))
                return {r[0]: {"trades": r[1], "pnl": float(r[2])} for r in cur.fetchall()}
        except Exception:
            return {}

    def close(self):
        if self.conn:
            self.conn.close()
