import psycopg2

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
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (timestamp, pair, side, price, quantity, order_id, status, grid_level, realized_pnl)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                trade["timestamp"],
                trade["pair"],
                trade["side"],
                trade["price"],
                trade["quantity"],
                trade.get("order_id"),
                trade["status"],
                trade.get("grid_level"),
                trade.get("realized_pnl")
            ))

    def close(self):
        if self.conn:
            self.conn.close()
