import asyncio
import builtins
import logging
import sys
import yaml
import os
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))

from exchange_wrapper import ExchangeWrapper
from strategist import Strategist
from notifier import Notifier
from ingestor import Ingestor
from heartbeat import Heartbeat
from news_filter import NewsFilter
from executor import Executor

def load_config():
    load_dotenv()
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    config["exchange"]["api_key"] = os.getenv("EXCHANGE_API_KEY", config["exchange"]["api_key"])
    config["exchange"]["api_secret"] = os.getenv("EXCHANGE_API_SECRET", config["exchange"]["api_secret"])
    config["exchange"]["testnet"] = os.getenv("EXCHANGE_TESTNET", str(config["exchange"]["testnet"])).lower() == "true"
    config["notifications"]["telegram"]["token"] = os.getenv("TELEGRAM_TOKEN", config["notifications"]["telegram"]["token"])
    config["notifications"]["telegram"]["chat_id"] = os.getenv("TELEGRAM_CHAT_ID", config["notifications"]["telegram"]["chat_id"])
    config["redis"]["host"] = os.getenv("REDIS_HOST", config["redis"]["host"])
    config["redis"]["port"] = int(os.getenv("REDIS_PORT", config["redis"]["port"]))
    config["redis"]["password"] = os.getenv("REDIS_PASSWORD", config["redis"]["password"])
    config["timescaledb"]["host"] = os.getenv("TIMESCALE_DB_HOST", config["timescaledb"]["host"])
    config["timescaledb"]["port"] = int(os.getenv("TIMESCALE_DB_PORT", config["timescaledb"]["port"]))
    config["timescaledb"]["dbname"] = os.getenv("TIMESCALE_DB_NAME", config["timescaledb"]["dbname"])
    config["timescaledb"]["user"] = os.getenv("TIMESCALE_DB_USER", config["timescaledb"]["user"])
    config["timescaledb"]["password"] = os.getenv("TIMESCALE_DB_PASSWORD", config["timescaledb"]["password"])
    config["fallback"]["api_key"] = os.getenv("FALLBACK_API_KEY", config["fallback"]["api_key"])
    config["fallback"]["endpoint"] = os.getenv("FALLBACK_ENDPOINT", config["fallback"]["endpoint"])
    config["fallback"]["model"] = os.getenv("FALLBACK_MODEL", config["fallback"]["model"])
    config["solscan"]["api_key"] = os.getenv("SOLSCAN_API_KEY", config["solscan"]["api_key"])
    config["etherscan"]["api_key"] = os.getenv("ETHERSCAN_API_KEY", config["etherscan"]["api_key"])
    config["bscscan"]["api_key"] = os.getenv("BSCSCAN_API_KEY", config["bscscan"]["api_key"])
    active_profile = os.getenv("ACTIVE_PROFILE", "standard")
    if active_profile in config.get("profiles", {}):
        p = config["profiles"][active_profile]
        if "grid" in p:
            config["grid"].update(p["grid"])
        if "strategy" in p:
            for k, v in p["strategy"].items():
                if k in config["strategy"] and isinstance(v, dict):
                    config["strategy"][k].update(v)
                else:
                    config["strategy"][k] = v
        if "risk" in p:
            config["risk"].update(p["risk"])
    if active_profile != "standard":
        for pair in config["pairs"]:
            if "grid" in pair:
                pair["grid"].pop("width_percent", None)
                pair["grid"].pop("count", None)
                pair["grid"].pop("equity_percent_per_level", None)
    config["active_profile"] = active_profile
    config["timezone"] = int(os.getenv("TIMEZONE", "7"))
    trade_pairs = os.getenv("TRADE_PAIRS", "")
    if trade_pairs:
        wanted = {p.strip().upper() for p in trade_pairs.split(",")}
        configured = {p["name"].split("/")[0] for p in config["pairs"]}
        for pair in config["pairs"]:
            base = pair["name"].split("/")[0]
            pair["enabled"] = base in wanted
        for ticker in wanted:
            if ticker not in configured:
                config["pairs"].append({
                    "name": f"{ticker}/USDT",
                    "enabled": True,
                    "grid": {
                        "width_percent": config["grid"]["default_width_percent"],
                        "count": config["grid"]["default_count"],
                        "equity_percent_per_level": config["grid"]["default_equity_percent_per_level"]
                    }
                })
    return config

def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "vortex.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )
    _print = builtins.print
    def patched_print(*args, **kwargs):
        message = " ".join(str(a) for a in args)
        logging.log(logging.INFO, message)
    builtins.print = patched_print
    logging.info(f"Logging initialized → {log_path}")

async def main():
    setup_logging()
    config = load_config()
    exchange = ExchangeWrapper(config)
    notifier = None
    executor = None
    try:
        await exchange.connect()
        notifier = Notifier(config)
        await notifier.connect()
        ingestor = Ingestor(config, exchange)
        strategist = Strategist(config, exchange)
        executor = Executor(config, exchange, strategist, notifier)
        executor.news_filter = NewsFilter()
        notifier.set_executor(executor)
        heartbeat = Heartbeat(config, exchange, notifier, executor)

        async def safe_task(name: str, factory):
            while True:
                try:
                    await factory()
                    if name == "executor" and executor and executor.redis:
                        try:
                            key = "vortex:loss_limit_hit"
                            if await executor.redis.exists(key):
                                ttl = await executor.redis.ttl(key)
                                pause_secs = ttl if isinstance(ttl, int) and ttl > 0 else 300
                                print(f"[{name}] Daily loss limit reached — pausing {pause_secs}s before retry")
                                await asyncio.sleep(min(pause_secs + 5, 300))
                                continue
                        except Exception:
                            pass
                    print(f"[{name}] exited cleanly — restarting in 5s")
                    await asyncio.sleep(5)
                except Exception as e:
                    print(f"[{name}] crashed: {e} — restarting in 5s")
                    await asyncio.sleep(5)

        await asyncio.gather(
            safe_task("ingestor", ingestor.run),
            safe_task("strategist", strategist.run),
            safe_task("executor", executor.run),
            safe_task("heartbeat", heartbeat.run),
            safe_task("notifier", notifier.start_polling),
            safe_task("resync", exchange.resync_time),
            return_exceptions=True,
        )
    finally:
        if executor:
            try:
                await executor.cancel_open_orders()
                print("Cancelled all open orders on shutdown")
            except Exception:
                pass
        if notifier:
            await notifier.close()
        await exchange.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
