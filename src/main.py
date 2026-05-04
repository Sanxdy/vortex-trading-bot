import asyncio
import yaml
import os
from dotenv import load_dotenv
from exchange_wrapper import ExchangeWrapper
from ingestor import Ingestor
from strategist import Strategist
from executor import Executor
from notifier import Notifier
from heartbeat import Heartbeat

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
    return config

async def main():
    config = load_config()
    exchange = ExchangeWrapper(config)
    await exchange.connect()
    notifier = Notifier(config)
    await notifier.connect()
    ingestor = Ingestor(config, exchange)
    strategist = Strategist(config, exchange)
    executor = Executor(config, exchange, strategist, notifier)
    heartbeat = Heartbeat(config, exchange, notifier, executor)
    await asyncio.gather(
        ingestor.run(),
        strategist.run(),
        executor.run(),
        heartbeat.run()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
