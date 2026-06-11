import os
import sys
import yaml
import asyncio
import logging

sys.path.insert(0, os.path.dirname(__file__))

from exchange_wrapper import ExchangeWrapper
from strategist import Strategist
from executor import Executor
from notifier import Notifier
from heartbeat import Heartbeat
from ingestor import Ingestor


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "config-futures.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    config["exchange"]["api_key"] = os.getenv("FUTURES_API_KEY", config["exchange"]["api_key"])
    config["exchange"]["api_secret"] = os.getenv("FUTURES_API_SECRET", config["exchange"]["api_secret"])
    config["exchange"]["testnet"] = os.getenv("FUTURES_TESTNET", str(config["exchange"]["testnet"])).lower() == "true"
    config["futures"]["leverage"] = int(os.getenv("FUTURES_LEVERAGE", config["futures"]["leverage"]))
    return config


async def main():
    config = load_config()
    exchange = ExchangeWrapper(config)
    await exchange.connect()

    # Set leverage and margin mode for each futures pair
    for pair_config in config.get("pairs", []):
        symbol = pair_config["name"]
        lev = config["futures"]["leverage"]
        margin_mode = config["futures"]["margin_mode"]
        try:
            await exchange.exchange.set_leverage(lev, symbol)
            await exchange.exchange.set_margin_mode(margin_mode, symbol)
            print(f"  {symbol}: leverage={lev}x, margin={margin_mode}")
        except Exception as e:
            print(f"  {symbol}: setup error: {e}")

    config["active_profile"] = "sideway"
    config["timezone"] = int(os.getenv("TIMEZONE", "7"))

    notifier = Notifier(config)
    await notifier.connect()
    # Skip start_polling — same bot token conflicts with spot bot polling

    strategist = Strategist(config, exchange)
    ingestor = Ingestor(config, exchange)
    asyncio.create_task(ingestor.run())
    asyncio.create_task(strategist.run())

    executor = Executor(config, exchange, strategist, notifier)
    await executor.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(main())
