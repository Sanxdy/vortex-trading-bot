from telegram import Bot
from typing import Optional

class Notifier:
    def __init__(self, config: dict):
        self.token = config["notifications"]["telegram"]["token"]
        self.chat_id = config["notifications"]["telegram"]["chat_id"]
        self.bot: Optional[Bot] = None

    async def connect(self):
        self.bot = Bot(self.token)
        await self.bot.get_me()
        print("Telegram bot connected")

    async def send_message(self, message: str):
        if not self.bot:
            await self.connect()
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message)
        except Exception as e:
            print(f"Telegram error: {e}")

    async def close(self):
        if self.bot:
            await self.bot.close()
