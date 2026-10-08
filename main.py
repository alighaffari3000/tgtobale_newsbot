"""
main.py — Entry point. Initializes Telegram client, Bale client, DB, and starts the service.
"""

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from bale import BaleClient
from db import Database
from forwarder import Forwarder

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tg2bale.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        logger.critical("Missing required environment variable: %s", key)
        sys.exit(1)
    return value


async def main():
    api_id = int(_require_env("API_ID"))
    api_hash = _require_env("API_HASH")
    bale_token = _require_env("BALE_BOT_TOKEN")
    bale_channel_id = _require_env("BALE_CHANNEL_ID")
    session_name = os.getenv("SESSION_NAME", "tg2bale")
    db_path = os.getenv("DB_PATH", "tg2bale.db")

    raw_channels = _require_env("SOURCE_CHANNELS")
    source_channels = [ch.strip() for ch in raw_channels.split(",") if ch.strip()]
    if not source_channels:
        logger.critical("SOURCE_CHANNELS is empty. At least one channel is required.")
        sys.exit(1)

    raw_blocked = os.getenv("BLOCKED_WORDS", "")
    blocked_words = [w.strip() for w in raw_blocked.split(",") if w.strip()]
    if blocked_words:
        logger.info("Blocked words loaded: %s", blocked_words)

    logger.info("Starting tg2bale service")
    logger.info("Source channels: %s", source_channels)

    # ── Initialize components ────────────────────────────────────────────────
    db = Database(db_path)
    bale = BaleClient(bale_token, bale_channel_id)

    tg_client = TelegramClient(session_name, api_id, api_hash)

    async with tg_client:
        # Login check
        if not await tg_client.is_user_authorized():
            phone = input("Enter your Telegram phone number: ")
            await tg_client.send_code_request(phone)
            code = input("Enter the code you received: ")
            try:
                await tg_client.sign_in(phone, code)
            except SessionPasswordNeededError:
                password = input("Two-step verification enabled. Enter your password: ")
                await tg_client.sign_in(password=password)

        me = await tg_client.get_me()
        logger.info("Telegram login successful (account: %s)", me.username or me.id)

        forwarder = Forwarder(tg_client, bale, db, source_channels, blocked_words)
        forwarder.register_handlers()

        logger.info("Listening for new posts...")

        # Two background tasks:
        # 1. immediate_retry_loop  — retries once-failed messages after 1 minute
        # 2. connectivity_loop     — every 30 min, checks Bale and flushes pending queue
        tasks = [
            asyncio.create_task(forwarder.immediate_retry_loop()),
            asyncio.create_task(forwarder.connectivity_loop()),
        ]

        try:
            await tg_client.run_until_disconnected()
        finally:
            for t in tasks:
                t.cancel()
            bale.close()
            logger.info("Service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
