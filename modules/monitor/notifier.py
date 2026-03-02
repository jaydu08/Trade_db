import logging
import time
import requests
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

class Notifier:
    """
    Handles sending notifications (Telegram).
    Safe for cross-thread usage.
    Using direct HTTP API to avoid asyncio complexity in threads.
    """
    
    @staticmethod
    def send_telegram(chat_id: int, message: str, max_retries: int = 3):
        """
        Send a message to a specific chat_id using synchronous requests.
        Retries up to max_retries times with exponential backoff on failure.
        """
        if not chat_id:
            logger.warning("No chat_id provided for notification.")
            return

        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            logger.error("TELEGRAM_BOT_TOKEN not found in environment.")
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        for attempt in range(max_retries):
            try:
                resp = requests.post(url, json=payload, timeout=10)
                if resp.status_code == 200:
                    logger.info(f"Notification sent to {chat_id} (attempt {attempt + 1})")
                    return
                else:
                    logger.warning(
                        f"Telegram API Error (attempt {attempt + 1}/{max_retries}): "
                        f"{resp.status_code} - {resp.text}"
                    )
            except Exception as e:
                logger.warning(f"Failed to send telegram message (attempt {attempt + 1}/{max_retries}): {e}")
            
            # Exponential backoff: 2s, 4s, 8s
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)
        
        logger.error(f"Failed to send telegram message to {chat_id} after {max_retries} attempts.")

    @staticmethod
    def broadcast(message: str):
        """
        Send to all admin users or last active chat.
        """
        # Try to get admin ID
        admin_id = os.getenv("TELEGRAM_ADMIN_ID")
        # Also try ALLOWED_USER_IDS
        if not admin_id:
            allowed = os.getenv("ALLOWED_USER_IDS")
            if allowed:
                admin_id = allowed.split(",")[0]
        
        if admin_id:
            try:
                Notifier.send_telegram(int(admin_id), message)
            except ValueError:
                logger.error(f"Invalid admin ID: {admin_id}")
        else:
            # Fallback to last chat id from bot module if available
            try:
                from interface.telegram_bot import LAST_CHAT_ID
                if LAST_CHAT_ID:
                    Notifier.send_telegram(LAST_CHAT_ID, message)
                else:
                    logger.warning("No target found for broadcast.")
            except ImportError:
                logger.warning("Could not import LAST_CHAT_ID")
