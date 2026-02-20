import os
import sys
import logging
import asyncio
from dotenv import load_dotenv

# Load env
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("trade_db.log")
    ]
)
logger = logging.getLogger(__name__)

from core.db import db_manager
from core.scheduler import task_scheduler
from interface.telegram_bot import create_bot

def main():
    """
    Main Entry Point
    """
    logger.info("Starting Trade_db System...")
    
    # 1. Init DB
    db_manager.init_meta_db()
    db_manager.init_ledger_db()
    
    # 2. Start Scheduler
    task_scheduler.start()
    
    # 3. Start Bot (Blocking)
    bot = create_bot()
    if bot:
        bot.run()
    else:
        logger.error("Failed to start Bot. Please check TELEGRAM_BOT_TOKEN.")
        # Keep running for scheduler if bot fails
        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass

    # Shutdown
    task_scheduler.stop()

if __name__ == "__main__":
    main()
