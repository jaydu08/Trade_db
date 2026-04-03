import os
import sys
import logging
import asyncio
import atexit
import fcntl
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
from modules.monitor.repository import WatchlistRepository



def _acquire_single_instance_lock():
    """防止重复启动导致定时任务和推送重复执行。"""
    lock_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(lock_dir, exist_ok=True)
    lock_path = os.path.join(lock_dir, "trade_db.lock")

    fp = open(lock_path, "w")
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.error("Another Trade_db instance is already running, exiting.")
        try:
            fp.close()
        except Exception:
            pass
        return None

    fp.seek(0)
    fp.truncate()
    fp.write(str(os.getpid()))
    fp.flush()

    def _release():
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            fp.close()
        except Exception:
            pass

    atexit.register(_release)
    return fp

def main():
    """
    Main Entry Point
    """
    logger.info("Starting Trade_db System...")

    lock_fp = _acquire_single_instance_lock()
    if lock_fp is None:
        return
    
    # 1. Init DB
    db_manager.init_meta_db()
    db_manager.init_ledger_db()

    # 1.5 Watchlist schema audit (read-only)
    try:
        repo = WatchlistRepository()
        audit = repo.audit_key_format()
        auto_migrate = os.getenv("WATCHLIST_AUTO_MIGRATE", "0") == "1"

        if auto_migrate and audit["legacy_keys"] > 0:
            stats = repo.migrate_legacy_keys(create_backup=True)
            logger.info("Watchlist auto-migrate executed: %s", stats)
            # Re-audit after migration for accurate startup health log
            audit = repo.audit_key_format()

        if audit["legacy_keys"] > 0 or audit["invalid_items"] > 0:
            logger.warning(
                "Watchlist key format check: total=%s canonical=%s legacy=%s invalid=%s",
                audit["total"],
                audit["canonical_keys"],
                audit["legacy_keys"],
                audit["invalid_items"],
            )
        else:
            logger.info(
                "Watchlist key format check passed: total=%s canonical=%s",
                audit["total"],
                audit["canonical_keys"],
            )
    except Exception as e:
        logger.warning(f"Watchlist key format audit skipped due to error: {e}")
    
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
