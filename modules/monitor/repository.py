import json
import threading
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class WatchlistRepository:
    """
    Thread-safe repository for watchlist data.
    """
    _instance = None
    _lock = threading.Lock()
    DATA_DIR = Path("/root/Trade_db/data")
    FILE_PATH = DATA_DIR / "watchlist.json"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(WatchlistRepository, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Ensure directory exists
        if not self.DATA_DIR.exists():
            self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def load_all(self) -> Dict[str, Any]:
        """Load all watchlist items."""
        with self._lock:
            if not self.FILE_PATH.exists():
                return {}
            try:
                with open(self.FILE_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load watchlist: {e}")
                return {}

    def save_all(self, data: Dict[str, Any]):
        """Save all watchlist items."""
        with self._lock:
            try:
                with open(self.FILE_PATH, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed to save watchlist: {e}")

    def add_item(self, symbol: str, item: Dict[str, Any]):
        """Add or update a single item."""
        data = self.load_all()
        data[symbol] = item
        self.save_all(data)

    def remove_item(self, symbol: str) -> bool:
        """Remove an item by symbol."""
        data = self.load_all()
        if symbol in data:
            del data[symbol]
            self.save_all(data)
            return True
        return False

    def get_item(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get a single item."""
        data = self.load_all()
        return data.get(symbol)
