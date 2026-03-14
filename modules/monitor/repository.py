import json
import threading
import logging
import shutil
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from config.settings import DATA_DIR as PROJECT_DATA_DIR

logger = logging.getLogger(__name__)

class WatchlistRepository:
    """
    Thread-safe repository for watchlist data.
    """
    _instance = None
    _lock = threading.RLock()
    DATA_DIR = Path(PROJECT_DATA_DIR)
    FILE_PATH = DATA_DIR / "watchlist.json"
    KEY_PATTERN = re.compile(r"^[A-Z]{2,5}:.+$")

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
            return self._load_all_unlocked()

    def save_all(self, data: Dict[str, Any]):
        """Save all watchlist items."""
        with self._lock:
            self._save_all_unlocked(data)

    def add_item(self, symbol: str, item: Dict[str, Any]):
        """Add or update a single item."""
        with self._lock:
            data = self._load_all_unlocked()
            data[symbol] = item
            self._save_all_unlocked(data)

    def upsert_by_symbol_market(self, item: Dict[str, Any]) -> str:
        """
        Upsert one item by (symbol, market), preserving canonical key format.
        Returns the effective key.
        """
        symbol = str(item.get("symbol", "")).strip()
        market = str(item.get("market", "")).strip().upper()
        if not symbol or not market:
            raise ValueError("item must include non-empty symbol and market")

        with self._lock:
            data = self._load_all_unlocked()
            existing_key = self._find_key_unlocked(data, symbol=symbol, market=market)
            target_key = existing_key or self.build_item_key(symbol, market)
            data[target_key] = item
            self._save_all_unlocked(data)
            return target_key

    @staticmethod
    def build_item_key(symbol: str, market: str) -> str:
        """Build canonical key for watchlist storage."""
        return f"{market}:{symbol}"

    def add_unique_by_symbol_market(self, item: Dict[str, Any]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Atomically add one item if (symbol, market) does not exist.
        Returns: (added, key, existing_item)
        """
        symbol = str(item.get("symbol", "")).strip()
        market = str(item.get("market", "")).strip().upper()
        if not symbol or not market:
            raise ValueError("item must include non-empty symbol and market")

        with self._lock:
            data = self._load_all_unlocked()
            existing_key = self._find_key_unlocked(data, symbol=symbol, market=market)
            if existing_key:
                return False, existing_key, data.get(existing_key)

            item_key = self.build_item_key(symbol, market)
            data[item_key] = item
            self._save_all_unlocked(data)
            return True, item_key, None

    def remove_item(self, symbol: str) -> bool:
        """Remove an item by symbol."""
        with self._lock:
            data = self._load_all_unlocked()
            if symbol in data:
                del data[symbol]
                self._save_all_unlocked(data)
                return True
            return False

    def remove_first_match(self, query: str) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """
        Atomically remove first matched item by:
        1) exact key match
        2) exact symbol (case-insensitive)
        3) name contains query
        Returns: (removed, removed_key, removed_item)
        """
        query = (query or "").strip()
        if not query:
            return False, None, None

        with self._lock:
            data = self._load_all_unlocked()

            # Exact key match first (supports both legacy and canonical keys)
            if query in data:
                item = data.pop(query)
                self._save_all_unlocked(data)
                return True, query, item

            q_upper = query.upper()
            matched_key = None
            for k, v in data.items():
                symbol = str(v.get("symbol", "")).upper()
                name = str(v.get("name", ""))
                if symbol == q_upper or k.upper() == q_upper or query in name:
                    matched_key = k
                    break

            if matched_key:
                item = data.pop(matched_key)
                self._save_all_unlocked(data)
                return True, matched_key, item

            return False, None, None

    def find_matches(self, query: str) -> list[Tuple[str, Dict[str, Any]]]:
        """
        Find matched items without modifying storage.
        Match rules are aligned with remove_first_match.
        """
        query = (query or "").strip()
        if not query:
            return []

        with self._lock:
            data = self._load_all_unlocked()
            matches: list[Tuple[str, Dict[str, Any]]] = []
            q_upper = query.upper()
            for k, v in data.items():
                symbol = str(v.get("symbol", "")).upper()
                name = str(v.get("name", ""))
                if query == k or symbol == q_upper or k.upper() == q_upper or query in name:
                    matches.append((k, v))
            return matches

    def get_item(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get a single item."""
        with self._lock:
            data = self._load_all_unlocked()
            return data.get(symbol)

    def _find_key_unlocked(self, data: Dict[str, Any], symbol: str, market: str) -> Optional[str]:
        """Find existing key by symbol+market under lock. Supports legacy keys."""
        target_symbol = str(symbol).upper()
        target_market = str(market).upper()
        canonical = self.build_item_key(symbol, market)

        # Canonical key direct hit
        if canonical in data:
            return canonical

        # Legacy key fallback or mismatched key shapes
        for k, v in data.items():
            if str(v.get("symbol", "")).upper() == target_symbol and str(v.get("market", "")).upper() == target_market:
                return k
        return None

    @staticmethod
    def _parse_dt_safe(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).strip())
        except Exception:
            return None

    @classmethod
    def _choose_newer_item(cls, left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
        """Choose the newer item by added_at/last_alert_at fallback."""
        left_added = cls._parse_dt_safe(left.get("added_at"))
        right_added = cls._parse_dt_safe(right.get("added_at"))
        if left_added and right_added:
            return left if left_added >= right_added else right
        if right_added and not left_added:
            return right
        if left_added and not right_added:
            return left

        left_alert = cls._parse_dt_safe(left.get("last_alert_at"))
        right_alert = cls._parse_dt_safe(right.get("last_alert_at"))
        if left_alert and right_alert:
            return left if left_alert >= right_alert else right
        return right

    def migrate_legacy_keys(self, create_backup: bool = True) -> Dict[str, Any]:
        """
        Migrate legacy watchlist keys (symbol) to canonical keys (MARKET:SYMBOL).
        Idempotent: already-canonical data will remain unchanged.
        """
        with self._lock:
            data = self._load_all_unlocked()
            if not data:
                return {
                    "total": 0,
                    "legacy_keys": 0,
                    "migrated": 0,
                    "already_canonical": 0,
                    "conflicts": 0,
                    "invalid": 0,
                    "changed": False,
                    "backup_path": None,
                }

            migrated: Dict[str, Any] = {}
            stats = {
                "total": len(data),
                "legacy_keys": 0,
                "migrated": 0,
                "already_canonical": 0,
                "conflicts": 0,
                "invalid": 0,
                "changed": False,
                "backup_path": None,
            }

            for key, item in data.items():
                symbol = str(item.get("symbol", "")).strip()
                market = str(item.get("market", "")).strip().upper()
                if not symbol or not market:
                    stats["invalid"] += 1
                    continue

                canonical = self.build_item_key(symbol, market)
                if key == canonical:
                    stats["already_canonical"] += 1
                else:
                    stats["legacy_keys"] += 1
                    stats["migrated"] += 1

                if canonical in migrated:
                    stats["conflicts"] += 1
                    migrated[canonical] = self._choose_newer_item(migrated[canonical], item)
                else:
                    migrated[canonical] = item

            # Detect whether effective data changed.
            if migrated != data:
                stats["changed"] = True

            if stats["changed"] and create_backup and self.FILE_PATH.exists():
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = self.FILE_PATH.with_name(f"{self.FILE_PATH.name}.bak.{ts}")
                shutil.copy2(self.FILE_PATH, backup_path)
                stats["backup_path"] = str(backup_path)

            if stats["changed"]:
                self._save_all_unlocked(migrated)

            return stats

    def audit_key_format(self) -> Dict[str, int]:
        """
        Audit watchlist key format without modifying any data.
        Returns counters for total/canonical/legacy/invalid_items.
        """
        with self._lock:
            data = self._load_all_unlocked()
            result = {
                "total": len(data),
                "canonical_keys": 0,
                "legacy_keys": 0,
                "invalid_items": 0,
            }

            for key, item in data.items():
                symbol = str(item.get("symbol", "")).strip()
                market = str(item.get("market", "")).strip().upper()
                if not symbol or not market:
                    result["invalid_items"] += 1
                    continue

                canonical = self.build_item_key(symbol, market)
                if key == canonical and self.KEY_PATTERN.match(str(key)):
                    result["canonical_keys"] += 1
                else:
                    result["legacy_keys"] += 1

            return result

    def _load_all_unlocked(self) -> Dict[str, Any]:
        """Load watchlist data. Caller must hold lock."""
        if not self.FILE_PATH.exists():
            return {}
        try:
            with open(self.FILE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load watchlist: {e}")
            return {}

    def _save_all_unlocked(self, data: Dict[str, Any]):
        """Save watchlist data. Caller must hold lock."""
        try:
            with open(self.FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save watchlist: {e}")
