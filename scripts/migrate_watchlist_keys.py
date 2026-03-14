#!/usr/bin/env python3
"""
Migrate watchlist keys from legacy `symbol` to canonical `MARKET:SYMBOL`.
Creates a timestamped backup before writing changes.
"""
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.monitor.repository import WatchlistRepository


def main() -> None:
    repo = WatchlistRepository()
    stats = repo.migrate_legacy_keys(create_backup=True)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
