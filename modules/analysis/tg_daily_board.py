"""Date-stable archive of the exact shortlist selected for the daily TG message."""
import datetime
import json
import os
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BOARD_DIR = os.getenv('TG_DAILY_BOARD_DIR', os.path.join(ROOT, 'data', 'tg_daily_boards'))


def _path(market, day):
    if market not in {'CN', 'HK', 'US'}:
        raise ValueError('invalid market')
    parsed = datetime.date.fromisoformat(str(day))
    return os.path.join(BOARD_DIR, f'{market}_{parsed.isoformat()}.json')


def read_board(market, day):
    try:
        with open(_path(market, day), encoding='utf-8') as handle:
            result = json.load(handle)
        return result if isinstance(result, dict) else None
    except (OSError, ValueError):
        return None


def save_board(market, day, items):
    path = _path(market, day)
    os.makedirs(BOARD_DIR, exist_ok=True)
    result = {'market': market, 'date': str(day), 'items': items, 'board_source': 'tg_selected_snapshot'}
    fd, tmp = tempfile.mkstemp(prefix='.tg_daily_', dir=BOARD_DIR)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(result, handle, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return result
