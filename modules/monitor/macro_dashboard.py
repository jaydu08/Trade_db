"""Cached macro and commodity dashboard. Refresh runs outside HTTP requests."""
import csv
import io
import json
import logging
import math
import os
import tempfile
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo
from zipfile import ZipFile

LOG = logging.getLogger(__name__)
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SNAPSHOT_PATH = os.getenv('MACRO_DASHBOARD_SNAPSHOT', os.path.join(ROOT, 'data', 'macro_dashboard_snapshot.json'))
FRED_SERIES = {
    'DFEDTARU': ('联邦基金目标利率上限', '%', '政策'),
    'DGS2': ('美债 2Y', '%', '利率'),
    'DGS10': ('美债 10Y', '%', '利率'),
    'DFII10': ('美债 10Y 实际利率', '%', '利率'),
    'T10YIE': ('10Y 盈亏平衡通胀率', '%', '通胀预期'),
    'DTWEXBGS': ('广义美元指数', '指数', '美元'),
}
FUTURES = {
    'CL': ('NYMEX原油', 'WTI 原油', '美元/桶', '能源'),
    'OIL': ('布伦特原油', 'Brent 原油', '美元/桶', '能源'),
    'GC': ('COMEX黄金', 'COMEX 黄金', '美元/盎司', '贵金属'),
    'SI': ('COMEX白银', 'COMEX 白银', '美元/盎司', '贵金属'),
    'HG': ('COMEX铜', 'COMEX 铜', '美分/磅', '工业金属'),
}


def finite(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def read_snapshot():
    try:
        with open(SNAPSHOT_PATH, encoding='utf-8') as handle:
            result = json.load(handle)
            return result if isinstance(result, dict) else {}
    except (OSError, ValueError):
        return {}


def parse_fred_csv(raw):
    history = {key: [] for key in FRED_SERIES}
    for row in csv.DictReader(io.StringIO(raw)):
        day = row.get('observation_date') or row.get('DATE')
        if not day:
            continue
        for key in FRED_SERIES:
            value = finite(row.get(key))
            if value is not None:
                history[key].append((day, value))
    result = []
    for key, (name, unit, group) in FRED_SERIES.items():
        values = history[key]
        if not values:
            continue
        day, value = values[-1]
        previous = values[-2][1] if len(values) > 1 else None
        result.append({'id': key, 'name': name, 'unit': unit, 'group': group,
                       'value': value, 'change': round(value - previous, 4) if previous is not None else None,
                       'asof': day, 'source': 'FRED / 原始发布机构',
                       'url': 'https://fred.stlouisfed.org/series/' + key})
    return result


def fetch_fred():
    query = urlencode({'id': ','.join(FRED_SERIES)})
    with urlopen('https://fred.stlouisfed.org/graph/fredgraph.csv?' + query, timeout=20) as response:
        raw = response.read(4_000_001)
    if len(raw) > 4_000_000:
        raise ValueError('FRED CSV exceeds size limit')
    result = parse_fred_response(raw)
    if len(result) < 4:
        raise ValueError('too few FRED series')
    return result


def parse_fred_response(raw):
    """FRED may ZIP different calendars into separate CSVs for one request."""
    if raw.startswith(b'PK'):
        with ZipFile(io.BytesIO(raw)) as archive:
            result = []
            for info in archive.infolist():
                if info.filename.endswith('.csv') and info.file_size <= 2_000_000:
                    result.extend(parse_fred_csv(archive.read(info).decode('utf-8-sig')))
            return result
    return parse_fred_csv(raw.decode('utf-8-sig'))


def parse_futures_frame(frame):
    by_name = {str(row.get('名称', '')).strip(): row for _, row in frame.iterrows()}
    result = []
    for key, (source_name, name, unit, group) in FUTURES.items():
        row = by_name.get(source_name)
        if row is None:
            continue
        value = finite(row.get('最新价'))
        if value is None or value <= 0:
            continue
        day = str(row.get('日期', '')).strip()
        clock = str(row.get('行情时间', '')).strip()
        try:
            asof = datetime.fromisoformat(day + 'T' + clock).replace(tzinfo=ZoneInfo('Asia/Shanghai')).isoformat()
        except ValueError:
            asof = ''
        result.append({'id': key, 'name': name, 'unit': unit, 'group': group,
                       'value': value, 'change_pct': finite(row.get('涨跌幅')),
                       'asof': asof, 'source': '新浪外盘期货 / AkShare',
                       'url': 'https://finance.sina.com.cn/money/future/hf.html'})
    return result


def fetch_futures():
    import akshare as ak
    result = parse_futures_frame(ak.futures_foreign_commodity_realtime(symbol=list(FUTURES)))
    if len(result) < 3:
        raise ValueError('too few futures quotes')
    return result


def refresh_snapshot(force=False, now=None):
    """Keep each successful feed if the other fails; never overwrite it with empties."""
    now = now or time.time()
    snapshot = read_snapshot()
    errors = {}
    for key, ttl, fetcher in [('macro', 6 * 3600, fetch_fred),
                              ('futures', 15 * 60, fetch_futures)]:
        if not force and snapshot.get(key) and now - snapshot.get(key + '_checked_at', 0) < ttl:
            continue
        try:
            snapshot[key] = fetcher()
            snapshot[key + '_checked_at'] = now
        except Exception as exc:
            errors[key] = str(exc)[:180]
            LOG.warning('Macro dashboard %s refresh failed: %s', key, exc)
    from modules.monitor.market_activity import (fetch_binance, fetch_dex,
                                                  fetch_sentiment, update_crypto_history)
    activity = snapshot.setdefault('crypto_activity', {})
    for key, ttl, fetcher in [('spot', 3600, lambda: fetch_binance(False)),
                              ('perp', 3600, lambda: fetch_binance(True)),
                              ('dex', 1800, fetch_dex),
                              ('sentiment', 6 * 3600, fetch_sentiment)]:
        old = activity.get(key, {})
        if not force and old and now - old.get('checked_at', 0) < ttl:
            continue
        try:
            activity[key] = {**fetcher(), 'checked_at': now}
        except Exception as exc:
            errors['crypto_' + key] = str(exc)[:180]
            LOG.warning('Crypto activity %s refresh failed: %s', key, exc)
    update_crypto_history(snapshot, now)
    snapshot['errors'] = errors
    snapshot['refreshed_at'] = now
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix='.macro_dashboard_', dir=os.path.dirname(SNAPSHOT_PATH))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(snapshot, handle, ensure_ascii=False)
        os.replace(tmp_path, SNAPSHOT_PATH)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return snapshot


def commodity_movers():
    """Read existing CF daily bars; this is not a live full-universe futures feed."""
    from sqlmodel import select
    from core.db import get_ledger_session
    from domain.ledger.analytics import TrendDailyBar
    from modules.monitor.commodity_scanner import CommodityScanner

    with get_ledger_session() as session:
        latest = session.exec(select(TrendDailyBar.date).where(TrendDailyBar.market == 'CF')
                              .order_by(TrendDailyBar.date.desc()).limit(1)).first()
        if not latest:
            return [], None
        bars = session.exec(select(TrendDailyBar).where(TrendDailyBar.market == 'CF')
                            .where(TrendDailyBar.date == latest)).all()
    result = []
    for bar in bars:
        if not bar.open or bar.open <= 0 or not bar.close or bar.close <= 0:
            continue
        change = (bar.close / bar.open - 1) * 100
        if not math.isfinite(change) or abs(change) < 1.5:
            continue
        result.append({'symbol': bar.symbol, 'name': bar.name,
                       'category': CommodityScanner._determine_category(bar.name),
                       'price': bar.close, 'change_pct': round(change, 2),
                       'date': str(bar.date), 'source': bar.source,
                       'from_daily_monitor': bar.source == 'commodity'})
    result.sort(key=lambda row: (row['from_daily_monitor'], abs(row['change_pct'])), reverse=True)
    return result[:20], str(latest)


def get_dashboard():
    snapshot = read_snapshot()
    from modules.monitor.market_activity import stock_dashboard, crypto_dashboard
    try:
        movers, mover_date = commodity_movers()
    except Exception:
        LOG.exception('Commodity movers unavailable')
        movers, mover_date = [], None
    return {'macro': snapshot.get('macro', []), 'futures': snapshot.get('futures', []),
            'stock_activity': stock_dashboard(), 'crypto_activity': crypto_dashboard(snapshot),
            'macro_checked_at': snapshot.get('macro_checked_at'),
            'futures_checked_at': snapshot.get('futures_checked_at'),
            'errors': snapshot.get('errors', {}), 'movers': movers,
            'mover_date': mover_date, 'asof': time.time(),
            'notes': ['日度宏观指标与期货报价更新频率不同；请以各项时间戳为准。',
                      '国内商品异动取现有 CF 趋势日线，非全市场实时期货排名。']}


if __name__ == '__main__':
    result = refresh_snapshot(force=True)
    print('macro:', len(result.get('macro', [])), 'futures:', len(result.get('futures', [])),
          'errors:', result.get('errors', {}))
