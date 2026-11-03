"""Market activity inputs for the macro dashboard.

Stock turnover is a sum over Trade_db's Sina stock quote universe, not an
official exchange-wide turnover statistic. Crypto CEX turnover covers Binance
USDT spot and USDT-M perpetual pairs only, so the two venues are not added.
"""
import json
import logging
import math
import os
import tempfile
import time
from datetime import datetime
from statistics import median
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STOCK_PATH = os.getenv('MARKET_ACTIVITY_STOCK_PATH', os.path.join(ROOT, 'data', 'market_activity_stock.json'))
SHANGHAI = ZoneInfo('Asia/Shanghai')


def _finite(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def _json(url):
    request = Request(url, headers={'User-Agent': 'TradeDB-MacroDashboard/1.0'})
    with urlopen(request, timeout=18) as response:
        raw = response.read(32_000_001)
    if len(raw) > 32_000_000:
        raise ValueError('market activity response exceeds size limit')
    return json.loads(raw)


def _atomic_json(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.market_activity_', dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(value, handle, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def stock_history():
    try:
        with open(STOCK_PATH, encoding='utf-8') as handle:
            rows = json.load(handle)
        return rows if isinstance(rows, list) else []
    except (OSError, ValueError):
        return []


def record_stock_turnover(market, frame, now=None):
    """Record the full pre-ranking quote frame once its coverage is credible."""
    if market not in {'CN', 'HK'} or frame is None or frame.empty:
        return None
    if '成交额' not in frame or '代码' not in frame:
        return None
    import pandas as pd
    work = frame.drop_duplicates(subset='代码')
    amounts = pd.to_numeric(work['成交额'], errors='coerce')
    positive = amounts[(amounts > 0) & (amounts < 1e14)]
    minimum = 1000 if market == 'CN' else 500
    if len(positive) < minimum:
        LOG.warning('Stock turnover %s skipped: only %s positive quotes', market, len(positive))
        return None
    stamp = datetime.fromtimestamp(now or time.time(), SHANGHAI)
    row = {'market': market, 'day': stamp.date().isoformat(), 'amount': round(float(positive.sum()), 2),
           'quotes': len(work), 'traded': len(positive), 'asof': stamp.isoformat(),
           'source': 'Trade_db / 新浪股票行情覆盖池', 'unit': 'CNY' if market == 'CN' else 'HKD'}
    existing = stock_history()
    previous = max((item for item in existing if item.get('market') == market and item.get('day') < row['day']),
                   key=lambda item: item['day'], default=None)
    if previous and (row['traded'] < previous.get('traded', 0) * .75 or
                     abs(row['amount'] - previous.get('amount', 0)) < 1):
        LOG.warning('Stock turnover %s skipped: partial or unchanged holiday quote snapshot', market)
        return None
    rows = [item for item in existing if not (item.get('market') == market and item.get('day') == row['day'])]
    rows.append(row)
    rows.sort(key=lambda item: (item.get('day', ''), item.get('market', '')))
    _atomic_json(STOCK_PATH, rows[-240:])
    return row


def stock_dashboard():
    rows = stock_history()
    result = []
    for market in ('CN', 'HK'):
        history = sorted((r for r in rows if r.get('market') == market and _finite(r.get('amount'))),
                         key=lambda r: r['day'])
        if not history:
            continue
        current = history[-1]
        previous = history[-6:-1]
        baseline = median(r['amount'] for r in previous) if len(previous) >= 3 else None
        result.append({**current, 'baseline_5d': baseline,
                       'vs_baseline_pct': round((current['amount'] / baseline - 1) * 100, 1) if baseline else None,
                       'history': history[-20:]})
    return result


def sum_usdt_volume(tickers, symbols):
    """Only active USDT pairs, ignoring duplicate/stale ticker records."""
    seen = set()
    total = 0.0
    count = 0
    for row in tickers:
        symbol = row.get('symbol')
        value = _finite(row.get('quoteVolume'))
        if symbol not in symbols or symbol in seen or value is None or value < 0:
            continue
        seen.add(symbol)
        total += value
        count += 1
    return {'value': round(total, 2), 'pairs': count}


def fetch_binance(derivatives=False):
    base = 'https://fapi.binance.com/fapi/v1/' if derivatives else 'https://api.binance.com/api/v3/'
    info = _json(base + 'exchangeInfo')
    symbols = {r['symbol'] for r in info['symbols'] if r.get('quoteAsset') == 'USDT'
               and r.get('status') == 'TRADING'
               and (not derivatives or r.get('contractType') == 'PERPETUAL')}
    tickers = _json(base + 'ticker/24hr')
    result = sum_usdt_volume(tickers, symbols)
    if result['pairs'] < (100 if derivatives else 300):
        raise ValueError('too few active Binance USDT pairs')
    return result


def fetch_dex():
    data = _json('https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true')
    value = _finite(data.get('total24h'))
    previous = _finite(data.get('total48hto24h'))
    if value is None or value <= 0:
        raise ValueError('missing DefiLlama DEX total24h')
    return {'value': round(value, 2), 'previous_24h': round(previous, 2) if previous else None}


def fetch_sentiment():
    rows = _json('https://api.alternative.me/fng/?limit=2')['data']
    if not rows:
        raise ValueError('missing Fear and Greed data')
    value = _finite(rows[0].get('value'))
    if value is None or not 0 <= value <= 100:
        raise ValueError('invalid Fear and Greed value')
    return {'value': int(value), 'classification': rows[0].get('value_classification', ''),
            'timestamp': int(rows[0]['timestamp']),
            'previous': int(rows[1]['value']) if len(rows) > 1 else None}


def crypto_history(snapshot):
    return snapshot.get('crypto_activity_history', []) if isinstance(snapshot.get('crypto_activity_history'), list) else []


def update_crypto_history(snapshot, now):
    """Keep one observation per local hour for like-time rolling-24h comparisons."""
    local = datetime.fromtimestamp(now, SHANGHAI)
    day = local.date().isoformat()
    hour = local.hour
    activity = snapshot.get('crypto_activity', {})
    values = {key: activity[key]['value'] for key in ('spot', 'perp', 'dex')
              if isinstance(activity.get(key), dict) and _finite(activity[key].get('value')) is not None
              and now - activity[key].get('checked_at', 0) < 3 * 3600}
    if not values:
        return
    rows = [row for row in crypto_history(snapshot) if not (row.get('day') == day and row.get('hour') == hour)]
    rows.append({'day': day, 'hour': hour, 'asof': now, **values})
    snapshot['crypto_activity_history'] = sorted(rows, key=lambda row: (row['day'], row.get('hour', -1)))[-1440:]


def crypto_dashboard(snapshot):
    activity = snapshot.get('crypto_activity', {})
    history = crypto_history(snapshot)
    result = []
    specs = [('spot', '币安 USDT 现货', 'https://www.binance.com/zh-CN/markets/overview'),
             ('perp', '币安 USDT 永续', 'https://www.binance.com/zh-CN/futures/markets/overview'),
             ('dex', '全链 DEX 现货', 'https://defillama.com/dexs/'),
             ('sentiment', '恐惧贪婪指数', 'https://alternative.me/crypto/fear-and-greed-index/')]
    for key, name, url in specs:
        row = activity.get(key)
        if not isinstance(row, dict):
            continue
        local = datetime.fromtimestamp(row['checked_at'], SHANGHAI)
        past = [r[key] for r in history if r.get('day') != local.date().isoformat() and r.get('hour') == local.hour
                and _finite(r.get(key)) is not None][-7:]
        baseline = median(past) if len(past) >= 3 and key != 'sentiment' else None
        previous = row.get('previous_24h') if key == 'dex' else None
        result.append({**row, 'id': key, 'name': name, 'url': url,
                       'baseline_7d': baseline, 'vs_baseline_pct': round((row['value'] / baseline - 1) * 100, 1) if baseline else None,
                       'vs_previous_pct': round((row['value'] / previous - 1) * 100, 1) if previous else None,
                       'history': [{'day': r['day'], 'value': r[key]} for r in history if key in r][-14:]})
    return result
