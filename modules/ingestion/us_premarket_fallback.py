"""Session-bound Nasdaq/Yahoo fallback; never substitute regular quotes."""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import logging
import math
import threading
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)
NY = ZoneInfo('America/New_York')


def parse_nasdaq(payload, symbol, now=None):
    now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(NY)
    data = payload.get('data') or {}
    if data.get('marketStatus') != 'Pre-Market':
        return None
    primary, secondary = data.get('primaryData') or {}, data.get('secondaryData') or {}
    try:
        def number(value):
            return float(str(value).replace('$', '').replace(',', '').strip())
        def timestamp(value):
            value = str(value).removeprefix('Closed at ')
            return dt.datetime.strptime(value, '%b %d, %Y %I:%M %p ET').replace(tzinfo=NY)
        stamp = timestamp(primary.get('lastTradeTimestamp'))
        reference_time = timestamp(secondary.get('lastTradeTimestamp'))
        price, previous, volume = (number(primary.get('lastSalePrice')),
                                   number(secondary.get('lastSalePrice')), number(primary.get('volume')))
        if not all(math.isfinite(v) and v > 0 for v in (price, previous, volume)):
            return None
        if (stamp.date() != now.date() or not dt.time(4) <= stamp.time() < dt.time(9, 30)
                or not dt.time(4) <= now.time().replace(tzinfo=None) < dt.time(9, 30)
                or stamp > now + dt.timedelta(minutes=5)
                or not 1 <= (stamp.date() - reference_time.date()).days <= 4):
            return None
    except (ValueError, TypeError):
        return None
    return {'symbol': symbol, 'name': data.get('companyName') or symbol,
            'price': price, 'prev_close': previous, 'regular_close': previous,
            'pct': round((price / previous - 1) * 100, 2),
            'pre_change': round(price - previous, 4), 'volume': volume, 'amount': volume * price,
            'pre_time': stamp.isoformat(), 'provider': 'Nasdaq premarket',
            'reference_close_source': 'Nasdaq completed regular session',
            'session_scope': 'premarket', 'amount_basis': 'premarket_volume_times_last_price'}


def parse_chart(payload, symbol, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    rows = payload.get('chart', {}).get('result') or []
    if not rows:
        return None
    data = rows[0]
    meta = data.get('meta', {})
    period = meta.get('currentTradingPeriod', {}).get('pre', {})
    start, end = period.get('start', 0), period.get('end', 0)
    if not start <= now.timestamp() < end:
        return None
    # The reference must be the latest completed regular session, not its
    # preceding close, and must not be from today's yet-unfinished session.
    regular_time = meta.get('regularMarketTime', 0)
    previous = meta.get('regularMarketPrice')
    if not (0 < regular_time < start and previous and float(previous) > 0):
        return None
    if (dt.datetime.fromtimestamp(start, NY).date() -
            dt.datetime.fromtimestamp(regular_time, NY).date()).days > 4:
        return None
    timestamps = data.get('timestamp') or []
    indicators = data.get('indicators', {}).get('quote') or [{}]
    closes, volumes = indicators[0].get('close', []), indicators[0].get('volume', [])
    samples = [(ts, float(close), float(volume or 0))
               for ts, close, volume in zip(timestamps, closes, volumes)
               if start <= ts < min(end, now.timestamp() + 1) and close is not None
               and math.isfinite(float(close)) and float(close) > 0
               and math.isfinite(float(volume or 0)) and float(volume or 0) >= 0]
    if not samples:
        return None
    samples.sort()
    price = samples[-1][1]
    volume = sum(sample[2] for sample in samples)
    if volume <= 0:
        return None
    return {'symbol': symbol, 'name': meta.get('shortName') or meta.get('longName') or symbol,
            'price': price, 'prev_close': float(previous), 'regular_close': float(previous),
            'pct': round((price / float(previous) - 1) * 100, 2),
            'pre_change': round(price - float(previous), 4),
            'volume': volume, 'amount': volume * price,
            'pre_time': dt.datetime.fromtimestamp(samples[-1][0], NY).isoformat(),
            'provider': 'Yahoo minute bars', 'reference_close_source': 'Yahoo completed regular session',
            'session_scope': 'premarket', 'amount_basis': 'premarket_volume_times_last_price'}


def fetch_premarket(symbols, timeout=10):
    now = dt.datetime.now(NY)
    if now.weekday() >= 5 or not dt.time(4) <= now.time().replace(tzinfo=None) < dt.time(9, 30):
        return []
    throttled = threading.Event()

    def fetch(symbol):
        if throttled.is_set():
            return None
        try:
            response = requests.get(
                'https://api.nasdaq.com/api/quote/' + symbol.replace('.', '-') + '/info',
                params={'assetclass': 'stocks'}, headers={'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json, text/plain, */*', 'Origin': 'https://www.nasdaq.com'},
                timeout=timeout)
            if response.status_code in {403, 429}:
                throttled.set()
                return None
            response.raise_for_status()
            row = parse_nasdaq(response.json(), symbol)
            if row:
                return row
            # A provider can omit extended-session fields. Only actual minute
            # bars with positive premarket volume qualify as a second source.
            response = requests.get(
                'https://query1.finance.yahoo.com/v8/finance/chart/' + symbol.replace('.', '-'),
                params={'interval': '1m', 'range': '1d', 'includePrePost': 'true'},
                headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
            if response.status_code in {403, 429}:
                throttled.set()  # Respect provider denial/rate limit, no alternate-domain hopping.
                return None
            response.raise_for_status()
            return parse_chart(response.json(), symbol)
        except (requests.RequestException, ValueError, TypeError, KeyError):
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        results = [row for row in pool.map(fetch, symbols) if row]
    logger.warning('US premarket fallback: requested=%s quotes=%s throttled=%s',
                   len(symbols), len(results), throttled.is_set())
    return results
