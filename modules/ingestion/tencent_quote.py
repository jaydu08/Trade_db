"""Public Tencent quotes used only when the primary provider is unavailable.

Normalize currency/volume units, retain source timestamps and never interpret a
regular-session US quote as pre/post-market data.
"""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import logging
import math
import re
import time

import requests

logger = logging.getLogger(__name__)
_SINA_RETRY_AFTER = 0.0


def sina_available():
    return time.monotonic() >= _SINA_RETRY_AFTER


def mark_sina_denied():
    global _SINA_RETRY_AFTER
    _SINA_RETRY_AFTER = max(_SINA_RETRY_AFTER, time.monotonic() + 300)


def quote_code(symbol: str, market: str) -> str:
    raw = str(symbol).strip()
    market = market.upper()
    if market == 'US':
        if raw.split('.', 1)[0] in {'105', '106', '107'}:
            raw = raw.split('.', 1)[1]
        raw = raw.removeprefix('gb_').replace('$', '.').upper()
        return 'us' + {'INX': 'INX', '.INX': 'INX'}.get(raw, raw)
    if market == 'HK':
        raw = raw.removeprefix('hk')
        return 'hk' + (raw.zfill(5) if raw.isdigit() else raw)
    if market == 'CN':
        if raw[:2].lower() in {'sh', 'sz', 'bj'}:
            return raw.lower()
        if '.' in raw and raw.rsplit('.', 1)[1].upper() in {'SH', 'SZ', 'BJ'}:
            code, exchange = raw.rsplit('.', 1)
            return exchange.lower() + code
        exchange = 'bj' if raw.startswith(('4', '8', '92')) else 'sh' if raw.startswith(('5', '6', '9')) else 'sz'
        return exchange + raw
    raise ValueError('Unsupported Tencent quote market: ' + market)


def _number(value):
    try:
        result = float(str(value).replace(',', '').strip())
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def parse_quotes(text: str, code_to_symbol: dict[str, str], market: str) -> dict[str, dict]:
    result = {}
    for code, raw in re.findall(r'v_([A-Za-z0-9_.-]+)="([^"\n]*)"', text):
        if code not in code_to_symbol:
            continue
        fields = raw.split('~')
        if len(fields) < 38:
            continue
        price, previous = _number(fields[3]), _number(fields[4])
        pct = _number(fields[32])
        if not price or price <= 0 or not previous or previous <= 0 or pct is None:
            continue
        stamp = fields[30].strip()
        if len(stamp) == 14 and stamp.isdigit():
            try:
                stamp = dt.datetime.strptime(stamp, '%Y%m%d%H%M%S').isoformat(sep=' ')
            except ValueError:
                continue
        stamp = stamp.replace('/', '-')
        try:
            dt.datetime.fromisoformat(stamp)
        except ValueError:
            continue
        volume = _number(fields[6])
        amount = _number(fields[37])
        # Mainland: hands and CNY 10,000; HK/US: shares and local currency.
        if market == 'CN':
            volume = volume * 100 if volume is not None else None
            amount = amount * 10000 if amount is not None else None
        symbol = code_to_symbol[code]
        cap = _number(fields[45]) if len(fields) > 45 else None
        result[symbol] = {
            'symbol': symbol, 'name': fields[1], 'price': price,
            'prev_close': previous, 'change': _number(fields[31]), 'pct_chg': pct,
            'open': _number(fields[5]), 'high': _number(fields[33]),
            'day_low': _number(fields[34]), 'volume': volume, 'amount': amount,
            'turnover_rate': _number(fields[38]) if len(fields) > 38 else None,
            'timestamp': stamp, 'provider': 'Tencent',
            'currency': {'CN': 'CNY', 'HK': 'HKD', 'US': 'USD'}[market],
            'timestamp_timezone': 'America/New_York' if market == 'US' else 'Asia/Shanghai',
            'session_scope': 'regular', 'adjustment': 'unadjusted',
            'amount_basis': 'provider_reported',
            'market_cap': cap * 100000000 if cap and cap > 0 else None,
        }
    return result


def fetch_quotes(symbols, market: str, timeout: int = 12) -> dict[str, dict]:
    market = market.upper()
    mapping = {quote_code(s, market): str(s) for s in dict.fromkeys(symbols) if str(s).strip()}
    codes = list(mapping)
    batches = [codes[i:i + 100] for i in range(0, len(codes), 100)]

    def fetch(batch):
        try:
            response = requests.get('https://qt.gtimg.cn/q=' + ','.join(batch),
                                    headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
            response.raise_for_status()
            response.encoding = 'gbk'
            return parse_quotes(response.text, {code: mapping[code] for code in batch}, market)
        except (requests.RequestException, ValueError) as exc:
            logger.warning('Tencent fallback unavailable: market=%s size=%s error=%s',
                           market, len(batch), type(exc).__name__)
            return {}

    results = {}
    if batches:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(batches))) as pool:
            for rows in pool.map(fetch, batches):
                results.update(rows)
    return results
