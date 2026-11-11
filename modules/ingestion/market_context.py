"""Explicit index identities and independent, dated vendor-market breadth.

This is NOT the Trend pool. Breadth counts the provider's full enumerated market
screen (including its listed product types), not an exchange-certified census.
"""
import concurrent.futures
import datetime as dt
import math
import threading
from zoneinfo import ZoneInfo
import requests

INDEXES = {
    'CN': [('sh000001', '上证指数', 'SSE'), ('sz399001', '深证成指', 'SZSE'),
           ('sz399006', '创业板指', 'SZSE'), ('sh000300', '沪深300', 'SSE/SZSE')],
    'HK': [('HSI', '恒生指数', 'HKEX'), ('HSCEI', '恒生中国企业', 'HKEX'), ('HSTECH', '恒生科技', 'HKEX')],
    'US': [('INX', '标普500', 'S&P DJI'), ('DJI', '道琼斯工业', 'S&P DJI'), ('IXIC', '纳斯达克综合', 'NASDAQ')],
}
FILTERS = {
    'CN': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048',
    'HK': 'm:128 t:3,m:128 t:4,m:128 t:1,m:128 t:2',
    'US': 'm:105,m:106,m:107',
}


def finite(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def full_market_rows(market, get=requests.get):
    params = dict(pn=1, pz=100, np=1, fltt=2, fid='f12', po=0,
                  fs=FILTERS[market], fields='f12,f13,f14,f2,f3,f5,f6,f124')
    stopped = threading.Event()
    def page(n):
        if stopped.is_set():
            raise ValueError('provider rejected request; remaining page requests cancelled')
        response = get('https://push2.eastmoney.com/api/qt/clist/get',
                       params=dict(params, pn=n), timeout=12)
        if response.status_code in (403, 429) or (isinstance(response.status_code,int) and response.status_code>=500):
            stopped.set()
        response.raise_for_status()
        data = response.json().get('data') or {}
        rows = data.get('diff')
        if not isinstance(rows, list) or not isinstance(data.get('total'), int):
            raise ValueError('market enumeration missing total/rows')
        return data['total'], rows
    total, first = page(1)
    if total < {'CN': 3000, 'HK': 1000, 'US': 3000}[market] or total > 25000:
        raise ValueError('unexpected market enumeration size')
    size = len(first)
    if not size:
        raise ValueError('empty market page')
    pages = math.ceil(total / size)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        rest = list(pool.map(page, range(2, pages + 1)))
    if any(count != total for count, _ in rest):
        raise ValueError('market universe changed during pagination')
    rows = first + [row for _, batch in rest for row in batch]
    unique = {(r.get('f13'), r.get('f12')) for r in rows}
    if len(rows) != total or len(unique) != total:
        raise ValueError('incomplete or repeated market pages')
    return rows, total


def breadth_from_rows(market, rows, total, expected, now):
    zone = ZoneInfo('America/New_York' if market == 'US' else 'Asia/Shanghai')
    counts = dict(advancing=0, declining=0, unchanged=0)
    excluded, exchanges, observed = {}, {}, []
    for row in rows:
        pct, price, stamp = finite(row.get('f3')), finite(row.get('f2')), finite(row.get('f124'))
        reason = None
        if pct is None or price is None or price <= 0:
            reason = 'invalid_or_suspended_quote'
        elif not stamp:
            reason = 'missing_quote_time'
        else:
            time = dt.datetime.fromtimestamp(stamp, zone)
            if time.date().isoformat() != expected or time > now.astimezone(zone) + dt.timedelta(minutes=5):
                reason = 'unverified_session_date'
        if reason:
            excluded[reason] = excluded.get(reason, 0) + 1
            continue
        key = 'advancing' if pct > 0 else 'declining' if pct < 0 else 'unchanged'
        counts[key] += 1
        venue = {'0': 'SZSE/BSE vendor segment', '1': 'SSE', '128': 'HKEX',
                 '105': 'NASDAQ', '106': 'NYSE', '107': 'NYSE American'}.get(str(row.get('f13')), str(row.get('f13')))
        if market == 'CN' and str(row.get('f13')) == '0':
            venue = 'BSE' if str(row.get('f12', '')).startswith(('4', '8', '92')) else 'SZSE'
        part = exchanges.setdefault(venue, dict(advancing=0, declining=0, unchanged=0))
        part[key] += 1
        observed.append(time.isoformat())
    covered = sum(counts.values())
    return qualify_breadth(market, dict(**{k: None for k in counts},
                observed_counts=counts, enumerated_count=total, covered_count=covered,
                coverage_ratio=round(covered / total, 4) if total else None,
                exchange_counts=exchanges, excluded_reason_counts=excluded,
                coverage_scope='complete Eastmoney market-screen enumeration; only valid same-session quotes counted; not Trend candidates or exchange-certified all-market totals',
                provider_filter=FILTERS[market], includes='provider-listed securities; may include funds, ADRs or preferred issues according to market screen',
                source='Eastmoney full market quote screen', unit='securities', session_date=expected,
                collected_at=now.isoformat(), data_asof=max(observed, default=None),
                quality='missing', stale=True, missing_reason='unqualified coverage'))


def qualify_breadth(market, record):
    """Publish exact observed counts, never extrapolate missing or inactive issues."""
    counts = record.get('observed_counts')
    total, covered = record.get('enumerated_count'), record.get('covered_count')
    if not isinstance(counts, dict) or not total or covered is None:
        return record
    required = min(total, max({'CN': 3000, 'HK': 1000, 'US': 3000}[market], math.ceil(total * .5)))
    valid = covered >= required
    return dict(record, **{k: counts.get(k) if valid else None for k in ('advancing', 'declining', 'unchanged')},
                quality=('ok' if covered == total else 'partial') if valid else 'missing', stale=not valid,
                is_all_market_total=False, count_basis='exact valid same-session issues; no extrapolation',
                missing_reason=(None if covered == total else 'inactive/undated/invalid issues excluded; counts cover only the explicitly reported observed universe') if valid
                else 'too few same-session quotes to publish meaningful market breadth')


def qualify_index_record(record):
    record = dict(record)
    if record.get('market') == 'US':
        # Index points * vendor volume is not traded USD. This field has no
        # verified cash-notional definition for Tencent US index symbols.
        record.update(turnover=None, turnover_currency=None,
                      turnover_missing_reason='US index quote amount/unit not verified; never treat index-points times volume as USD turnover')
    return record


def fetch_indices(market, expected, now):
    from modules.ingestion.tencent_quote import fetch_quotes
    quotes = fetch_quotes([code for code, _, _ in INDEXES[market]], market)
    result = []
    for code, name, exchange in INDEXES[market]:
        quote = quotes.get(code) or {}
        stamp = str(quote.get('timestamp') or '')
        valid = stamp[:10] == expected and bool(quote.get('price'))
        try:
            observed = dt.datetime.fromisoformat(stamp).replace(tzinfo=ZoneInfo(quote.get('timestamp_timezone') or 'Asia/Shanghai'))
            if market == 'US':
                from modules.market_sessions import us_close_at
                closed = observed >= us_close_at(dt.date.fromisoformat(expected))
            else:
                closed = observed.hour >= (15 if market == 'CN' else 16)
            valid = valid and closed and observed <= now.astimezone(observed.tzinfo)
        except ValueError:
            valid = False
        result.append(dict(symbol=code, name=name, market=market, exchange=exchange,
                           instrument_type='INDEX', session_date=expected, observed_session_date=stamp[:10] or None,
                           close=quote.get('price') if valid else None,
                           change_pct=quote.get('pct_chg') if valid else None,
                           volume=quote.get('volume') if valid and (quote.get('volume') or 0) > 0 else None, volume_unit='shares in provider index scope',
                           turnover=quote.get('amount') if valid and (quote.get('amount') or 0) > 0 else None,
                           turnover_currency={'CN': 'CNY', 'HK': 'HKD', 'US': 'USD'}[market],
                           source='Tencent index quote', data_asof=stamp or None,
                           timestamp_timezone=quote.get('timestamp_timezone'), collected_at=now.isoformat(),
                           coverage_scope='named index and provider-reported trading aggregate, not all-market turnover',
                           price_adjustment='index-provider published level; not a stock adjusted price',
                           quality='ok' if valid else 'missing', stale=not valid,
                           missing_reason=None if valid else 'index quote absent or does not match expected completed session'))
    return [qualify_index_record(row) for row in result]
