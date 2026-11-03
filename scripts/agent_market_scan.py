"""Agent-only A/H post-close quote scan; never writes Trend, TG or UI state."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from generate_agent_snapshot import SHANGHAI, atomically_write, number, readonly_db
from agent_market_calendar import SOURCES, closed_reason, last_session


def parse_observed_at(value):
    try:
        parsed = datetime.fromisoformat(str(value or '').strip().replace('/', '-'))
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    except (TypeError, ValueError):
        return None


def candidate_symbols(root, market, day):
    path = root / 'data/ledger.db'
    if not path.exists():
        return {}
    with readonly_db(path) as db:
        rows = db.execute('SELECT symbol,name FROM trendseedpool WHERE market=? AND date>=?',
                          (market, (day - timedelta(days=120)).isoformat())).fetchall()
        bars = db.execute('SELECT symbol,name FROM trenddailybar WHERE market=? AND date=? AND close>0',
                          (market, day.isoformat())).fetchall()
        result = {str(row['symbol']): str(row['name'] or '') for row in rows}
        result.update({str(row['symbol']): str(row['name'] or '') for row in bars})
    return result


def validate_quote(market, day, quote, checked_at):
    if not quote:
        return None, 'no_quote'
    price, change = number(quote.get('price')), number(quote.get('pct_chg'))
    if price is None or price <= 0:
        return None, 'invalid_price'
    if change is None:
        return None, 'missing_change'
    observed = parse_observed_at(quote.get('timestamp'))
    if not observed:
        return None, 'missing_observation_time'
    if observed.date() != day or observed > checked_at + timedelta(minutes=5):
        return None, 'wrong_session_date'
    close_hour = 15 if market == 'CN' else 16
    if observed.hour < close_hour:
        return None, 'pre_close_quote'
    return (price, change, observed), None


def scan(root, output, now=None, quote_getter=None):
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('scan time must be timezone-aware')
    now = now.astimezone(SHANGHAI)
    if now.hour < 17:
        raise ValueError('A/H post-close scan cannot run before 17:00 Beijing time')
    live_getter = quote_getter is None
    if live_getter:
        sys.path.insert(0, str(root))
        from modules.monitor.trend_service import TrendService
        quote_getter = TrendService._pick_trusted_quote
    results = {}
    for market in ('CN', 'HK'):
        candidates = candidate_symbols(root, market, now.date())
        closure = closed_reason(market, now.date())
        if closure:
            results[market] = {
                'session_date': last_session(market, now.date()).isoformat(),
                'market_status': 'closed', 'closure_reason': closure,
                'calendar_source': SOURCES.get(market),
                'source_universe_count': len(candidates),
                'available_universe_count': 0,
                'coverage_scope': 'market_closed_no_current_session',
                'excluded_reason_counts': {'market_closed': len(candidates)},
                'items': [],
            }
            continue
        rows, excluded = [], Counter()
        def fetch(pair):
            symbol, name = pair
            try:
                return symbol, name, quote_getter(symbol, market), None
            except Exception as exc:
                return symbol, name, None, type(exc).__name__
        with ThreadPoolExecutor(max_workers=12) as pool:
            for symbol, name, quote, error in pool.map(fetch, candidates.items()):
                checked = datetime.now(SHANGHAI) if live_getter else now
                parsed, reason = validate_quote(market, now.date(), quote, checked)
                if error or reason:
                    excluded['provider_error' if error else reason] += 1
                    continue
                price, change, observed = parsed
                rows.append({'market': market, 'symbol': symbol,
                             'name': str(quote.get('name') or name),
                             'price': price, 'change_pct': round(change, 3),
                             'observed_at': observed.isoformat(timespec='seconds'),
                             'source_checked_at': checked.isoformat(timespec='seconds'),
                             'provider': str(quote.get('provider') or 'unknown'),
                             'price_type': 'eod_vendor_close'})
        results[market] = {'session_date': now.date().isoformat(),
                           'market_status': 'open',
                           'source_universe_count': len(candidates),
                           'available_universe_count': len(rows),
                           'coverage_scope': 'rolling_120d_TrendSeedPool_union_current_archived_bars',
                           'excluded_reason_counts': dict(excluded), 'items': rows}
    payload = {'schema_version': '1.0', 'scan_id': f'agent-market-{now.date().isoformat()}',
               'generated_at': now.isoformat(timespec='seconds'),
               'source': 'Trade_db trusted quote provider; independent Agent scan',
               'markets': results}
    atomically_write(output / f'{now.date().isoformat()}.json',
                     (json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode())
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path, default=None)
    args = parser.parse_args()
    payload = scan(args.root, args.output or args.root / 'data/agent_market_scan')
    print({market: (row['source_universe_count'], row['available_universe_count'])
           for market, row in payload['markets'].items()})


if __name__ == '__main__':
    main()
