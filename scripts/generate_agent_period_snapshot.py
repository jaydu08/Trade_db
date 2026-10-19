"""Frozen 7/30-calendar-day Agent export from existing Trend archives (read-only)."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from generate_agent_snapshot import (NEW_YORK, SHANGHAI, classify_stock_asset, verified_hk_product,
                                     epoch_iso, immutably_create, number, read_json,
                                     readonly_db, utc_naive_iso, macro_freshness)

MAX_BYTES = 200_000


def archive_session(market, archive_day):
    """US archive dates are UTC ingestion dates, not NY trade dates.

    The existing US EOD writer runs just after 00:00 UTC and archives the
    preceding New York session. `updated_at` is a write timestamp, not a
    vendor-confirmed exchange timestamp, so it must not define the session.
    """
    day = date.fromisoformat(str(archive_day))
    return day - timedelta(days=1) if market == 'US' else day


def amount_measure(value, source, *, verified_zero=False):
    value = number(value)
    if value is None or value < 0:
        return None, 'missing', 'amount not supplied by archive'
    if value == 0 and not verified_zero:
        return None, 'missing', f'{source}: zero is an unverified default, not confirmed no trading'
    return value, 'verified_zero' if value == 0 else 'observed', None


def baseline_quality(market_sessions, baseline, target):
    """An earlier close is valid only if no observed market session was skipped."""
    missed = [day for day in market_sessions if baseline < day <= target]
    return ('aligned' if not missed else 'missing_asset_bars', len(missed))


def excluded_summary(row):
    fields = ('market', 'symbol', 'asset_type',
              'product_metadata', 'return_pct', 'archive_date', 'asset_session_date',
              'market_reference_session', 'session_gap', 'eligibility_status',
              'exclusion_reason', 'actual_baseline_date', 'baseline_quality',
              'window_start_target', 'source', 'data_asof')
    return {key: row[key] for key in fields if key in row}


def _baseline(points, target, max_lag):
    return next(((day, row) for day, row in reversed(points) if day <= target and
                 (target - day).days <= max_lag), None)


def _return(points, days):
    """Mirror TrendCalculator's calendar-day anchor and corporate-action sanity gate."""
    if not points:
        return None, None
    end, latest = points[-1]
    base = _baseline(points[:-1], end - timedelta(days=days),
                     max(4, min(14, round(days * .1) + 3)))
    if not base:
        return None, None
    price, old = number(latest['close']), number(base[1]['close'])
    window = [(day, number(row['close'])) for day, row in points if base[0] <= day <= end]
    suspicious = any(current / previous >= 2.5 or current / previous <= .4
                     for (_, previous), (_, current) in zip(window, window[1:]) if previous and current)
    if not price or not old or suspicious:
        return None, base[0].isoformat()
    return round((price / old - 1) * 100, 3), base[0].isoformat()


def _stock_catalog(root):
    result = defaultdict(lambda: {'asset_type': None, 'listing_status': None,
                                  'industries': [], 'concepts': []})
    path = root / 'data/meta.db'
    if not path.exists():
        return result
    with closing(readonly_db(path)) as db:
        for row in db.execute('SELECT symbol,asset_type,listing_status FROM asset'):
            result[row['symbol']]['asset_type'] = row['asset_type']
            result[row['symbol']]['listing_status'] = row['listing_status']
        for row in db.execute('SELECT l.symbol,i.name FROM asset_industry_link l JOIN industry i ON i.code=l.industry_code'):
            result[row['symbol']]['industries'].append(row['name'])
        for row in db.execute('SELECT l.symbol,c.name FROM asset_concept_link l JOIN concept c ON c.code=l.concept_code'):
            result[row['symbol']]['concepts'].append(row['name'])
    return result


def _stock_trend_scores(days):
    # Same service used by /api/trend. Its rising-only selection is supplemented
    # below with negative returns from the same TrendDailyBar history.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from modules.monitor.trend_service import TrendCalculator
    result = TrendCalculator.calculate_trend(days=days, topn_override=20)
    return {(market, str(item.get('symbol'))): number(item.get('trend_score'))
            for market, items in result.items() if isinstance(items, list)
            for item in items if isinstance(item, dict)}


def _rank(rows, limit):
    for direction, selected in (('up', sorted((r for r in rows if r['return_pct'] > 0),
                                             key=lambda r: r['return_pct'], reverse=True)[:limit]),
                                ('down', sorted((r for r in rows if r['return_pct'] < 0),
                                               key=lambda r: r['return_pct'])[:limit])):
        for rank, row in enumerate(selected, 1):
            row['direction'], row['rank'] = direction, rank
            yield row


def stock_period(root, days, now, scores):
    path = root / 'data/ledger.db'
    if not path.exists():
        return [], [{'domain': 'stocks_and_commodities', 'quality': 'missing', 'missing_reason': 'ledger.db unavailable'}]
    catalog = _stock_catalog(root)
    with closing(readonly_db(path)) as db:
        raw = db.execute('SELECT date,market,symbol,name,close,amount,turnover_rate,source,updated_at '
                         'FROM trenddailybar WHERE date>=? AND close>0',
                         ((now.date() - timedelta(days=days + 25)).isoformat(),)).fetchall()
        seen = defaultdict(dict)
        preference = {'heatmap': 4, 'daily_hot': 3, 'commodity': 3, 'trend_pool_refresh_eod': 1}
        local_now = now.astimezone(SHANGHAI)
        today = local_now.date().isoformat()
        incomplete_markets = set()
        for row in raw:
            written = utc_naive_iso(row['updated_at'])
            if written and datetime.fromisoformat(written) > now.astimezone(timezone.utc):
                # A historical replay must not see rows backfilled after its cutoff.
                continue
            # A/H and commodity bars from a morning trading session are not a
            # completed close. The month-end 10:00 report excludes them.
            if row['market'] in ('CN', 'HK', 'CF') and row['date'] == today and local_now.hour < 17:
                incomplete_markets.add(row['market'])
                continue
            if row['date'] > today:
                continue
            key = (row['market'], str(row['symbol']))
            prior = seen[key].get(row['date'])
            if prior is None or preference.get(row['source'], 0) > preference.get(prior['source'], 0):
                seen[key][row['date']] = row
        latest_market = {}
        for (market, _), by_day in seen.items():
            last = max(by_day)
            latest_market[market] = max(last, latest_market.get(market, last))
        persistence = defaultdict(set)
        for row in db.execute('SELECT date,market,symbol FROM trendseedpool WHERE date>=?',
                              ((now.date() - timedelta(days=days)).isoformat(),)):
            persistence[(row['market'], str(row['symbol']))].add(row['date'])
    market_sessions = defaultdict(set)
    for (market, _), by_day in seen.items():
        market_sessions[market].update(archive_session(market, day) for day in by_day)
    reference = {market: max(sessions) for market, sessions in market_sessions.items() if sessions}
    groups, health, excluded = defaultdict(list), [], defaultdict(list)
    for market in ('CN', 'HK', 'US', 'CF'):
        last = reference.get(market)
        health.append({'domain': f'stocks_{market}' if market != 'CF' else 'commodities',
                       'data_asof': last.isoformat() if last else None,
                       'market_reference_session': last.isoformat() if last and market != 'CF' else None,
                       'archive_reference_date': last.isoformat() if last and market == 'CF' else None,
                       'calendar_basis': ('CF archive date; exchange session not verified' if market == 'CF' else
                                          'observed market-wide TrendDailyBar sessions; not an official exchange calendar'),
                       'coverage_pool': 'TrendDailyBar candidate archive; not full market',
                       'quality': 'partial' if last else 'missing',
                       'stale': not last or (now.date() - last).days > 5,
                       'missing_reason': None if last else 'no historical bars'})
    for (market, symbol), by_day in seen.items():
        if market not in ('CN', 'HK', 'US', 'CF'):
            continue
        points = sorted((date.fromisoformat(day), row) for day, row in by_day.items())
        end, latest = points[-1]
        market_date = reference.get(market)
        asset_session = archive_session(market, end)
        if not market_date:
            continue
        session_gap = sum(asset_session < session <= market_date for session in market_sessions[market])
        change, baseline = _return(points, days)
        if change is None:
            if session_gap:
                excluded[market].append({'market': market, 'symbol': symbol, 'name': latest['name'],
                    'asset_session_date': asset_session.isoformat(),
                    'market_reference_session': market_date.isoformat(), 'archive_date': end.isoformat(),
                    'session_gap': session_gap, 'return_pct': None,
                    'eligibility_status': ('suspended' if str(catalog[symbol].get('listing_status') or '').upper()
                                           in ('SUSPENDED', 'HALTED') else 'lagged'),
                    'is_latest_eligible': False,
                    'exclusion_reason': 'no current close and no comparable historical baseline',
                    'source': f'Trade_db TrendDailyBar:{latest["source"]}',
                    'data_asof': utc_naive_iso(latest['updated_at'])})
            continue
        meta = catalog[symbol]
        baseline_session = archive_session(market, baseline)
        target_session = market_date - timedelta(days=days)
        base_status, missing_base_sessions = baseline_quality(
            market_sessions[market], baseline_session, target_session)
        listing_status = str(meta.get('listing_status') or '').upper()
        verified_product = verified_hk_product(symbol)
        if verified_product and verified_product['counter_status'].startswith('terminated_'):
            eligibility = 'suspended'
            exclusion_reason = ('HKEX terminated this trading counter on 2026-09-18; '
                                'historical product identity retained, current ranking prohibited')
        elif session_gap:
            eligibility = 'suspended' if listing_status in ('SUSPENDED', 'HALTED') else 'lagged'
            exclusion_reason = (f'last valid session {asset_session} is {session_gap} observed trading sessions '
                                f'behind market reference {market_date}; archive has no verified current close')
        elif base_status != 'aligned':
            eligibility = 'insufficient_history'
            exclusion_reason = (f'baseline {baseline_session} skips {missing_base_sessions} observed market '
                                f'session(s) before target {target_session}')
        else:
            eligibility, exclusion_reason = 'eligible', None
        asset_type, asset_source = classify_stock_asset(symbol, latest['name'], meta['asset_type'])
        if market == 'CF':
            asset_type, asset_source = 'COMMODITY_FUTURE', 'Trade_db CF archive'
        amount_latest, amount_status, amount_reason = amount_measure(latest['amount'], latest['source'])
        amounts = [(day, number(row['amount'])) for day, row in points
                   if number(row['amount']) and row['source'] == latest['source']]
        early_bars = [(d, v) for d, v in amounts if d <= end - timedelta(days=days // 2)]
        late_bars = [(d, v) for d, v in amounts if d > end - timedelta(days=days // 2)]
        early, late = [v for _, v in early_bars], [v for _, v in late_bars]
        amount_change = round((sum(late[-3:]) / len(late[-3:]) /
                               (sum(early[-3:]) / len(early[-3:])) - 1) * 100, 2) if amount_latest is not None and len(early) >= 2 and len(late) >= 2 and market != 'CF' else None
        half = days // 2
        first_half, _ = _return(points, half)
        if first_half is not None and change is not None:
            second_half = round(((1 + change / 100) / (1 + first_half / 100) - 1) * 100, 3)
        else:
            second_half = None
        score = scores.get((market, symbol)) if market not in incomplete_markets else None
        row = {'market': market, 'symbol': symbol, 'name': latest['name'],
               'asset_type': asset_type, 'asset_type_source': asset_source,
               'product_metadata': verified_product,
               'return_pct': change, 'trend_score': score,
               'trend_score_missing_reason': (None if score is not None else
                   'current-day incomplete bars excluded; live Trend score not comparable' if market in incomplete_markets else
                   'not selected by rising-only Trend algorithm'),
               'price': number(latest['close']), 'price_adjustment': 'unknown_from_existing_archive',
               'volume_change_pct': None, 'amount_change_pct': amount_change,
               'amount_change_missing_reason': None if amount_change is not None else
                    'latest amount unverified or insufficient comparable historical amount bars',
               'amount_change_basis': ({'early_observed_days': len(early[-3:]), 'late_observed_days': len(late[-3:]),
                                        'early_dates': [d.isoformat() for d, _ in early_bars[-3:]],
                                        'late_dates': [d.isoformat() for d, _ in late_bars[-3:]],
                                        'source': latest['source'], 'method': 'mean of up to three same-source observed bars per half'}
                                       if amount_change is not None else None),
               'amount_latest': amount_latest, 'amount_status': amount_status,
               'amount_missing_reason': amount_reason, 'amount_source': latest['source'],
               'amount_currency': {'CN': 'CNY', 'HK': 'HKD', 'US': 'USD', 'CF': None}.get(market),
               'amount_unit': 'quote_currency' if market != 'CF' else None,
               'amount_observed_at': asset_session.isoformat() if amount_latest is not None else None,
               'amount_timestamp_basis': 'archive_session_date_only; DB updated_at is ingestion time',
               'relative_strength_pct': None,
               'relative_strength_basis': 'same-market candidate median',
               'window_start_target': target_session.isoformat(),
               'baseline_date': baseline, 'window_end': end.isoformat(),
               'actual_baseline_date': baseline_session.isoformat(),
               'baseline_gap_days': (target_session - baseline_session).days,
               'baseline_quality': base_status,
               'window_basis': (f'{days} calendar archive days; CF exchange session unverified' if market == 'CF' else
                                f'{days} calendar days ending at market reference session; earlier baseline only if no observed market session skipped'),
               'session_date': asset_session.isoformat() if market != 'CF' else None,
               'archive_date': end.isoformat(),
               'archive_reference_date': market_date.isoformat() if market == 'CF' else None,
               'market_reference_session': market_date.isoformat() if market != 'CF' else None,
               'asset_session_date': asset_session.isoformat() if market != 'CF' else None,
               'session_gap': session_gap if market != 'CF' else None,
               'archive_gap': session_gap if market == 'CF' else None,
               'eligibility_status': eligibility, 'is_latest_eligible': eligibility == 'eligible',
               'exclusion_reason': exclusion_reason,
               'data_asof': utc_naive_iso(latest['updated_at']),
               'source': f'Trade_db TrendDailyBar:{latest["source"]}',
               'coverage_pool': 'TrendDailyBar candidate archive; not full market',
               'industries': meta['industries'][:3], 'concepts': meta['concepts'][:5],
               'persistence': {'trend_seed_days': len(persistence[(market, symbol)]),
                               'observed_bar_days': len(points)},
               'phase_returns_pct': {'recent_half': first_half, 'earlier_half': second_half},
               'quality': 'partial', 'stale': (now.date() - asset_session).days > 5 or eligibility != 'eligible',
               'missing_reason': 'unadjusted/adjustment status unverified' }
        if eligibility == 'eligible':
            groups[market].append(row)
        else:
            excluded[market].append(excluded_summary(row))
    for item in health:
        market = item['domain'].replace('stocks_', '') if item['domain'].startswith('stocks_') else 'CF'
        item['excluded_count'] = len(excluded[market])
        limit = (25 if market in ('CN', 'US') else 12 if market == 'HK' else 3) if days == 7 else 10
        item['excluded_candidates'] = sorted(excluded[market], key=lambda r: abs(r['return_pct'] or 0), reverse=True)[:limit]
    output = []
    for market, rows in groups.items():
        ordered = sorted(r['return_pct'] for r in rows)
        median = ordered[len(ordered) // 2]
        phases = {}
        for phase in ('earlier_half', 'recent_half'):
            values = sorted(r['phase_returns_pct'][phase] for r in rows
                            if r['phase_returns_pct'][phase] is not None)
            phases[phase] = values[len(values) // 2] if values else None
        for row in rows:
            row['relative_strength_pct'] = round(row['return_pct'] - median, 3)
            early, recent = row['phase_returns_pct']['earlier_half'], row['phase_returns_pct']['recent_half']
            row['relative_strength_rotation_pct'] = (
                round((recent - phases['recent_half']) - (early - phases['earlier_half']), 3)
                if early is not None and recent is not None and all(v is not None for v in phases.values()) else None)
        output.extend(_rank(rows, 8 if market != 'CF' else 6))
    return output, health


def crypto_period(path, days, now):
    if not path.exists():
        return [], [{'domain': 'crypto', 'quality': 'missing', 'missing_reason': 'crypto_trend.sqlite unavailable'}]
    output, health = [], []
    with closing(readonly_db(path)) as db:
        for market in ('spot', 'futures', 'onchain'):
            hr = db.execute('SELECT updated,payload FROM health WHERE component=?', (market,)).fetchone()
            hp = json.loads(hr['payload']) if hr else {}
            selected = set(hp.get('selected_symbols') or []) if market != 'onchain' else None
            age = now.timestamp() - hr['updated'] if hr else None
            health.append({'domain': f'crypto_{market}', 'data_asof': epoch_iso(hr['updated']) if hr else None,
                           'quality': 'partial' if hr and age is not None and age >= -300 else 'missing',
                           'stale': age is None or age > 900 or age < -300,
                           'coverage_pool': 'discovered contracts' if market == 'onchain' else 'Binance 24h quote-volume Top 150',
                           'missing_reason': None if hr else 'health unavailable'})
            rows = db.execute('SELECT a.payload,s.payload AS security FROM assets a LEFT JOIN security s ON a.id=s.id WHERE a.market=?',
                              (market,)).fetchall()
            candidates = []
            for raw in rows:
                item = json.loads(raw['payload'])
                if selected is not None and item.get('symbol') not in selected:
                    continue
                updated = number(item.get('updated'))
                if not updated or abs(now.timestamp() - updated) > 900:
                    continue
                security = json.loads(raw['security']) if raw['security'] else {}
                status = security.get('status', 'unknown')
                if market == 'onchain':
                    if status == 'blocked' or (number(item.get('liquidity')) or 0) < 50_000 or (number(item.get('volume_24h')) or 0) < 50_000:
                        continue
                    if status == 'pass' and ((number(security.get('expires_at')) or 0) < now.timestamp() or security.get('pair') != item.get('pair')):
                        status = 'unknown'
                source = 'dex:' + str(item.get('pair') or '') if market == 'onchain' else 'binance'
                target = updated - days * 86400
                base = db.execute('SELECT ts,price FROM bars WHERE id=? AND source=? AND ts<=? ORDER BY ts DESC LIMIT 1',
                                  (item['id'], source, target)).fetchone()
                if not base or target - base['ts'] > 3700 or not number(base['price']) or not number(item.get('price')):
                    continue
                change = round((item['price'] / base['price'] - 1) * 100, 3)
                if market == 'onchain' and (change > 1000 or change < -90):
                    continue
                candidates.append({'market': market, 'exchange': 'Binance' if market != 'onchain' else None,
                                   'asset_type': {'spot': 'SPOT', 'futures': 'PERPETUAL', 'onchain': 'ONCHAIN_TOKEN'}[market],
                                   'symbol': item.get('symbol'), 'chain': item.get('chain') if market == 'onchain' else None,
                                   'contract_address': item.get('address') if market == 'onchain' else None,
                                   'return_pct': change, 'trend_score': None,
                                   'trend_score_missing_reason': 'crypto Trend API does not expose a comparable trend score',
                                   'price': number(item.get('price')), 'price_adjustment': 'not_applicable',
                                   'volume_24h': number(item.get('volume_24h')),
                                   'volume_change_pct': None,
                                   'volume_change_missing_reason': 'historical comparable turnover series is not archived',
                                   'liquidity': number(item.get('liquidity')) if market == 'onchain' else None,
                                   'security_status': status if market == 'onchain' else None,
                                   'relative_strength_pct': None, 'relative_strength_basis': 'same-market candidate median',
                                   'window_start_target': epoch_iso(target), 'baseline_date': epoch_iso(base['ts']),
                                   'window_end': epoch_iso(updated), 'session_date': None,
                                   'data_asof': epoch_iso(updated), 'source': item.get('price_source') or source,
                                   'coverage_pool': 'discovered contracts' if market == 'onchain' else 'Binance 24h quote-volume Top 150',
                                   'quality': 'partial' if status in ('unknown', 'review') and market == 'onchain' else 'ok',
                                   'stale': False, 'missing_reason': 'security unverified' if status in ('unknown', 'review') and market == 'onchain' else None})
            if candidates:
                ordered = sorted(r['return_pct'] for r in candidates)
                median = ordered[len(ordered) // 2]
                for row in candidates:
                    row['relative_strength_pct'] = round(row['return_pct'] - median, 3)
            output.extend(_rank(candidates, 5))
    return output, health


def macro_period(root, days, now):
    payload = read_json(root / 'data/macro_dashboard_snapshot.json', {})
    result = []
    for item in payload.get('global_assets', []):
        change = number((item.get('changes') or {}).get(f'{days}d'))
        if change is None:
            continue
        result.append({'market': 'GLOBAL', 'symbol': item.get('id'), 'name': item.get('name'),
                       'asset_type': 'INDEX' if item.get('kind') == '指数' else 'ETF',
                       'return_pct': change, 'trend_score': None, 'relative_strength_pct': None,
                       'relative_strength_basis': 'global index/ETF candidate median',
                       'price': number(item.get('value')), 'price_adjustment': 'unknown_from_source',
                       'window_start_target': (date.fromisoformat(str(item['asof'])[:10]) - timedelta(days=days)).isoformat(),
                       'baseline_date': None, 'window_end': str(item['asof'])[:10],
                       'session_date': str(item['asof'])[:10], 'data_asof': item.get('asof'),
                       'source': item.get('source'), 'coverage_pool': 'macro dashboard global-assets watchlist',
                       'quality': 'partial', 'stale': (now.date() - date.fromisoformat(str(item['asof'])[:10])).days > 7,
                       'missing_reason': 'source cache omits exact baseline date and adjustment status'})
    if result:
        ordered = sorted(r['return_pct'] for r in result)
        median = ordered[len(ordered) // 2]
        for row in result:
            row['relative_strength_pct'] = round(row['return_pct'] - median, 3)
    rates = []
    for item in payload.get('macro', []):
        if item.get('id') not in ('DGS2', 'DGS10', 'DFII10', 'DFEDTARU', 'DTWEXBGS'):
            continue
        status, stale, reason, expected = macro_freshness(
            item.get('id'), item.get('asof'), payload.get('macro_checked_at'), now)
        rates.append({'symbol': item.get('id'), 'name': item.get('name'),
                      'value': number(item.get('value')), 'unit': item.get('unit'),
                      'period_return_pct': None, 'data_asof': item.get('asof'),
                      'source_checked_at': epoch_iso(payload.get('macro_checked_at')),
                      'expected_observation_date': expected, 'source': item.get('source'),
                      'quality': 'stale' if stale else 'partial', 'stale': stale,
                      'stale_reason': reason if stale else None,
                      'missing_reason': 'no 7/30-day historical series in current macro cache'})
    return list(_rank(result, 8)), rates


def build(root, crypto_db, days, now, scores=None):
    if days not in (7, 30):
        raise ValueError('period must be 7 or 30 calendar days')
    scores = _stock_trend_scores(days) if scores is None else scores
    stocks, stock_health = stock_period(root, days, now, scores)
    crypto, crypto_health = crypto_period(crypto_db, days, now)
    global_assets, rates = macro_period(root, days, now)
    if not stocks or not crypto:
        raise ValueError('stock or crypto history missing; refusing pseudo-complete period snapshot')
    all_health = stock_health + crypto_health
    macro_cache = read_json(root / 'data/macro_dashboard_snapshot.json', {})
    macro_checked = number(macro_cache.get('refreshed_at'))
    all_health.append({'domain': 'macro', 'data_asof': epoch_iso(macro_checked),
                       'coverage_pool': 'global-assets watchlist and latest FRED rates',
                       'quality': 'partial' if global_assets or rates else 'missing',
                       'stale': not macro_checked or now.timestamp() - macro_checked > 86400,
                       'missing_reason': None if global_assets or rates else 'macro cache unavailable'})
    domain_items = {'stocks_CN': [r for r in stocks if r['market'] == 'CN'],
                    'stocks_HK': [r for r in stocks if r['market'] == 'HK'],
                    'stocks_US': [r for r in stocks if r['market'] == 'US'],
                    'commodities': [r for r in stocks if r['market'] == 'CF'],
                    'crypto_spot': [r for r in crypto if r['market'] == 'spot'],
                    'crypto_futures': [r for r in crypto if r['market'] == 'futures'],
                    'crypto_onchain': [r for r in crypto if r['market'] == 'onchain'],
                    'macro': global_assets + rates}
    stale_ids = []
    partial_count = 0
    for health in all_health:
        items = domain_items.get(health['domain'], [])
        ids = [str(r.get('symbol')) for r in items if r.get('stale')]
        stale_ids.extend(f"{health['domain']}:{symbol}" for symbol in ids)
        partial_count += sum(r.get('quality') == 'partial' for r in items)
        health['collection_freshness'] = 'stale' if health.get('stale') else 'current'
        health['observation_freshness'] = 'stale' if ids else 'current' if items else 'unavailable'
        health['stale_item_count'] = len(ids)
        health['stale_item_ids'] = ids
        health['partial_item_count'] = sum(r.get('quality') == 'partial' for r in items)
        health['critical_data_available'] = bool(items)
        if ids:
            health['stale'] = True
            health['quality'] = 'partial'
    sessions = {market: max((r['session_date'] for r in stocks if r['market'] == market and r['session_date']), default=None)
                for market in ('CN', 'HK', 'US', 'CF')}
    month_end_morning = (days == 30 and (now + timedelta(days=1)).month != now.month
                         and now.astimezone(SHANGHAI).hour < 17)
    return {'schema_version': '1.2', 'snapshot_id': f'{days}d-{now.date().isoformat()}-{int(now.timestamp())}',
            'generated_at': now.isoformat(timespec='seconds'), 'timezone': 'Asia/Shanghai',
            'session_date': sessions,
            'data_asof': {h['domain']: h['data_asof'] for h in all_health},
            'period': 'weekly' if days == 7 else 'monthly',
            'run_kind': 'scheduled' if (now.weekday() == 6 if days == 7 else (now + timedelta(days=1)).month != now.month) else 'initial_preview',
            'publication_context': {
                'report_cutoff': now.isoformat(timespec='seconds'),
                'full_calendar_month_complete': False if month_end_morning else None,
                'month_end_morning_note': ('Month-end current day is not complete: CN/HK/CF exclude unfinished current-day bars; '
                                           'stocks use each market latest completed archived session, crypto uses actual sample time. '
                                           'This is a rolling 30-calendar-day report, not a full finished calendar month.')
                    if month_end_morning else None},
            'window_definition': {'stocks_and_commodities': f'{days} calendar days between archived closes; earlier baseline excluded if an observed market session was skipped; CF exchange session dates unverified',
                                  'crypto': f'{days}*24 rolling hours from each actual sample, baseline <=1h old',
                                  'global_assets': f'{days} calendar days in existing macro cache; exact baseline date unavailable'},
            'coverage': {'stocks': 'TrendDailyBar candidate archive, not full market',
                         'cex': 'Binance spot and perpetual each 24h quote-volume Top 150',
                         'onchain': 'discovered contracts with >=$50k liquidity and 24h volume; blocked excluded',
                         'macro': 'CF candidate bars and global-assets watchlist'},
            'markets': {'equities_and_commodities': stocks, 'crypto': crypto, 'global_assets': global_assets,
                        'macro_rates': rates},
            'source_health': all_health,
            'quality': 'partial', 'stale': bool(stale_ids) or any(x.get('stale') for x in all_health),
            'stale_item_count': len(stale_ids), 'stale_item_ids': stale_ids,
            'stale_domains': [h['domain'] for h in all_health if h.get('stale')],
            'partial_item_count': partial_count,
            'critical_data_available': bool(stocks and crypto),
            'missing_reason': 'not a full-market universe; adjustment status, some volume and macro-history fields unavailable',
            'limitations': ['Trend score is existing rising-only /api/trend score; down rows retain null score',
                            '30d rankings use full 30-calendar-day bar returns, never month-end single-day Top',
                            'ONCHAIN_TOKEN security pass is not a guarantee of sellability']}


def generate(root, crypto_db, output, days, now=None, scores=None):
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('generation time must be timezone-aware')
    subdir = 'weekly' if days == 7 else 'monthly' if days == 30 else None
    if not subdir:
        raise ValueError('days must be 7 or 30')
    archive = output / subdir / f'{now.date().isoformat()}.json'
    if archive.exists():
        data = archive.read_bytes()
        snapshot = json.loads(data)
    else:
        snapshot = build(root, crypto_db, days, now, scores)
        data = (json.dumps(snapshot, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()
        if len(data) > MAX_BYTES:
            raise ValueError(f'period snapshot exceeds {MAX_BYTES} bytes: {len(data)}')
        immutably_create(archive, data)
        data = archive.read_bytes()
    return json.loads(data), len(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--crypto-db', type=Path, default=Path('/root/alpha_news/data/crypto_trend.sqlite'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--days', type=int, choices=(7, 30), required=True)
    args = parser.parse_args()
    snapshot, size = generate(args.root, args.crypto_db, args.output, args.days)
    print(snapshot['snapshot_id'], size)


if __name__ == '__main__':
    main()
