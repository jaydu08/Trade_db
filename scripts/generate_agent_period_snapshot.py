"""Frozen 7/30-calendar-day Agent export from existing Trend archives (read-only)."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path

from generate_agent_snapshot import (NEW_YORK, SHANGHAI, classify_stock_asset,
                                     epoch_iso, immutably_create, number, read_json,
                                     readonly_db, utc_naive_iso)

MAX_BYTES = 200_000


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
    result = defaultdict(lambda: {'asset_type': None, 'industries': [], 'concepts': []})
    path = root / 'data/meta.db'
    if not path.exists():
        return result
    with closing(readonly_db(path)) as db:
        for row in db.execute('SELECT symbol,asset_type FROM asset'):
            result[row['symbol']]['asset_type'] = row['asset_type']
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
        for row in raw:
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
    groups, health = defaultdict(list), []
    for market in ('CN', 'HK', 'US', 'CF'):
        last = latest_market.get(market)
        health.append({'domain': f'stocks_{market}' if market != 'CF' else 'commodities',
                       'data_asof': last, 'coverage_pool': 'TrendDailyBar candidate archive; not full market',
                       'quality': 'partial' if last else 'missing',
                       'stale': not last or (now.date() - date.fromisoformat(last)).days > 5,
                       'missing_reason': None if last else 'no historical bars'})
    for (market, symbol), by_day in seen.items():
        if market not in ('CN', 'HK', 'US', 'CF'):
            continue
        points = sorted((date.fromisoformat(day), row) for day, row in by_day.items())
        end, latest = points[-1]
        market_date = latest_market.get(market)
        if not market_date or (date.fromisoformat(market_date) - end).days > 4:
            continue
        change, baseline = _return(points, days)
        if change is None:
            continue
        meta = catalog[symbol]
        asset_type, asset_source = classify_stock_asset(symbol, latest['name'], meta['asset_type'])
        if market == 'CF':
            asset_type, asset_source = 'COMMODITY_FUTURE', 'Trade_db CF archive'
        amounts = [(day, number(row['amount'])) for day, row in points if number(row['amount'])]
        early = [v for day, v in amounts if day <= end - timedelta(days=days // 2)]
        late = [v for day, v in amounts if day > end - timedelta(days=days // 2)]
        amount_change = round((sum(late[-3:]) / len(late[-3:]) /
                               (sum(early[-3:]) / len(early[-3:])) - 1) * 100, 2) if early and late else None
        half = days // 2
        first_half, _ = _return(points, half)
        if first_half is not None and change is not None:
            second_half = round(((1 + change / 100) / (1 + first_half / 100) - 1) * 100, 3)
        else:
            second_half = None
        row = {'market': market, 'symbol': symbol, 'name': latest['name'],
               'asset_type': asset_type, 'asset_type_source': asset_source,
               'return_pct': change, 'trend_score': scores.get((market, symbol)),
               'trend_score_missing_reason': None if (market, symbol) in scores else 'not selected by rising-only Trend algorithm',
               'price': number(latest['close']), 'price_adjustment': 'unknown_from_existing_archive',
               'volume_change_pct': None, 'amount_change_pct': amount_change,
               'amount_change_missing_reason': None if amount_change is not None else 'insufficient comparable amount bars',
               'amount_latest': number(latest['amount']), 'relative_strength_pct': None,
               'relative_strength_basis': 'same-market candidate median',
               'window_start_target': (end - timedelta(days=days)).isoformat(),
               'baseline_date': baseline, 'window_end': end.isoformat(),
               'session_date': end.isoformat() if market != 'US' else
                    datetime.fromisoformat(utc_naive_iso(latest['updated_at'])).astimezone(NEW_YORK).date().isoformat()
                    if latest['updated_at'] else end.isoformat(),
               'data_asof': utc_naive_iso(latest['updated_at']),
               'source': f'Trade_db TrendDailyBar:{latest["source"]}',
               'coverage_pool': 'TrendDailyBar candidate archive; not full market',
               'industries': meta['industries'][:3], 'concepts': meta['concepts'][:5],
               'persistence': {'trend_seed_days': len(persistence[(market, symbol)]),
                               'observed_bar_days': len(points)},
               'phase_returns_pct': {'recent_half': first_half, 'earlier_half': second_half},
               'quality': 'partial', 'stale': (now.date() - end).days > 5,
               'missing_reason': 'unadjusted/adjustment status unverified' }
        groups[market].append(row)
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
                           'quality': 'partial' if hr else 'missing', 'stale': age is None or age > 900,
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
                if not updated or now.timestamp() - updated > 900:
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
    rates = [{'symbol': i.get('id'), 'name': i.get('name'), 'value': number(i.get('value')),
              'unit': i.get('unit'), 'period_return_pct': None, 'data_asof': i.get('asof'),
              'source': i.get('source'), 'quality': 'partial',
              'stale': not i.get('asof') or (now.date() - date.fromisoformat(str(i['asof'])[:10])).days > 7,
              'missing_reason': 'no 7/30-day historical series in current macro cache'}
             for i in payload.get('macro', []) if i.get('id') in ('DGS2', 'DGS10', 'DFII10', 'DFEDTARU', 'DTWEXBGS')]
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
    sessions = {market: max((r['session_date'] for r in stocks if r['market'] == market), default=None)
                for market in ('CN', 'HK', 'US', 'CF')}
    return {'schema_version': '1.1', 'snapshot_id': f'{days}d-{now.date().isoformat()}-{int(now.timestamp())}',
            'generated_at': now.isoformat(timespec='seconds'), 'timezone': 'Asia/Shanghai',
            'session_date': sessions,
            'data_asof': {h['domain']: h['data_asof'] for h in all_health},
            'period': 'weekly' if days == 7 else 'monthly',
            'run_kind': 'scheduled' if (now.weekday() == 6 if days == 7 else (now + timedelta(days=1)).month != now.month) else 'initial_preview',
            'window_definition': {'stocks_and_commodities': f'{days} calendar days between archived closes; non-trading dates use nearest earlier close within TrendCalculator tolerance',
                                  'crypto': f'{days}*24 rolling hours from each actual sample, baseline <=1h old',
                                  'global_assets': f'{days} calendar days in existing macro cache; exact baseline date unavailable'},
            'coverage': {'stocks': 'TrendDailyBar candidate archive, not full market',
                         'cex': 'Binance spot and perpetual each 24h quote-volume Top 150',
                         'onchain': 'discovered contracts with >=$50k liquidity and 24h volume; blocked excluded',
                         'macro': 'CF candidate bars and global-assets watchlist'},
            'markets': {'equities_and_commodities': stocks, 'crypto': crypto, 'global_assets': global_assets,
                        'macro_rates': rates},
            'source_health': all_health,
            'quality': 'partial', 'stale': any(x.get('stale') for x in all_health),
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
            raise ValueError('period snapshot exceeds 200 KB')
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
