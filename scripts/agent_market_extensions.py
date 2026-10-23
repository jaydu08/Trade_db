"""Public, read-only exports of classified Binance securities and global ETFs.

This is additive data access, not a new Trend or Telegram selection algorithm.
"""
import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def iso(value, now):
    return datetime.fromtimestamp(value, now.tzinfo).isoformat(timespec='seconds') if value else None


def build_extensions(root, crypto_db, now, days=None, full=False):
    """Full public universe separately; embedded snapshots use compact up/down samples."""
    hours = days * 24 if days else 12
    groups, issues = [], []
    if crypto_db.exists():
        with closing(sqlite3.connect(f'{crypto_db.resolve().as_uri()}?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            for market, group in [('spot', 'securities'), ('futures', 'securities'), ('futures', 'macro')]:
                health = db.execute('SELECT updated,payload FROM health WHERE component=?', (market,)).fetchone()
                hp = json.loads(health['payload']) if health else {}
                selected = set(hp.get('selected_symbols') or [])
                rows = []
                for record in db.execute('SELECT id,payload FROM assets WHERE market=?', (market,)):
                    item = json.loads(record['payload'])
                    if item.get('asset_group') != group or item.get('symbol') not in selected:
                        continue
                    updated = number(item.get('updated'))
                    price = number(item.get('price'))
                    stale = updated is None or now.timestamp() - updated > 900 or updated > now.timestamp() + 60
                    windows = {}
                    for h in ([12, 168, 720] if full else [hours]):
                        target = updated - h * 3600 if updated else None
                        baseline = db.execute('SELECT ts,price FROM bars WHERE id=? AND source=? AND ts<=? '
                                              'ORDER BY ts DESC LIMIT 1', (record['id'], 'binance', target)).fetchone() if target else None
                        valid = bool(baseline and number(baseline['price']) and baseline['price'] > 0
                                     and target - baseline['ts'] <= 3700 and price is not None and price > 0)
                        windows[f'{h}h'] = dict(return_pct=round((price / baseline['price'] - 1) * 100, 4) if valid else None,
                            window_start_target=iso(target, now), baseline_at=iso(baseline['ts'], now) if valid else None,
                            window_end=iso(updated, now), quality='stale' if stale else 'partial' if valid else 'missing',
                            missing_reason='baseline unavailable or >3700s before target' if not valid else
                                'source sample stale' if stale else 'trend persistence and relative strength not exported')
                    row = {key: item.get(key) for key in ('symbol', 'name', 'asset_group', 'instrument_type',
                        'instrument_label', 'underlying_type', 'contract_type', 'classification_source',
                        'base_asset', 'spot_available', 'spot_symbol', 'spot_label')}
                    row.update(market=market, asset_type=item.get('instrument_type'), price=price,
                        volume_24h=number(item.get('volume_24h')), funding_rate_percent=number(item.get('funding_rate_percent')),
                        data_asof=iso(updated, now), session_date=datetime.fromtimestamp(updated, now.tzinfo).date().isoformat() if updated else None,
                        price_adjustment='exchange traded token/contract price; not split/dividend adjusted equity close',
                        source='Binance exchangeInfo/product directory/ticker and local hourly bars',
                        windows=windows, stale=stale, quality='stale' if stale else 'partial',
                        trend_score=None, relative_strength_pct=None)
                    rows.append(row)
                universe_count = len(rows)
                candidates = [r for r in rows if r['windows'][f'{hours}h']['return_pct'] is not None]
                if not full:
                    positive = sorted((r for r in candidates if r['windows'][f'{hours}h']['return_pct'] > 0),
                                      key=lambda r: r['windows'][f'{hours}h']['return_pct'], reverse=True)[:5]
                    negative = sorted((r for r in candidates if r['windows'][f'{hours}h']['return_pct'] < 0),
                                      key=lambda r: r['windows'][f'{hours}h']['return_pct'])[:5]
                    rows = positive + negative
                groups.append(dict(market=market, asset_group=group, universe_count=universe_count,
                    eligible_window_count=len(candidates), selected_count=len(rows), items=rows,
                    selection='all classified collector-selected symbols' if full else 'positive Top5 and negative Bottom5 by window return; not TG selection',
                    data_asof=iso(health['updated'], now) if health else None,
                    stale=not health or now.timestamp() - health['updated'] > 900,
                    coverage_pool='separate 24h quote-volume pool: securities up to150 per market; macro up to30; pure crypto quota unchanged',
                    missing_reason=None if health else 'collector health unavailable'))
    else:
        issues.append('Binance trend database unavailable')
    path = root / 'data/macro_dashboard_snapshot.json'
    cache = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    checked = number(cache.get('global_assets_checked_at'))
    etfs = []
    for item in cache.get('global_assets', []):
        asof = str(item.get('asof') or '')[:10]
        try:
            old = (now.date() - datetime.fromisoformat(asof).date()).days > 7
        except ValueError:
            old = True
        stale = old or not checked or now.timestamp() - checked > 86400 or item.get('collection_quality') == 'stale'
        row = {key: item.get(key) for key in ('id', 'name', 'group', 'symbol', 'listing_market', 'currency',
            'value', 'source', 'price_adjustment', 'previous_session')}
        row.update(asset_type='INDEX' if item.get('kind') == '指数' else 'ETF',
            leverage_multiplier=None, leverage_classification='unverified' if item.get('kind') != '指数' else 'not_applicable',
            data_asof=item.get('asof'), session_date=asof or None, source_checked_at=iso(checked, now),
            changes={key: number((item.get('changes') or {}).get(key)) for key in ('1d', '3d', '7d', '30d')},
            baseline_dates={'1d': item.get('previous_session'), '3d': None, '7d': None, '30d': None},
            stale=stale, quality='stale' if stale else 'partial',
            missing_reason=item.get('missing_reason') or 'exact 3/7/30d baseline dates and exchange-session freshness verification unavailable')
        etfs.append(row)
    if not etfs:
        issues.append('global index/ETF cache unavailable')
    return dict(schema_version='1.0', snapshot_id=f'market-extensions-{int(now.timestamp())}',
        generated_at=now.isoformat(timespec='seconds'), quality='partial',
        stale=bool(issues) or any(g['stale'] for g in groups) or any(r['stale'] for g in groups for r in g['items']) or any(r['stale'] for r in etfs),
        missing_reason=issues or None, standalone_path='agent/v1/market_extensions.json',
        window_definition={'binance': 'rolling actual-sample hours; 12h daily,168h weekly,720h monthly; not trading days',
            'global_assets_1d': 'previous available completed trading session, not intraday',
            'global_assets_3_7_30d': 'calendar-day target using earlier available close; exact baseline dates currently null'},
        securities_and_macro_contracts=groups, global_indices_and_etfs=etfs,
        limitations=['Tokenized securities are not direct stock ownership; perpetuals are derivatives.',
            'ETF labels follow verified Binance product metadata, not symbol suffix guesses.',
            'Price changes are not total returns; ETF leverage multipliers are unverified/null.',
            'Standalone file is live supplemental data; historical embedded data stays frozen.'])


def write_extensions(root, crypto_db, output, now):
    from generate_agent_snapshot import atomically_write
    payload = build_extensions(root, crypto_db, now, full=True)
    if not payload['securities_and_macro_contracts'] and not payload['global_indices_and_etfs']:
        return None
    atomically_write(output / 'market_extensions.json', json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8'))
    return payload
