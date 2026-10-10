"""Read-only Trend views over alpha_news public market/security evidence."""
import json
import os
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime

DB_PATH = os.getenv('CRYPTO_TREND_DB', '/root/alpha_news/data/crypto_trend.sqlite')
ALPHA_ROOT = os.getenv('ALPHA_NEWS_ROOT', '/root/alpha_news')
PERIODS = {'12h': 12 * 3600, '24h': 86400, '3d': 3 * 86400, '7d': 7 * 86400, '30d': 30 * 86400}
ONCHAIN_SORTS = {'period_change', 'price', 'volume_24h', 'liquidity', 'market_cap', 'fdv', 'age_hours', 'updated'}
ONCHAIN_DISPLAY_LIMIT = 10


def read_json(path):
    try:
        with open(path, encoding='utf-8') as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def period_return(db, item, seconds):
    target = item['updated'] - seconds
    source = 'dex:' + item.get('pair', '') if item['market'] == 'onchain' else 'binance'
    row = db.execute('SELECT ts,price FROM bars WHERE id=? AND source=? AND ts<=? ORDER BY ts DESC LIMIT 1',
                     (item['id'], source, target)).fetchone()
    if row and 0 <= target - row['ts'] <= 3700 and row['price'] > 0 and item.get('price'):
        return (item['price'] / row['price'] - 1) * 100, row['ts'], 'hourly_or_sampled_history'
    if item['market'] == 'onchain' and seconds == 86400 and item.get('provider_change_24h') is not None:
        return item['provider_change_24h'], None, 'DexScreener 24h'
    return None, None, 'insufficient_history'


def effective_security(payload, pair, now):
    result = dict(payload) if payload else {'status': 'unknown', 'label': '待核验', 'reasons': ['尚未完成检测']}
    if result.get('status') == 'pass' and (
        result.get('expires_at', 0) < now or result.get('pair') != pair
    ):
        result.update(status='unknown', label='检测已过期/定价池变化', allow_push=False,
                      reasons=['需重新检测；旧结果不能作为当前通过依据'])
    result.pop('evidence', None)
    return result


def trading_pool_age_hours(created_at, now):
    """Mirror the TG pool-age clock; do not imply contract deployment time."""
    try:
        created = datetime.fromisoformat(str(created_at).replace('Z', '+00:00')).timestamp()
        return round((now - created) / 3600, 1) if 0 < created <= now else None
    except (TypeError, ValueError, OverflowError):
        return None


def english_dominant(text):
    raw = str(text or '')
    latin = len(re.findall(r'[A-Za-z]', raw))
    han = len(re.findall(r'[\u4e00-\u9fff]', raw))
    return latin >= 25 and latin > han * 4


def get_crypto_trend(market='spot', period='12h', risk='pass', query='', pushed_only=False,
                     chain='all', sort_by='period_change', sort_dir='desc'):
    if market not in ('spot', 'futures', 'onchain'):
        raise ValueError('invalid market')
    allowed = ('24h', '3d', '7d', '30d') if market == 'onchain' else ('12h', '3d', '7d', '30d')
    if (period not in allowed or risk not in ('pass', 'review', 'unknown', 'blocked', 'all')
            or sort_by not in ONCHAIN_SORTS or sort_dir not in ('asc', 'desc')):
        raise ValueError('invalid period or risk')
    now = time.time()
    if not os.path.exists(DB_PATH):
        return {'items': [], 'available_chains': [], 'health': {},
                'warning': '采集器正在初始化', 'asof': now, 'counts': {}}
    db = sqlite3.connect('file:' + DB_PATH + '?mode=ro', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        health = {r['component']: dict(json.loads(r['payload']), updated=r['updated'],
                                      stale=now - r['updated'] > 900)
                  for r in db.execute('SELECT * FROM health')}
        state = read_json(os.path.join(ALPHA_ROOT, 'data/bn_monitor_v2_state.json'))
        push_data = read_json(os.path.join(ALPHA_ROOT, 'data/dex_stat/dex_push_records.json'))
        # Existing DEX records are displayed only when an exact chain/address match exists.
        pushes = {}
        def visit(value):
            if isinstance(value, list):
                for v in value:
                    visit(v)
            elif isinstance(value, dict):
                chain = value.get('chain')
                address = value.get('address') or value.get('token_address') or value.get('ca')
                if chain and address:
                    key = str(chain).lower() + ':' + (address.lower() if address.startswith('0x') else address)
                    pushes[key] = value
                else:
                    for v in value.values():
                        if isinstance(v, (dict, list)):
                            visit(v)
        visit(push_data)
        rows = list(db.execute('SELECT a.payload,s.payload AS security FROM assets a LEFT JOIN security s ON a.id=s.id WHERE a.market=?', (market,)))
        selected = health.get(market, {}).get('selected_symbols') if market != 'onchain' else None
        if isinstance(selected, list):
            selected = set(selected)
            rows = [row for row in rows if json.loads(row['payload']).get('symbol') in selected]
        all_items = [json.loads(row['payload']) for row in rows]
        available_chains = sorted({item.get('chain') for item in all_items if item.get('chain')}) if market == 'onchain' else []
        benchmark = None
        if market != 'onchain':
            changes = []
            for symbol in ('BTCUSDT', 'ETHUSDT'):
                base = next((item for item in all_items if item['symbol'] == symbol), None)
                if base and now - base['updated'] <= 900:
                    change = period_return(db, base, PERIODS[period])[0]
                    if change is not None:
                        changes.append(change)
            if len(changes) == 2:
                benchmark = sum(changes) / 2
        names = Counter(i.get('symbol', '').casefold() for i in all_items)
        global_inventory_counts = Counter()
        inventory_counts = Counter()
        history_counts = Counter()
        counts = Counter()
        items = []
        for row, item in zip(rows, all_items):
            security = effective_security(json.loads(row['security']) if row['security'] else None,
                                          item.get('pair', ''), now)
            if market == 'onchain':
                global_inventory_counts[security['status']] += 1
                if chain != 'all' and item.get('chain') != chain:
                    continue
                inventory_counts[security['status']] += 1
            if query and query.casefold() not in ' '.join(str(item.get(k, '')) for k in ('symbol', 'name', 'address', 'narrative')).casefold():
                continue
            item['security'] = security if market == 'onchain' else None
            if market == 'onchain' and english_dominant(item.get('narrative')):
                item['narrative'] = '该合约的公开项目资料为英文，中文摘要整理中；暂不据此推断项目背景。'
                item['narrative_source'] = '原始资料：' + str(item.get('narrative_source') or '公开元数据')
            item['same_name_count'] = names[item.get('symbol', '').casefold()] if market == 'onchain' else 1
            alert = state.get('relative:' + item['symbol'], {}) if market != 'onchain' else {}
            item['last_alert_at'] = alert.get('last_alert_at')
            item['has_alert'] = bool(alert.get('last_alert_at')) if market != 'onchain' else item['id'] in pushes
            item['alert_note'] = '现货/合约共振告警' if market != 'onchain' and item['has_alert'] else ''
            if pushed_only and not item['has_alert']:
                continue
            value, baseline, source = period_return(db, item, PERIODS[period])
            item.update(period_change=value, baseline_at=baseline, return_source=source,
                        stale=now - item['updated'] > 900, oi_change_percent=None,
                        relative_change_percent=(value - benchmark if value is not None and benchmark is not None else None))
            if market == 'onchain':
                item['age_hours'] = trading_pool_age_hours(item.get('pair_created_at'), now)
            if market != 'onchain':
                if now - item.get('depth_updated', 0) > 3600:
                    item['depth_impact_percent'] = None
                if now - item.get('volume_ratio_updated', 0) > 7200:
                    item['hourly_volume_ratio'] = None
            oi_rows = item.pop('oi_history', [])
            if oi_rows:
                end = oi_rows[-1]
                target = end['ts'] - PERIODS[period]
                bases = [r for r in oi_rows if r['ts'] <= target and target - r['ts'] <= 7300]
                if bases and bases[-1].get('quantity') and end.get('quantity') is not None:
                    item['oi_change_percent'] = (end['quantity'] / bases[-1]['quantity'] - 1) * 100
                item['oi_asof'] = end['ts']
            item['history_available'] = value is not None
            if market == 'onchain' and not item['stale'] and value is not None:
                history_counts[security['status']] += 1
            if market == 'onchain' and not item['stale'] and value is not None and value > 0:
                counts[security['status']] += 1
            if market == 'onchain' and risk != 'all' and security['status'] != risk:
                continue
            if market == 'onchain':
                if item['stale'] or value is None or value <= 0:
                    continue
            items.append(item)
        if market == 'onchain':
            # Counts must represent the current period's eligible rising assets, not the
            # historical security inventory. Calculate all statuses even under a risk tab.
            status_order = {'pass': 0, 'review': 1, 'unknown': 2, 'blocked': 3}
            items.sort(key=lambda x: (status_order[x['security']['status']] if risk == 'all' else 0,
                                      x.get(sort_by) is None,
                                      -(x.get(sort_by) or 0) if sort_dir == 'desc' else x.get(sort_by) or 0,
                                      -x['period_change']))
            matching_count = len(items)
            if risk != 'pass':
                items = items[:ONCHAIN_DISPLAY_LIMIT]
        else:
            items.sort(key=lambda x: (not x['stale'] and x['period_change'] is not None,
                                       x['period_change'] if x['period_change'] is not None else float('-inf')), reverse=True)
            matching_count = len(items)
        rank = 0
        for item in items:
            if item['history_available'] and not item['stale']:
                rank += 1
                item['rank'] = rank
            else:
                item['rank'] = None
        return {'market': market, 'period': period, 'items': items, 'health': health,
                'available_chains': available_chains,
                'asof': now, 'counts': dict(counts), 'history_counts': dict(history_counts),
                'inventory_counts': dict(inventory_counts), 'global_inventory_counts': dict(global_inventory_counts),
                'matching_count': matching_count, 'display_limit': ONCHAIN_DISPLAY_LIMIT if market == 'onchain' and risk != 'pass' else None,
                'risk': risk, 'chain': chain,
                'universe_count': len(rows),
                'ranked_count': rank, 'warning': '链上仅覆盖已发现标的；通过筛查不等于官方认证或保证可卖出。'
                if market == 'onchain' else 'USDT现货 / USDT永续各按24h成交额选前150；历史基准为小时级。相对强度是同期涨幅减去BTC/ETH等权涨幅；盘口冲击为$5k模拟，不含手续费。'}
    finally:
        db.close()
