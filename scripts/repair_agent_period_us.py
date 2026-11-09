"""Explicit versioned US-only correction; never replay today's crypto into an old report."""
import argparse
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from generate_agent_period_snapshot import stock_period, MAX_BYTES
from generate_agent_snapshot import atomically_write, immutably_create


def repair(root, path, now, apply=False):
    original = path.read_bytes()
    old = json.loads(original)
    if old.get('revision', {}).get('repair') == 'verified-us-session-v1':
        return old, []
    cutoff = datetime.fromisoformat(old['publication_context']['report_cutoff'])
    days = 7 if old['period'] == 'weekly' else 30
    rows, health = stock_period(root, days, cutoff, {}, allow_late_verified_backfill=True)
    us_rows = [r for r in rows if r['market'] == 'US']
    us_health = next(h for h in health if h['domain'] == 'stocks_US')
    if us_health['stale'] or us_health['latest_session_security_count'] < 10:
        raise ValueError('verified historical US session recovery not ready')
    for row in us_rows:
        row['trend_score'] = None
        row['trend_score_missing_reason'] = 'historical correction; live Trend scores not replayed'
        row['historically_recovered_at'] = now.isoformat()
    new = copy.deepcopy(old)
    new['markets']['equities_and_commodities'] = [
        r for r in old['markets']['equities_and_commodities'] if r['market'] != 'US'] + us_rows
    us_health.update(collection_freshness='historically_recovered',
                     observation_freshness='current' if us_rows else 'unavailable',
                     stale_item_count=0, stale_item_ids=[],
                     partial_item_count=sum(r.get('quality') == 'partial' for r in us_rows),
                     critical_data_available=bool(us_rows), historically_recovered_at=now.isoformat())
    if not us_rows:
        us_health['missing_reason'] = 'latest close recovered; no US securities have sufficient verified window baseline'
    new['source_health'] = [us_health if h['domain'] == 'stocks_US' else h for h in old['source_health']]
    new['session_date']['US'] = us_health['data_asof']
    new['data_asof']['stocks_US'] = us_health['data_asof']
    new['stale_item_ids'] = [s for s in old.get('stale_item_ids', []) if not s.startswith('stocks_US:')]
    new['stale_item_count'] = len(new['stale_item_ids'])
    new['stale_domains'] = [h['domain'] for h in new['source_health'] if h.get('stale')]
    new['stale'] = bool(new['stale_domains'] or new['stale_item_ids'])
    new['partial_item_count'] = sum(h.get('partial_item_count', 0) for h in new['source_health'])
    previous_id = old['snapshot_id']
    backup = path.parent.parent / 'revisions' / old['period'] / path.stem / (previous_id + '.original.json')
    new['snapshot_id'] = previous_id + '-us-session-r1'
    new['revision'] = dict(repair='verified-us-session-v1', revised_at=now.isoformat(),
                           supersedes=previous_id, original_sha256=hashlib.sha256(original).hexdigest(),
                           original_path=str(backup.relative_to(path.parent.parent.parent.parent)),
                           scope='US equities only; original report cutoff, CN/HK/CF, crypto and macro retained',
                           reason='mixed vendor-session/next-day archive dates plus failed disposable cache writes',
                           historical_recovery='Only verified US regular closes observed by original cutoff are added; recovery occurred later')
    # Original creation time stays original; revision time is separate and explicit.
    data = (json.dumps(new, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()
    if len(data) > MAX_BYTES:
        raise ValueError('corrected period exceeds size limit')
    if apply:
        immutably_create(backup, original)
        atomically_write(path, data)
    return new, [path, backup]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--path', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    result, paths = repair(args.root, args.path, datetime.now(ZoneInfo('Asia/Shanghai')), args.apply)
    print(json.dumps(dict(snapshot_id=result['snapshot_id'], session_date=result['session_date'],
                         us_rows=sum(r['market']=='US' for r in result['markets']['equities_and_commodities']),
                         files=[str(p) for p in paths], applied=args.apply), ensure_ascii=False))
