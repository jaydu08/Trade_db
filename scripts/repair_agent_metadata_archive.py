"""Explicit metadata-only archive correction, preserving every selected price."""
import copy
import hashlib
import json
from datetime import datetime

from modules.ingestion.market_context import qualify_index_record
from generate_agent_snapshot import atomically_write, immutably_create
from agent_snapshot_health import audit


def repair(repo, day, corrected_at):
    output = repo / 'agent/v1'
    path = output / 'history' / (day + '.json')
    raw = path.read_bytes()
    old = json.loads(raw)
    if old.get('revision', {}).get('kind') == 'verified_index_metadata':
        return []
    new = copy.deepcopy(old)
    if new.get('archive_date') != day:
        raise ValueError('archive date mismatch')
    all_rows = new['anomalies']['stocks_up'] + new['anomalies']['stocks_down']
    for market in new['overview']['markets']:
        market['major_indices'] = [qualify_index_record(row) for row in market.get('major_indices') or []]
        if market.get('major_index'):
            market['major_index'] = qualify_index_record(market['major_index'])
        checks = [row['source_checked_at'] for row in all_rows
                  if row['market'] == market['market'] and row.get('source_checked_at')]
        market['source_checked_at'] = max(checks, default=None)
    for row in new['market_anomalies']['coverage']:
        checks = [item['source_checked_at'] for item in all_rows
                  if item['market'] == row['market'] and item.get('source_checked_at')]
        row['source_checked_at'] = max(checks, default=None)
    original_id = old['snapshot_id']
    original_path = f'agent/v1/revisions/{day}/{original_id}.original.json'
    original_health_path = f'agent/v1/revisions/{day}/{original_id}.original-health.json'
    new['snapshot_id'] += '-metadata-r1'
    new['revision'] = dict(kind='verified_index_metadata', corrected_at=corrected_at.isoformat(),
                           original_path=original_path,original_health_path=original_health_path,
                           original_sha256=hashlib.sha256(raw).hexdigest(),prices_modified=False,
                           reason='unverified US index notional becomes null; source_checked_at separated from price observation time')
    report = audit(new,output,datetime.fromisoformat(old['generated_at']))
    if report['failures']:
        raise ValueError('metadata revision quality audit failed: ' + str(report['failures']))
    health_path = output / 'health' / (day + '.json')
    immutably_create(repo / original_path,raw)
    immutably_create(repo / original_health_path,health_path.read_bytes())
    data = (json.dumps(new,ensure_ascii=False,separators=(',', ':'),allow_nan=False)+'\n').encode()
    atomically_write(path,data)
    latest = output / 'latest.json'
    latest_matches = json.loads(latest.read_bytes()).get('snapshot_id') == original_id
    if latest_matches:
        atomically_write(latest,data)
    atomically_write(health_path,(json.dumps(report,ensure_ascii=False,separators=(',', ':'),allow_nan=False)+'\n').encode())
    return [str(p.relative_to(repo)) for p in (path,health_path,repo / original_path,repo / original_health_path)] + (['agent/v1/latest.json'] if latest_matches else [])
