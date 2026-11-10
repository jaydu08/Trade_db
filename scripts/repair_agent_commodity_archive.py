"""Explicit, versioned futures-only repair; other frozen market prices stay untouched."""
import copy
import hashlib
import json
from datetime import datetime
from generate_agent_snapshot import macro_snapshot, atomically_write, immutably_create
from agent_snapshot_health import audit


def repair(root,repo,day,now):
    output = repo / 'agent/v1'
    path = output / 'history' / (day+'.json')
    raw = path.read_bytes()
    old = json.loads(raw)
    if old.get('revision',{}).get('kind') == 'commodity-source-v1':
        return []
    if old.get('archive_date') != day or now.date().isoformat() != day:
        raise ValueError('live commodity repair only supports same-day archive; never inject live prices into historical dates')
    rows,_,_ = macro_snapshot(root,now)
    futures = [r for r in rows if r['category']=='futures']
    if {r['id'] for r in futures} != {'CL','OIL','GC','SI','HG'} or any(r['stale'] for r in futures):
        raise ValueError('five fresh, dated commodity quotes required for price correction')
    new = copy.deepcopy(old)
    new['overview']['macro'] = [r for r in old['overview']['macro'] if r['category']!='futures']+futures
    count = sum(bool(r.get('stale')) for r in new['overview']['macro'])
    for health in new['source_health']:
        if health['domain']=='macro':
            activity_stale = any(r.get('stale') for r in new['overview'].get('crypto_activity',{}).values())
            health.update(stale_indicator_count=count,observation_stale=bool(count) or activity_stale,
                          stale=bool(health.get('collection_stale')) or bool(count) or activity_stale,
                          commodity_source_checked_at=max(r['source_checked_at'] for r in futures),
                          missing_reason=f'{count} macro observations stale' if count else None)
    original_id = old['snapshot_id']
    backup = f'agent/v1/revisions/{day}/{original_id}.original.json'
    backup_health = f'agent/v1/revisions/{day}/{original_id}.original-health.json'
    new.update(snapshot_id=original_id+'-commodity-r1',generated_at=now.isoformat(timespec='seconds'))
    new['revision'] = dict(kind='commodity-source-v1',corrected_at=now.isoformat(),
                           original_generated_at=old['generated_at'],original_snapshot_id=original_id,
                           original_path=backup,original_health_path=backup_health,
                           original_sha256=hashlib.sha256(raw).hexdigest(),
                           scope='futures in overview.macro only, plus freshness/health rollup; no other market prices regenerated',
                           reason='AkShare Sina HTML dependency failed; verified Tencent dated futures quotes replace obsolete cached quotes')
    new['stale'] = any(h.get('stale') for h in new['source_health']) or bool(count)
    report = audit(new,output,now)
    if report['failures']:
        raise ValueError('commodity revision failed audit: '+str(report['failures']))
    immutably_create(repo / backup,raw)
    health_path = output / 'health' / (day+'.json')
    immutably_create(repo / backup_health,health_path.read_bytes())
    data = (json.dumps(new,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n').encode()
    atomically_write(path,data)
    files = [str(p.relative_to(repo)) for p in (path,health_path,repo/backup,repo/backup_health)]
    latest = output / 'latest.json'
    if json.loads(latest.read_bytes()).get('snapshot_id')==original_id:
        atomically_write(latest,data)
        files.append('agent/v1/latest.json')
    atomically_write(health_path,(json.dumps(report,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n').encode())
    return files
