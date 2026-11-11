"""Versioned breadth-only repair; never regenerate frozen prices or selections."""
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from modules.ingestion.breadth_fallback import fetch_breadth,VERSION
from generate_agent_snapshot import atomically_write,immutably_create,read_json
from agent_snapshot_health import audit


def repair(root,repo,day,now):
    output=repo/'agent/v1'
    path=output/'history'/f'{day}.json'
    raw=path.read_bytes()
    old=json.loads(raw)
    if old.get('revision',{}).get('kind')=='independent-breadth-v1':return []
    if old.get('archive_date')!=day:raise ValueError('archive date mismatch')
    records={}
    for market in old['overview']['markets']:
        code=market['market']
        if code not in ('CN','HK','US'):continue
        record=fetch_breadth(root,code,market['session_date'],now)
        if record.get('stale') or record.get('advancing') is None:
            raise ValueError(code+' no qualified independent breadth source')
        if record['session_date']!=market['session_date']:raise ValueError('breadth session mismatch')
        records[code]=record
    revised=max([now]+[datetime.fromisoformat(r['source_checked_at']) for r in records.values() if r.get('source_checked_at')])
    new=copy.deepcopy(old)
    for market in new['overview']['markets']:
        if market['market'] in records:market['breadth']=records[market['market']]
    for health in new['source_health']:
        if health['domain'].startswith('market_context_'):
            code=health['domain'].removeprefix('market_context_')
            market=next((m for m in new['overview']['markets'] if m['market']==code),None)
            if not market or code not in records:continue
            indices=market.get('major_indices') or []
            stale=not indices or any(i.get('stale') for i in indices)
            health.update(stale=stale,quality='partial',source='Tencent indices; '+records[code]['source'],breadth_source=records[code]['source'],
                          breadth_source_checked_at=records[code].get('source_checked_at'),
                          missing_reason='index source absent or unverified' if stale else records[code].get('missing_reason'))
    original_id=old['snapshot_id']
    backup=f'agent/v1/revisions/{day}/{original_id}.original.json'
    backup_health=f'agent/v1/revisions/{day}/{original_id}.original-health.json'
    new.update(snapshot_id=original_id+'-breadth-r1',generated_at=revised.isoformat(timespec='seconds'))
    new['revision']=dict(kind='independent-breadth-v1',corrected_at=revised.isoformat(),
                         original_snapshot_id=original_id,original_generated_at=old['generated_at'],
                         original_path=backup,original_health_path=backup_health,
                         original_sha256=hashlib.sha256(raw).hexdigest(),prices_modified=False,
                         scope='CN/HK/US breadth only and corresponding health; all prices/selections/commodity observations retained',
                         reason='Eastmoney HTTP 502; independent dated catalog quotes, not Trend candidates; US original EOD frame recovered')
    new['stale']=any(h.get('stale') for h in new['source_health']) or any(r.get('stale') for r in new['overview']['macro'])
    report=audit(new,output,revised)
    if report['failures']:raise ValueError('breadth revision audit failed: '+str(report['failures']))
    health_path=output/'health'/f'{day}.json'
    immutably_create(repo/backup,raw)
    immutably_create(repo/backup_health,health_path.read_bytes())
    encoded=(json.dumps(new,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n').encode()
    atomically_write(path,encoded)
    files=[str(p.relative_to(repo)) for p in (path,health_path,repo/backup,repo/backup_health)]
    latest=output/'latest.json'
    if json.loads(latest.read_bytes()).get('snapshot_id')==original_id:
        atomically_write(latest,encoded);files.append('agent/v1/latest.json')
    atomically_write(health_path,(json.dumps(report,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n').encode())
    # Update only dated context breadth; do not replace valid archived indices with live US indices.
    context_path=root/f'data/agent_market_context/{day}.json'
    context=read_json(context_path,{})
    for code,record in records.items():
        if context.get('markets',{}).get(code,{}).get('session_date')==record['session_date']:
            context['markets'][code]['breadth']=record
    if context:
        context['breadth_pipeline_version']=VERSION
        atomically_write(context_path,(json.dumps(context,ensure_ascii=False,allow_nan=False)+'\n').encode())
    return files
