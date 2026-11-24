"""Public-only Quant exports under the existing atomic agent-data Git commit."""
import copy,hashlib,json
from datetime import datetime,timedelta
from pathlib import Path
from .engine import MARKETS,FOCUS,FORMULA_VERSION,CATALOG_VERSION,expected_session,RULES,clean,signal_names
from .runtime import cached,evidence,atomic


def snapshot(root,now):
    data=copy.deepcopy(cached(root))
    for market in data.get('markets',[]):
        code=market['market']
        try:expected=str(expected_session(code,now))
        except ValueError:
            market.update(quality='pending',missing_reason='交易所新年度日历尚未验证，暂停该市场信号')
            for row in data.get('signals',[]):
                if row['market']==code:row.update(state=None,new_event=None,quality='pending',eligible_for_realtime_event=False,missing_reason=market['missing_reason'])
            continue
        if market.get('session_date')!=expected:
            market.update(quality='stale',missing_reason='快照应为最近完整交易日 '+expected,expected_session_date=expected)
            for row in data.get('signals',[]):
                if row['market']==code:row.update(state=None,new_event=None,quality='stale',missing_reason='价格尚未更新至最近完整交易日',eligible_for_realtime_event=False)
    generated=data.get('generated_at')
    if not generated or (now-datetime.fromisoformat(generated)).total_seconds()>7200:
        data['quality']='stale';data['collector_stale']=True
        for row in data.get('signals',[]):row.update(state=None,new_event=None,quality='stale',eligible_for_realtime_event=False,missing_reason='量化计算任务超过两小时未更新')
    if any(m.get('quality')!='ok' for m in data.get('markets',[])):data['quality']='partial'
    return data


def summary(root,now,days=None,compact=False):
    data=snapshot(root,now);rows=data.get('signals',[])
    result=dict(schema_version='1.0',snapshot_id=data.get('snapshot_id'),generated_at=data.get('generated_at'),
                quality=data.get('quality','missing'),formula_version=data.get('formula_version'),research_version=data.get('research_version'),
                markets=[{k:r.get(k) for k in ('market','name','session_date','expected_session_date','bar_end_at','available_at','fetched_at','price','price_source','quality','iv_last_value','iv_last_session','iv_quality','iv_name')} for r in data.get('markets',[])],
                signals=[{k:r.get(k) for k in ('market','signal_id','session_date','state','new_event','quality','episode_duration_bars','event_start_uncertain','eligible_for_realtime_event')} for r in rows],
                focus_evidence=[{k:r['evidence'].get(k) for k in ('market','signal_id','holding_period','completed_events','wins','confidence_interval','median_return_pct','worst_mae_pct','matched_state_baseline')}
                                for r in rows if (r['market'],r['signal_id']) in FOCUS and r.get('evidence')],
                forward={k:v for k,v in data.get('forward',{}).items() if k!='evaluations'},
                missing_reason=data.get('missing_reason'),
                history_path=f'agent/v1/quant/{"history" if days is None else "weekly" if days==7 else "monthly"}/{now.date()}.json' if data.get('snapshot_id') else None,
                evidence_path='agent/v1/quant/evidence/'+str(data.get('research_version'))+'.json' if data.get('research_version') else None,
                notes=data.get('notes',[]))
    if days:
        cutoff=(now.date()-timedelta(days=days-1)).isoformat();events=[]
        for market in MARKETS:
            path=Path(root)/f'data/quant/events/{market}.json'
            try:events.extend(e for e in json.loads(path.read_text())['events'] if cutoff<=e['session_date']<=str(now.date()))
            except (OSError,ValueError,KeyError):pass
        result.update(window=dict(days=days,basis='calendar days of signal session dates; not trading-day count',start=cutoff,end=str(now.date())),
                      period_events=[{k:e.get(k) for k in ('id','market','signal_id','session_date','status','origin')} for e in events])
    if compact:
        result=dict(schema_version='1.0',representation='reference_summary',snapshot_id=data.get('snapshot_id'),
                    generated_at=data.get('generated_at'),quality=data.get('quality','missing'),formula_version=data.get('formula_version'),
                    research_version=data.get('research_version'),rule_count=len(rows),
                    market_sessions={m['market']:{k:m.get(k) for k in ('session_date','quality','iv_quality')} for m in data.get('markets',[])},
                    focus_signals=[{k:r.get(k) for k in ('market','signal_id','state','new_event','quality')} for r in rows if (r['market'],r['signal_id']) in FOCUS],
                    rule_counts=dict(new=sum(r.get('new_event') is True for r in rows),true=sum(r.get('state') is True for r in rows),unknown=sum(r.get('state') is None for r in rows)),
                    history_path=result['history_path'],evidence_path=result['evidence_path'],window=result.get('window'),
                    period_event_count=len(result.get('period_events',[])),
                    note='Complete rule state, all period events, evidence and forward evaluation are in history_path; no market rankings removed')
    return clean(result)


def _bytes(payload):return json.dumps(clean(payload),ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()


def budget_reference(block):
    result={k:block.get(k) for k in ('schema_version','snapshot_id','generated_at','quality','formula_version','research_version','history_path','evidence_path')}
    result.update(representation='reference_only_size_budget',rule_count=block.get('rule_count',len(block.get('signals',[]))),
                  note='Full quantitative payload is in history_path; existing market report unchanged')
    return result


def write(root,output,now,days=None,preview=False):
    root=Path(root);output=Path(output);data=snapshot(root,now)
    if not data.get('snapshot_id'):return set()
    folder='previews' if preview else 'history' if days is None else 'weekly' if days==7 else 'monthly'
    history=output/'quant'/folder/((data['snapshot_id'] if preview else str(now.date()))+'.json')
    if history.exists():
        # A failed push may leave a frozen local file. Restage all dependencies,
        # rather than publishing a parent report whose referenced files are absent.
        frozen=json.loads(history.read_text());paths={history}
        for ref in list(frozen.get('event_files',{}).values())+[frozen.get('research_evidence',{}),frozen.get('catalog_ref',{})]:
            if not ref.get('path'):continue
            path=output.parent.parent/ref['path']
            if not path.resolve().is_relative_to(output.parent.parent.resolve()):raise ValueError('invalid quant reference')
            if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest()!=ref['sha256']:raise ValueError('frozen quant dependency missing or hash mismatch')
            paths.add(path)
        backtest=output/'quant/backtest_events'/(str(frozen.get('research_version'))+'.json')
        if backtest.exists():paths.add(backtest)
        catalog_alias=output/'quant/catalog.json'
        if catalog_alias.exists():paths.add(catalog_alias)
        for market in MARKETS:
            alias=output/f'quant/events/{market}.json'
            if alias.exists():paths.add(alias)
        hashes={str(p.relative_to(output.parent.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
        manifest=output/'quant/manifest.json'
        atomic(manifest,dict(schema_version='1.0',snapshot_id=frozen['snapshot_id'],report_path=str(history.relative_to(output.parent.parent)),
                             formula_version=frozen.get('formula_version'),research_version=frozen.get('research_version'),quality=frozen.get('quality'),files=hashes))
        paths.add(manifest)
        return {str(p.relative_to(output.parent.parent)) for p in paths}
    catalog=dict(schema_version='1.0',catalog_version=CATALOG_VERSION,formula_version=FORMULA_VERSION,markets=MARKETS,rules=RULES,
                 rule_applicability={market:signal_names(market) for market in MARKETS},
                 states=['ok','pending','stale','missing','invalid','insufficient_history','source_conflict','not_applicable'],
                 purpose='research monitor, not trade execution',percentile_ties='prior valid values <= current',
                 baseline_policy='Historical full sample/subperiod evidence separated from prospective evaluations')
    catalog_path=output/'quant/catalog_versions'/(CATALOG_VERSION+'.json')
    if catalog_path.exists() and json.loads(catalog_path.read_text())!=catalog:raise ValueError('immutable quant catalog changed without version bump')
    files={output/'quant/catalog.json':catalog,catalog_path:catalog}
    data['catalog_ref']=dict(path=str(catalog_path.relative_to(output.parent.parent)),sha256=hashlib.sha256(_bytes(catalog)).hexdigest())
    ev=evidence(root)
    if ev.get('research_version'):
        evidence_path=output/'quant/evidence'/(ev['research_version']+'.json')
        if evidence_path.exists() and json.loads(evidence_path.read_text())!=ev:raise ValueError('immutable evidence version changed')
        files[evidence_path]=ev
        data['research_evidence']=dict(path=str(evidence_path.relative_to(output.parent.parent)),sha256=hashlib.sha256(_bytes(ev)).hexdigest())
        backtest_path=root/'data/quant/backtest_events.json'
        if not backtest_path.exists():raise ValueError('frozen research event details missing')
        files[output/'quant/backtest_events'/(ev['research_version']+'.json')]=json.loads(backtest_path.read_text())
    event_refs={}
    for market in MARKETS:
        try:events=json.loads((root/f'data/quant/events/{market}.json').read_text())['events']
        except (OSError,ValueError,KeyError):events=[]
        slim=[]
        for event in events:
            item={k:v for k,v in event.items() if k not in ('source_records','data_version')}
            item['sources']={k:{f:v.get(f) for f in ('source','raw_response_ref','data_hash','available_at','fetched_at')} for k,v in event.get('source_records',{}).items() if v}
            slim.append(item)
        obj=dict(market=market,formula_version=FORMULA_VERSION,events=slim)
        digest=hashlib.sha256(_bytes(obj)).hexdigest();version_path=output/'quant/event_versions'/(digest+'.json')
        files[version_path]=obj;files[output/f'quant/events/{market}.json']=obj
        event_refs[market]=dict(path=str(version_path.relative_to(output.parent.parent)),sha256=digest,count=len(slim))
    data['event_files']=event_refs
    data['report_date']=str(now.date());data['report_period']='preview' if preview else 'daily' if days is None else str(days)+'d'
    if days:data['period_summary']=summary(root,now,days)
    files[history]=data
    hashes={str(path.relative_to(output.parent.parent)):hashlib.sha256(_bytes(obj)).hexdigest() for path,obj in files.items()}
    manifest=dict(schema_version='1.0',snapshot_id=data['snapshot_id'],published_candidate_at=now.isoformat(),
                  report_path=str(history.relative_to(output.parent.parent)),formula_version=FORMULA_VERSION,
                  research_version=ev.get('research_version'),quality=data.get('quality'),files=hashes)
    manifest_path=output/'quant/manifest.json'
    for path,obj in files.items():
        encoded=_bytes(obj)
        if path.exists() and '/event_versions/' in str(path) and path.read_bytes()!=encoded:raise ValueError('event content hash conflict')
        atomic(path,obj)
    # Verify every manifest reference before Git staging, not after consumer access.
    for path,digest in hashes.items():
        actual=hashlib.sha256((output.parent.parent/path).read_bytes()).hexdigest()
        if actual!=digest:raise ValueError('quant manifest mismatch '+path)
    atomic(manifest_path,manifest)
    files[manifest_path]=manifest
    return {str(path.relative_to(output.parent.parent)) for path in files}
