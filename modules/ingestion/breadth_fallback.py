"""Independent, dated asset-catalog breadth. Never a Trend/Top/holdings universe."""
import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, date, time
from pathlib import Path
from zoneinfo import ZoneInfo

from modules.ingestion.tencent_quote import quote_code, fetch_quotes
from modules.market_sessions import us_last_completed, us_close_at, us_is_session

VERSION = 'independent_breadth_v1'
MINIMUM = {'CN':3000,'HK':1000,'US':3000}


def _atomic(path, payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp = tempfile.mkstemp(dir=path.parent,prefix='.breadth-')
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as handle:
            json.dump(payload,handle,ensure_ascii=False,allow_nan=False)
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def frame_breadth(market, records, expected, checked_at, requested_count=None):
    """Count verified completed-session quotes, with conservative alias dedup."""
    from modules.ingestion.market_context import finite
    zone = ZoneInfo('America/New_York' if market=='US' else 'Asia/Shanghai')
    day = date.fromisoformat(expected)
    close = us_close_at(day) if market=='US' else datetime.combine(day,time(15 if market=='CN' else 16),zone)
    if market=='HK' and day in {date(2026,2,16),date(2026,12,24),date(2026,12,31)}:
        close = datetime.combine(day,time(12),zone)
    grouped, excluded = {}, {}
    for row in records:
        try:
            raw = str(row.get('代码') or '')
            identity = quote_code(raw,market) if raw else ''
        except ValueError:
            identity = ''
        if not identity:
            excluded['invalid_identity'] = excluded.get('invalid_identity',0)+1
            continue
        grouped.setdefault(identity,[]).append(row)
    counts = dict(advancing=0,declining=0,unchanged=0)
    sources, observations = {}, []
    duplicates = sum(len(rows)-1 for rows in grouped.values())
    for identity,rows in grouped.items():
        valid = []
        for row in rows:
            reason = None
            price,pct = finite(row.get('最新价')),finite(row.get('涨跌幅'))
            provider = str(row.get('行情来源') or '')
            if provider not in ('Tencent','Sina'):
                reason = 'unverified_quote_provider'
            elif price is None or price<=0 or pct is None:
                reason = 'invalid_or_suspended_quote'
            try:
                stamp = datetime.fromisoformat(str(row.get('行情时间') or ''))
                if stamp.tzinfo is None: stamp = stamp.replace(tzinfo=zone)
                stamp = stamp.astimezone(zone)
                if stamp.date()!=day: reason = 'wrong_session_date'
                elif stamp<close: reason = 'incomplete_session_quote'
                elif stamp>checked_at: reason = 'future_observation'
            except (ValueError,TypeError):
                reason = 'missing_quote_time'
            if reason:
                excluded[reason] = excluded.get(reason,0)+1
            else:
                valid.append((stamp,pct,provider))
        if not valid: continue
        latest = max(stamp for stamp,_,_ in valid)
        peers = [r for r in valid if r[0]==latest]
        if len({pct for _,pct,_ in peers})>1:
            excluded['conflicting_alias_quote'] = excluded.get('conflicting_alias_quote',0)+1
            continue
        stamp,pct,provider = sorted(peers,key=lambda r:r[2])[0]
        counts['advancing' if pct>0 else 'declining' if pct<0 else 'unchanged']+=1
        sources[provider] = sources.get(provider,0)+1
        observations.append(stamp.isoformat())
    covered = sum(counts.values())
    denominator = requested_count if requested_count is not None else len(grouped)
    ratio = covered/denominator if denominator else 0
    valid = covered>=MINIMUM[market] and ratio>=.5
    return dict(pipeline_version=VERSION,session_date=expected,
                **{k:counts[k] if valid else None for k in counts},observed_counts=counts,
                enumerated_count=len(grouped),requested_universe_count=requested_count,
                covered_count=covered,coverage_ratio=round(ratio,4),
                coverage_ratio_basis='requested full asset catalog' if requested_count is not None else 'unique received quotes; original requested universe count unavailable',
                source=' + '.join(sorted(sources))+' dated asset-catalog quotes',provider_counts=sources,
                collected_at=checked_at.isoformat(),source_checked_at=checked_at.isoformat(),
                data_asof=max(observations,default=None),observed_at=max(observations,default=None),
                unit='securities',is_all_market_total=False,
                count_basis='exact completed-session issues observed; aliases deduplicated; no extrapolation',
                coverage_scope='full Asset catalog quote acquisition, not Trend candidates/Top/watchlists; valid same-session received securities only, not exchange-certified all-market total',
                includes='provider-catalog securities, potentially funds, ADRs, warrants and rights; not ordinary-stock-only',
                excluded_reason_counts=excluded,duplicate_alias_count=duplicates,
                quality='partial' if valid else 'missing',stale=not valid,
                missing_reason='asset-catalog observation coverage only; not certified all-market totals' if valid else 'too few dated completed-session backup quotes')


def save_frame(root,market,frame,now,expected=None,checked_at=None,requested_count=None):
    expected = expected or (str(us_last_completed(now)) if market=='US' else str(now.astimezone(ZoneInfo('Asia/Shanghai')).date()))
    record = frame_breadth(market,frame.to_dict('records'),expected,checked_at or now,requested_count)
    if record['stale']: return record
    path = Path(root)/f'data/agent_breadth/{market}/{expected}.json'
    previous = load_saved(root,market,expected,now)
    if not previous or previous['covered_count']<=record['covered_count']:
        _atomic(path,record)
    return record


def load_saved(root,market,expected,now):
    try:
        record=json.loads((Path(root)/f'data/agent_breadth/{market}/{expected}.json').read_text())
        checked=datetime.fromisoformat(record['source_checked_at'])
        if (record.get('pipeline_version')==VERSION and record.get('session_date')==expected
                and not record.get('stale') and record.get('covered_count',0)>=MINIMUM[market]
                and checked.tzinfo and checked<=now):
            return record
    except (OSError,ValueError,KeyError,TypeError): pass
    return None


def live_backup(root,market,expected,now):
    if market=='US' and now.astimezone(ZoneInfo('America/New_York')).date()>date.fromisoformat(expected):
        # Once a new regular session starts, its returns cannot stand in for yesterday.
        local=now.astimezone(ZoneInfo('America/New_York'))
        if us_is_session(local.date()) and (local.hour,local.minute)>=(9,30):
            raise ValueError('US live session advanced; previous completed-session breadth requires saved EOD quotes')
    with sqlite3.connect((Path(root)/'data/meta.db').as_uri()+'?mode=ro',uri=True) as db:
        rows=db.execute('SELECT symbol,updated_at FROM asset WHERE market=?',(market,)).fetchall()
    identities={}
    for symbol,_ in sorted(rows):
        identities.setdefault(quote_code(symbol,market),symbol)
    if len(identities)<MINIMUM[market]: raise ValueError('independent full asset catalog too small')
    quotes=fetch_quotes(list(identities.values()),market)
    checked=datetime.now(ZoneInfo('Asia/Shanghai'))
    records=[{'代码':s,'最新价':q.get('price'),'涨跌幅':q.get('pct_chg'),
              '行情时间':q.get('timestamp'),'行情来源':q.get('provider')} for s,q in quotes.items()]
    record=frame_breadth(market,records,expected,checked,len(identities))
    record.update(universe_source='Trade_db full Asset metadata catalog; not TrendSeedPool',
                  catalog_latest_updated_at=max((r[1] for r in rows if r[1]),default=None),
                  universe_identity_sha256=hashlib.sha256('\n'.join(sorted(identities)).encode()).hexdigest())
    if record['stale']: raise ValueError(record['missing_reason'])
    _atomic(Path(root)/f'data/agent_breadth/{market}/{expected}.json',record)
    return record


def fetch_breadth(root,market,expected,now):
    from modules.ingestion.market_context import full_market_rows,breadth_from_rows
    attempts=[]
    try:
        rows,total=full_market_rows(market)
        result=breadth_from_rows(market,rows,total,expected,now)
        if result['stale']: raise ValueError(result.get('missing_reason'))
        return dict(result,fallback_used=False,source_attempts=[dict(source='Eastmoney',status='ok')])
    except Exception as exc:
        attempts.append(dict(source='Eastmoney',status='failed',error_type=type(exc).__name__,
                             http_status=getattr(getattr(exc,'response',None),'status_code',None)))
    result=load_saved(root,market,expected,now)
    if result:
        return dict(result,fallback_used=True,source_attempts=attempts+[dict(source=result['source'],status='ok',mode='saved_completed_session')],
                    read_at=now.isoformat())
    try:
        result=live_backup(root,market,expected,now)
        return dict(result,fallback_used=True,source_attempts=attempts+[dict(source=result['source'],status='ok',mode='independent_live_catalog')])
    except Exception as exc:
        attempts.append(dict(source='Tencent asset-catalog backup',status='failed',error_type=type(exc).__name__,reason=str(exc)[:160]))
    return dict(advancing=None,declining=None,unchanged=None,session_date=expected,source='Eastmoney + independent Tencent asset-catalog backup',
                quality='missing',stale=True,fallback_used=True,source_attempts=attempts,collected_at=now.isoformat(),
                coverage_scope='no qualified same-session source; never substituted with Trend candidates',missing_reason='all breadth sources failed or lacked completed-session coverage')
