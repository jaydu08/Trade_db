"""Bounded collector/computation job. Read-only UI; no TG or trading calls."""
import fcntl,hashlib,json,logging,os,tempfile,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
from .engine import MARKETS,RULES,FOCUS,FORMULA_VERSION,US_MARKETS,clean,compute,observations,expected_session,bar_end,venue,signal_names
from .store import Store
from . import sources

LOG=logging.getLogger(__name__)
ROOT=Path(__file__).resolve().parents[2]


def atomic(path,payload):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=path.parent,prefix='.quant-')
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as f:json.dump(clean(payload),f,ensure_ascii=False,allow_nan=False,separators=(',',':'))
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)


def cached(root=ROOT):
    try:return json.loads((Path(root)/'data/quant/latest.json').read_text())
    except (OSError,ValueError):return dict(schema_version='1.0',quality='missing',markets=[],signals=[],missing_reason='量化模块尚未初始化')


def evidence(root=ROOT):
    try:return json.loads((Path(root)/'data/quant/evidence.json').read_text())
    except (OSError,ValueError):return dict(quality='missing',records=[],primary=[],missing_reason='冻结研究尚未复现')


def seed(store,directory):
    from .research import load_frozen,RESEARCH_VERSION
    if store.setting('seeded_research')==RESEARCH_VERSION:return
    for market,(prices,iv,fng) in load_frozen(directory).items():
        name='Sina-'+{'CYB':'sz399006','CSI300':'sh000300','HSTECH':'HSTECH'}.get(market,'') if market in ('CYB','CSI300','HSTECH') else 'Yahoo-'+('^HSI' if market=='HSI' else market+'-USD')
        meta=dict(source=name,data_hash='frozen-20261120',available_at=None,fetched_at=None,
                  adapter_version='frozen-import-1',availability_basis='frozen attachment; precise historical publication time unavailable')
        store.merge(market,'price',{str(d.date()):r.to_dict() for d,r in prices.iterrows()},meta)
        ivsource='Optbbs-QVIX' if market in ('CYB','CSI300') else 'Sina-VHSI' if market in ('HSI','HSTECH') else 'Deribit-'+market+'-DVOL'
        store.merge(market,'iv',{str(d.date()):dict(value=v) for d,v in iv.items()},dict(meta,source=ivsource))
        if fng is not None:store.merge('BTC','fng',{str(d.date()):dict(value=v) for d,v in fng.items() if str(d.date())<='2026-11-20'},dict(meta,source='Alternative.me-FNG'))
    store.setting('seeded_research',RESEARCH_VERSION)


def _quality(market,prices,expected):
    last=prices.index[-1].date()
    if last!=expected:return 'stale','最近完整交易日应为 '+str(expected)+'，价格仍为 '+str(last)
    # Verify recent complete-session coverage; do not silently compress a missing bar.
    if MARKETS[market]['year']==365:
        required=pd.date_range(prices.index[-1]-pd.Timedelta(days=40),prices.index[-1],freq='D')
        if any(day not in prices.index for day in required):return 'invalid','最近40个UTC日存在价格缺口'
    else:
        from .calendar import closed
        exchange=venue(market)
        required=pd.date_range(prices.index[-1]-pd.Timedelta(days=45),prices.index[-1],freq='D')
        if any(d.year>=2026 and not closed(exchange,d.date()) and d not in prices.index for d in required):return 'invalid','最近45日存在未解释的交易日价格缺口'
    return 'ok',None


def unavailable_signals(market,quality,reason,ev):
    names=signal_names(market)
    return [dict(market=market,signal_id=name,label=RULES[name],tier=FOCUS.get((market,name),'对照规则'),
                 state=None,new_event=None,quality=quality,missing_reason=reason,session_date=None,
                 formula_version=FORMULA_VERSION,research_version=ev.get('research_version'),current_values={},thresholds=[],
                 eligible_for_realtime_event=False,event_start_uncertain=True,
                 evidence=next((r for r in ev.get('primary',[]) if r['market']==market and r['signal_id']==name),None)) for name in names]


def run(root=ROOT,force=False,refresh=True,now=None):
    root=Path(root);now=now or datetime.now(timezone.utc);directory=root/'data/quant';directory.mkdir(parents=True,exist_ok=True)
    with (directory/'run.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return cached(root)
        store=Store(root);started=time.monotonic();run_id='quant-'+now.strftime('%Y%m%dT%H%M%S%fZ')
        legacy_launch=store.setting('launched_at') if FORMULA_VERSION=='quant-1.0.0' else None
        launched=datetime.fromisoformat(store.setting('launched_at:'+FORMULA_VERSION,legacy_launch or now.isoformat()))
        ev=evidence(root)
        if not store.setting('seeded_research'):
            if not (directory/'research_input/BTC_prices.csv').exists():return dict(quality='missing',markets=[],signals=[],missing_reason='冻结研究数据尚未安装')
            if not ev.get('audit',{}).get('original_replay_verified'):return dict(quality='invalid',markets=[],signals=[],missing_reason='冻结研究复跑尚未通过')
            seed(store,directory/'research_input')
        errors={}
        if refresh:
            for market in MARKETS:
                key='price:'+market;previous=store.feed(key)
                current=store.series(market,'price')
                try:expected=str(expected_session(market,now))
                except ValueError as exc:
                    errors[key]=dict(error_type='CalendarPending',reason=str(exc));continue
                age=now.timestamp()-previous.get('last_success_epoch',0)
                if not force and age<6*3600 and current and max(current)>=expected:continue
                try:
                    rows,meta=sources.price(store,market,now)
                    revisions=store.merge(market,'price',rows,meta)
                    store.feed(key,dict(meta,quality='ok',last_success_epoch=now.timestamp(),last_attempt_at=now.isoformat(),latest_valid_session=max(rows),row_count=len(rows),**revisions))
                except Exception as exc:
                    errors[key]=dict(error_type=type(exc).__name__,reason=str(exc)[:400])
                    store.feed(key,dict(previous,last_attempt_at=now.isoformat(),last_error=errors[key]))
            if force or now.timestamp()-float(store.setting('indicators_last_attempt') or 0)>=3600:
                results,failed=sources.indicators(store,now)
                for market,(rows,meta) in results.items():
                    target='BTC' if market=='fng' else market;kind='fng' if market=='fng' else 'iv'
                    revisions=store.merge(target,kind,rows,meta)
                    store.feed(kind+':'+target,dict(meta,quality='ok',latest_valid_session=max(rows) if rows else None,
                                                  row_count=len(rows),last_success_epoch=now.timestamp(),**revisions))
                for market,error in failed.items():
                    key='fng:BTC' if market=='fng' else 'iv:'+market;errors[key]=error
                    store.feed(key,dict(store.feed(key),last_attempt_at=now.isoformat(),last_error=error))
                store.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('indicators_last_attempt',str(now.timestamp())));store.db.commit()
        for market in MARKETS:
            for key in ('price:'+market,'iv:'+market):
                if store.feed(key).get('last_error'):errors.setdefault(key,store.feed(key)['last_error'])
        if store.feed('fng:BTC').get('last_error'):errors.setdefault('fng:BTC',store.feed('fng:BTC')['last_error'])
        # HTTP completion may be later than job start; never label observations future.
        completed=datetime.now(timezone.utc) if refresh else now
        markets=[];signals=[];changes=0
        for market,meta in MARKETS.items():
            market_launched=datetime.fromisoformat(store.setting('launched_at:'+FORMULA_VERSION+':'+market,completed.isoformat() if market in US_MARKETS else launched.isoformat()))
            try:expected=expected_session(market,completed)
            except ValueError as exc:
                markets.append(dict(market=market,name=meta['name'],quality='pending',missing_reason=str(exc)))
                signals.extend(unavailable_signals(market,'pending',str(exc),ev));continue
            prices,iv,fg=store.frames(market,expected)
            if prices is None:
                markets.append(dict(market=market,name=meta['name'],quality='missing',missing_reason='price series missing'))
                signals.extend(unavailable_signals(market,'missing','价格源缺失',ev));continue
            try:features,rules=compute(market,prices,iv,fg)
            except ValueError as exc:
                markets.append(dict(market=market,name=meta['name'],quality='invalid',missing_reason=str(exc)))
                signals.extend(unavailable_signals(market,'invalid',str(exc),ev));continue
            quality,reason=_quality(market,prices,expected)
            feed=store.feed('price:'+market)
            if 'source_conflict' in str(feed.get('last_error','')):quality,reason='source_conflict','供应商重叠区间价格存在冲突，暂停信号判定'
            day=str(features.index[-1].date());version=store.data_version(market)
            obs=observations(market,features,rules,completed,quality)
            price_points=store.series(market,'price')
            iv_points=store.series(market,'iv');fng_points=store.series(market,'fng') if market=='BTC' else {}
            lastiv=iv.dropna().loc[:features.index[-1]]
            latest_iv=str(lastiv.index[-1].date()) if len(lastiv) else None
            iv_lag_sessions=sum(str(d.date())>latest_iv for d in prices.index) if latest_iv else None
            iv_quality='ok' if latest_iv==day else 'pending' if iv_lag_sessions is not None and iv_lag_sessions<=1 else 'stale' if latest_iv else 'missing'
            if not meta.get('iv_applicable',True):iv_quality='not_applicable'
            # Recalculate every historical edge on revision, but never emit TG notices.
            if quality=='ok':changes+=store.update_events(market,prices,features,rules,completed,version,market_launched)
            for row in obs:
                if len(features)>1:
                    prior_day=str(features.index[-2].date())
                    prior=store.db.execute('SELECT payload FROM observations WHERE id=?',(f"{market}:{row['signal_id']}:{prior_day}:{FORMULA_VERSION}",)).fetchone()
                    prior_data=json.loads(prior[0]) if prior else None
                    unknown=(prior_data.get('state') is None) if prior_data is not None else bar_end(market,prior_day)>=market_launched
                    if unknown:
                        row.update(new_event=None,event_id=None,event_start_uncertain=True)
                    elif row.get('state') is True and prior_data and prior_data.get('state') is True and prior_data.get('event_start_uncertain'):
                        row['event_start_uncertain']=True
                points=[price_points.get(day,{})]
                if row['signal_id'] in ('IV90_DOWN','VHSI_GE30_DOWN'):points.append(iv_points.get(day,{}))
                if row['signal_id'].startswith('FNG_'):points.append(fng_points.get(day,{}))
                available=[p.get('available_at') for p in points if p.get('available_at')]
                row.update(data_version=version,research_version=ev.get('research_version'),
                           eligible_for_realtime_event=row.get('event_id') in store.inserted_event_ids,
                           available_at=max(available) if available else None,fetched_at=feed.get('fetched_at'),
                           evidence_context='历史证据与当前数据版本分开；换源重叠比较见 price_feed，前瞻另算')
                if row['signal_id'] in ('IV90_DOWN','VHSI_GE30_DOWN') and row['state'] is None and quality=='ok':
                    row['quality']=iv_quality if iv_quality!='ok' else 'insufficient_history'
                last=store.db.execute('SELECT day,first_seen_at FROM events WHERE market=? AND signal=? AND status=? ORDER BY day DESC LIMIT 1',(market,row['signal_id'],'active')).fetchone()
                row['last_event_at']=last['first_seen_at'] if last else None
                row['last_event_session']=last['day'] if last else None
                relevant=[r for r in ev.get('primary',[]) if r['market']==market and r['signal_id']==row['signal_id']]
                row['evidence']=relevant[0] if relevant else None
                key=f"{market}:{row['signal_id']}:{day}:{FORMULA_VERSION}"
                if day==str(expected):
                    store.db.execute('INSERT OR REPLACE INTO observations VALUES(?,?)',(key,json.dumps(clean(row),ensure_ascii=False,allow_nan=False)))
            signals.extend(obs)
            feature=clean(features.iloc[-1].to_dict());year=meta['year']
            price_point=price_points.get(day,{})
            coverage=next((r for r in ev.get('audit',{}).get('us_extension',{}).get('markets',[]) if r['market']==market),{})
            markets.append(dict(market=market,**meta,quality=quality,missing_reason=reason,expected_session_date=str(expected),session_date=day,launched_at=market_launched.isoformat(),
                research_coverage={k:coverage.get(k) for k in ('price_start','price_end','iv_start','iv_end','price_bars','iv_bars')} if coverage else None,
                bar_end_at=bar_end(market,day).isoformat(),available_at=price_point.get('available_at'),fetched_at=price_point.get('fetched_at'),
                price=feature['close'],price_source=price_point.get('source'),price_adjustment='unadjusted OHLC/price index; excludes dividends',
                features=feature,iv_last_value=float(lastiv.iloc[-1]) if len(lastiv) else None,iv_last_session=latest_iv,
                iv_quality=iv_quality,iv_lag_sessions=iv_lag_sessions,
                iv_quality_policy='same completed price session: ok; one observed trading session behind: pending; more: stale',
                chart=[dict(date=str(d.date()),close=float(p.close)) for d,p in prices.tail(180).iterrows()],
                iv_source=store.feed('iv:'+market),price_feed=feed,formula_version=FORMULA_VERSION,data_version=version,
                windows=dict(price_start=str(prices.index[0].date()),price_end=day,price_bars=len(prices),
                    iv_window_start=str(features.index[max(0,len(features)-year-1)].date()),iv_window_end=str(features.index[-2].date()) if len(features)>1 else None,
                    required_year_bars=year,iv_valid_count=feature['iv_window_count'],iv_coverage=feature['iv_window_count']/year if feature['iv_window_count'] is not None else None,
                    rv_valid_count=feature['rv_window_count'],quantile_excludes_current=True,percentile_tie_policy='prior valid observations <= current / prior valid count')))
        store.db.commit()
        history={market:store.events(market,100000) for market in MARKETS}
        for market,events in history.items():atomic(directory/'events'/f'{market}.json',dict(market=market,events=events))
        result=clean(dict(schema_version='1.0',snapshot_id=run_id,generated_at=completed.isoformat(),timezone='UTC',
            formula_version=FORMULA_VERSION,research_version=ev.get('research_version'),research_cutoff='2026-11-20',launched_at=launched.isoformat(),
            markets=markets,signals=signals,quality='ok' if all(m['quality']=='ok' and m.get('iv_quality') in ('ok','not_applicable') for m in markets) else 'partial',
            source_errors=errors,forward=store.forward_summary(),duration_seconds=time.monotonic()-started,event_changes=changes,
            notes=['研究监控，不下单、不自动配仓；半年胜率不是明日或下周上涨概率',
                   'RV为实际波动率，不是隐波/VIX；恒科VHSI为跨指数参考；QVIX为第三方ETF隐波',
                   '历史回补不算上线后前瞻；未到期不计完成胜率；MAE为相对进入价最坏浮亏',
                   '美股SPX配VIX、NDX配VXN；SOX只用自身RV；分别独立回测，不借用其他市场胜率',
                   'VXN此次官方CSV起于2009-09-14，不能检验互联网泡沫和2008危机；美股按纽约完整交易日/夏令时/半日市',
                   '原始供应商发布时间未知时 available_at 是首次HTTP观测时间，不是事先可用时间']))
        store.db.execute('INSERT OR REPLACE INTO snapshots VALUES(?,?)',(run_id,json.dumps(result,ensure_ascii=False,allow_nan=False)));store.db.commit()
        atomic(directory/'latest.json',result);store.db.close()
        LOG.info('quant run_id=%s quality_status=%s duration=%.3f event_changes=%s',run_id,result['quality'],result['duration_seconds'],changes)
        return result
