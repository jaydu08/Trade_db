"""Independent US extension of the immutable six-market study; fixed cutoff/rules."""
import copy,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import numpy as np
import pandas as pd
from .engine import US_MARKETS,FORMULA_VERSION,clean,compute,edges,outcome,wilson
from .research import RESEARCH_VERSION as BASE_VERSION
from .runtime import atomic
from .store import Store
from .sources import yahoo,cboe_iv

US_RESEARCH_VERSION='us-20261120-r1'
BUNDLE_VERSION='frozen-20261120-us-extension-r2'
CUTOFF='2026-11-20'


def digest(value):
    return hashlib.sha256(json.dumps(clean(value),sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def verify_raw(root,inputs):
    raw=Path(root)/'data/quant/raw'
    for data in inputs['markets'].values():
        for key in ('price_source','iv_source'):
            meta=data[key]
            if not meta:continue
            path=raw/meta['source']/(meta['data_hash']+'.bin')
            if not path.resolve().is_relative_to(raw.resolve()) or not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest()!=meta['data_hash']:
                raise ValueError('US frozen raw response missing or hash mismatch')


def collect(root,now):
    root=Path(root);path=root/'data/quant/us_research_input/frozen-20261120.json'
    if path.exists():return json.loads(path.read_text())
    store=Store(root)
    try:
        data={}
        for market in US_MARKETS:
            prices,pm=yahoo(store,market,now,history=True)
            iv,im=cboe_iv(store,market,now) if market!='SOX' else ({},{})
            data[market]=dict(prices={d:v for d,v in prices.items() if d<=CUTOFF},iv={d:v for d,v in iv.items() if d<=CUTOFF},price_source=pm,iv_source=im)
        result=dict(cutoff=CUTOFF,markets=data)
        atomic(path,result)
        return result
    finally:store.db.close()


def evaluate(market,prices,iv,data_version):
    f,rules=compute(market,prices,iv);records=[];all_events=[]
    for signal,(condition,valid) in rules.items():
        candidates=np.flatnonzero(edges(condition,valid))
        for cooldown in (63,126):
            selected=[];last=-100000
            for i in candidates:
                if i-last>=cooldown+1:selected.append(int(i));last=i
            for h in (21,63,126):
                events=[]
                for i in selected:
                    o=outcome(prices,i,h,.002)
                    if o:
                        o.update(market=market,signal_id=signal,holding_period=h,sampling_interval=cooldown+1,
                                 formula_version=FORMULA_VERSION,research_version=US_RESEARCH_VERSION,origin='historical_backtest')
                        events.append(o);all_events.append(o)
                forward=prices.close.shift(-h)/prices.open.shift(-1)-1-.002
                for period in ('all','pre2023','2023plus'):
                    completed=[o for o in events if period=='all' or (o['exit_date']<'2023-01-01' if period=='pre2023' else o['signal_date']>='2023-01-01')]
                    bv=valid.copy()
                    if period=='pre2023':bv &= pd.Series(prices.index,index=prices.index).shift(-h)<pd.Timestamp('2023-01-01')
                    elif period=='2023plus':bv &= prices.index>=pd.Timestamp('2023-01-01')
                    def baseline(mask):
                        values=forward[mask].dropna()
                        return dict(win_rate_pct=float((values>0).mean()*100) if len(values) else None,
                                    mean_return_pct=float(values.mean()*100) if len(values) else None,
                                    observation_count=len(values),overlapping_windows=True)
                    values=np.array([o['net_return'] for o in completed]);n=len(values);wins=int((values>0).sum())
                    matched=baseline(bv&(f.ret_20<0)) if signal in ('IV90_DOWN','RV90_DOWN') else baseline(bv&(prices.close>f.ma_200)) if signal in ('BREAK20_ABOVE_MA200','RECOVER_MA200') else None
                    records.append(clean(dict(research_version=US_RESEARCH_VERSION,formula_version=FORMULA_VERSION,data_version=data_version,
                        market=market,signal_id=signal,period=period,holding_period=h,holding_unit='observed trading bars',
                        sampling_interval=cooldown+1,cost_assumption=.002,completed_events=n,wins=wins,win_rate_pct=wins/n*100 if n else None,
                        confidence_interval=wilson(wins,n),mean_return_pct=float(values.mean()*100) if n else None,
                        median_return_pct=float(np.median(values)*100) if n else None,worst_terminal_return_pct=float(values.min()*100) if n else None,
                        worst_mae_pct=min(o['mae'] for o in completed)*100 if n else None,unconditional_baseline=baseline(bv),matched_state_baseline=matched,
                        sample_start=min((o['signal_date'] for o in completed),default=None),sample_end=max((o['signal_date'] for o in completed),default=None),
                        data_cutoff=CUTOFF,feature_start=str(prices.index[0].date()),iv_start=str(iv.index.min().date()) if len(iv) else None,
                        overlapping_windows=h>cooldown,validation_type='historical_full_sample' if period=='all' else 'historical_subperiod',
                        missing_reason='No completed events for this period/horizon' if not n else None)))
    return records,all_events


def extend(base,base_events,inputs):
    if base.get('research_version')!=BASE_VERSION:raise ValueError('US extension requires unchanged original six-market evidence')
    if inputs.get('cutoff')!=CUTOFF:raise ValueError('US cutoff mismatch')
    records=[];events=[];audit=[]
    for market in US_MARKETS:
        data=inputs['markets'][market]
        if not data['prices'] or max(data['prices'])>CUTOFF or max(data['iv'],default=CUTOFF)>CUTOFF:raise ValueError('US frozen inputs contain future/empty data')
        p=pd.DataFrame(data['prices']).T.sort_index().astype(float);p.index=pd.to_datetime(p.index)
        iv=pd.Series({pd.Timestamp(d):v['value'] for d,v in data['iv'].items()},dtype=float).sort_index()
        if len(p)<1000 or (market!='SOX' and len(iv)<1000):raise ValueError('US research history too short; do not publish short-sample substitute')
        if market=='SOX' and len(iv):raise ValueError('SOX must not inherit VIX/VXN')
        version=digest(data);rows,ev=evaluate(market,p,iv,version);records.extend(rows);events.extend(ev)
        # Independent vectorized checks on the real fixed inputs, including prefix stability.
        f,rules=compute(market,p,iv);prefix,prules=compute(market,p.iloc[:-30],iv.loc[:p.index[-31]])
        pd.testing.assert_frame_equal(f.iloc[:-30],prefix)
        for name in rules:
            pd.testing.assert_series_equal(edges(*rules[name]).iloc[:-30],edges(*prules[name]))
        pd.testing.assert_series_equal(f.ret_20,p.close/p.close.shift(20)-1,check_names=False)
        pd.testing.assert_series_equal(f.rv_20,np.log(p.close).diff().rolling(20).std(ddof=1)*np.sqrt(252)*100,check_names=False)
        audit.append(dict(market=market,data_version=version,price_start=str(p.index[0].date()),price_end=str(p.index[-1].date()),price_bars=len(p),
                          iv_start=str(iv.index[0].date()) if len(iv) else None,iv_end=str(iv.index[-1].date()) if len(iv) else None,
                          iv_bars=len(iv),independent_feature_checks=True,prefix_no_lookahead_verified=True,
                          price_source=data['price_source'],iv_source=data['iv_source'],
                          price_adjustment='price index OHLC, excludes dividends; not ETF/total return',historical_calendar_basis='vendor observed completed daily bars; verified US calendar from 2026'))
    result=copy.deepcopy(base)
    result.update(research_version=BUNDLE_VERSION,data_version=digest(dict(base=base['data_version'],us=inputs)),
                  previous_research_version=BASE_VERSION,us_research_version=US_RESEARCH_VERSION,
                  records=base['records']+records,primary=base['primary']+[r for r in records if r['period']=='all' and r['holding_period']==126 and r['sampling_interval']==127],
                  event_count_across_variants=base['event_count_across_variants']+len(events))
    result['audit']['us_extension']=dict(cutoff=CUTOFF,rule_selection='prespecified before outcomes: seven common rules for SPX/NDX, six non-IV rules for SOX',
        markets=audit,base_records_sha256=digest(base['records']),base_records_unchanged=True)
    result['limitations']=[s for s in base['limitations'] if not s.startswith('46 rule/market')]+[
        '66 prespecified rule/market comparisons; no multiple-comparison adjustment or out-of-sample validation',
        'VXN official downloaded history starts 2009-09-14; IV signals cannot test the dot-com crash or 2008',
        'SOX RV is realized volatility of its own price index; no VIX win rate, IV proxy or ETF return is substituted',
        'US extension was added after the original study; its frozen historical results are NOT prospective evidence']
    return clean(result),base_events+events


def install(root,now=None):
    root=Path(root);directory=root/'data/quant';now=now or datetime.now(timezone.utc)
    # Same lock as the collector and publisher: no mixed evidence/event versions.
    import fcntl
    with (directory/'run.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        current=json.loads((directory/'evidence.json').read_text());backtest=json.loads((directory/'backtest_events.json').read_text())
        archive=directory/'evidence_versions';archive.mkdir(exist_ok=True)
        if current['research_version']==BASE_VERSION:
            atomic(archive/(BASE_VERSION+'.json'),current)
            atomic(archive/(BASE_VERSION+'-events.json'),backtest)
        base=json.loads((archive/(BASE_VERSION+'.json')).read_text())
        old_events=json.loads((archive/(BASE_VERSION+'-events.json')).read_text())['events']
        inputs=collect(root,now);verify_raw(root,inputs)
        result,events=extend(base,old_events,inputs)
        prior=archive/(BUNDLE_VERSION+'.json')
        if prior.exists() and json.loads(prior.read_text())!=result:raise ValueError('US immutable research version changed; requires a new version')
        atomic(prior,result)
        store=Store(root)
        try:
            for market,data in inputs['markets'].items():
                if not store.setting('us_research_seed:'+market):
                    store.merge(market,'price',data['prices'],data['price_source'])
                    if data['iv']:store.merge(market,'iv',data['iv'],data['iv_source'])
                    store.setting('us_research_seed:'+market,US_RESEARCH_VERSION)
        finally:store.db.close()
        atomic(directory/'backtest_events.json',dict(research_version=BUNDLE_VERSION,events=events))
        atomic(directory/'evidence.json',result)
        return result
