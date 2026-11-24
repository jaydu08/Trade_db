"""Frozen research replay and independently versioned evidence, not trading advice."""
import hashlib,json
from pathlib import Path
import numpy as np
import pandas as pd
from .engine import compute,edges,outcome,wilson,MARKETS,FORMULA_VERSION,clean

RESEARCH_VERSION='frozen-20261120-audit-r1'


def load_frozen(directory):
    root=Path(directory);result={}
    fear=pd.DataFrame(json.loads((root/'raw/feargreed.json').read_text())['data'])
    fear.index=pd.to_datetime(pd.to_numeric(fear.timestamp),unit='s').dt.normalize()+pd.Timedelta(days=1)
    fear=pd.Series(pd.to_numeric(fear.value).to_numpy(),index=fear.index).sort_index().loc[:'2026-11-20']
    for market in MARKETS:
        prices=pd.read_csv(root/f'{market}_prices.csv',index_col=0,parse_dates=True).loc[:'2026-11-20']
        features=pd.read_csv(root/f'{market}_features.csv',index_col=0,parse_dates=True).loc[:'2026-11-20']
        result[market]=(prices,features.iv.dropna(),fear if market=='BTC' else None)
    return result


def evidence(directory,replay_directory=None):
    root=Path(directory);sources=json.loads((root/'sources.json').read_text());checks=[]
    for name,expected in sources['raw_sha256'].items():
        path=root/'raw'/name
        actual=hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
        checks.append(dict(file=name,matched=actual==expected,actual=actual))
    if not all(r['matched'] for r in checks):raise ValueError('frozen raw hashes differ from sources.json')
    original=pd.read_csv(root/'results_all.csv')
    replay_verified=False
    if replay_directory:
        replay=pd.read_csv(Path(replay_directory)/'results_all.csv')
        pd.testing.assert_frame_equal(original,replay,check_exact=False,rtol=1e-9,atol=1e-9)
        for market in MARKETS:
            for suffix in ('prices','features'):
                given=pd.read_csv(root/f'{market}_{suffix}.csv',index_col=0,parse_dates=True)
                rebuilt=pd.read_csv(Path(replay_directory)/f'{market}_{suffix}.csv',index_col=0,parse_dates=True)
                pd.testing.assert_frame_equal(given,rebuilt,check_exact=False,rtol=1e-9,atol=1e-9)
        replay_verified=True
    data=load_frozen(root);summaries=[];all_events=[];diffs=[];precision=[]
    for market,(prices,iv,fg) in data.items():
        bad=(prices.high<prices[['open','close','low']].max(axis=1))|(prices.low>prices[['open','close','high']].min(axis=1))
        if bad.any():precision.append(dict(market=market,rows=int(bad.sum()),tolerance='relative 1e-6 OHLC rounding envelope',prices_modified=False))
        features,rules=compute(market,prices,iv,fg)
        crypto=MARKETS[market]['year']==365;hs=[30,90,180] if crypto else [21,63,126];cost=.004 if crypto else .002
        for signal,(condition,valid) in rules.items():
            candidates=np.flatnonzero(edges(condition,valid))
            for cooldown in hs[1:]:
                selected=[];last=-100000
                for i in candidates:
                    if i-last>=cooldown+1:selected.append(int(i));last=i
                for h in hs:
                    events=[]
                    for i in selected:
                        o=outcome(prices,i,h,cost)
                        if o:
                            o.update(market=market,signal_id=signal,holding_period=h,sampling_interval=cooldown+1,formula_version=FORMULA_VERSION,origin='historical_backtest')
                            events.append(o);all_events.append(o)
                    forward=prices.close.shift(-h)/prices.open.shift(-1)-1-cost
                    for period in ('all','pre2023','2023plus'):
                        completed=[o for o in events if period=='all' or (o['exit_date']<'2023-01-01' if period=='pre2023' else o['signal_date']>='2023-01-01')]
                        if not completed:continue
                        bv=valid.copy()
                        if period=='pre2023':bv &= pd.Series(prices.index,index=prices.index).shift(-h)<pd.Timestamp('2023-01-01')
                        elif period=='2023plus':bv &= prices.index>=pd.Timestamp('2023-01-01')
                        values=np.array([o['net_return'] for o in completed]);n=len(values);wins=int((values>0).sum())
                        baseline=lambda mask:dict(win_rate_pct=float((forward[mask].dropna()>0).mean()*100) if len(forward[mask].dropna()) else None,
                                                   mean_return_pct=float(forward[mask].dropna().mean()*100),overlapping_windows=True)
                        falling=signal in ('IV90_DOWN','RV90_DOWN','VHSI_GE30_DOWN')
                        summaries.append(clean(dict(research_version=RESEARCH_VERSION,formula_version=FORMULA_VERSION,
                            market=market,signal_id=signal,period=period,holding_period=h,holding_unit='UTC days' if crypto else 'observed trading bars',
                            sampling_interval=cooldown+1,cost_assumption=cost,completed_events=n,wins=wins,win_rate_pct=wins/n*100,
                            confidence_interval=wilson(wins,n),mean_return_pct=values.mean()*100,median_return_pct=np.median(values)*100,
                            worst_terminal_return_pct=values.min()*100,worst_mae_pct=min(o['mae'] for o in completed)*100,
                            unconditional_baseline=baseline(bv),matched_state_baseline=baseline(bv&(features.ret_20<0)) if falling else
                               baseline(bv&(prices.close>features.ma_200)) if signal in ('BREAK20_ABOVE_MA200','RECOVER_MA200') else None,
                            sample_start=min(o['signal_date'] for o in completed),sample_end=max(o['signal_date'] for o in completed),
                            data_cutoff='2026-11-20',overlapping_windows=h>cooldown,
                            validation_type='historical_full_sample' if period=='all' else 'historical_subperiod')))
        # Formula audit uses the same original price/IV inputs, not target fitting.
        orig=pd.read_csv(root/f'{market}_features.csv',index_col=0,parse_dates=True)
        mapping={'iv_q90_1y':'iv90_cutoff','rv_q90_1y':'rv90_cutoff','rv_20':'rv20','rsi_14':'rsi14','ret_20':'ret20','drawdown_1y':'dd_year','ma_20':'ma20','ma_200':'ma200'}
        for key,oldkey in mapping.items():
            a=features[key];b=orig[oldkey].reindex(a.index)
            changed=~(np.isclose(a,b,rtol=1e-8,atol=1e-8,equal_nan=True))
            if changed.any():diffs.append(dict(market=market,feature=key,changed_rows=int(changed.sum()),reason='ceil(95% coverage), full 365-day span; no target fitting' if key=='iv_q90_1y' else 'requires inspection'))
    primary=[r for r in summaries if r['period']=='all' and r['holding_period'] in (126,180) and r['sampling_interval']==r['holding_period']+1]
    if any(d['feature']!='iv_q90_1y' for d in diffs):raise ValueError('unexplained frozen feature differences; evidence publication refused')
    summary_diffs=[]
    for row in primary:
        old=original[(original.market==row['market'])&(original.signal==row['signal_id'])&(original.period=='all')&
                     (original.horizon==row['holding_period'])&(original.cooldown==row['holding_period'])]
        if len(old) and (int(old.iloc[0]['n'])!=row['completed_events'] or int(old.iloc[0]['wins'])!=row['wins']):
            summary_diffs.append(dict(market=row['market'],signal_id=row['signal_id'],old_n=int(old.iloc[0]['n']),old_wins=int(old.iloc[0]['wins']),new_n=row['completed_events'],new_wins=row['wins']))
    version=hashlib.sha256(json.dumps(checks,sort_keys=True).encode()).hexdigest()
    for row in summaries:row['data_version']=version
    return clean(dict(schema_version='1.0',research_version=RESEARCH_VERSION,formula_version=FORMULA_VERSION,data_version=version,
        cutoff='2026-11-20',audit=dict(original_replay_verified=replay_verified,raw_hash_checks=checks,formula_differences=diffs,primary_result_differences=summary_diffs,ohlc_precision=precision,
        corrections=['int(year*0.95) rounded down below 95%; production uses ceil', 'IV threshold additionally requires 365 calendar days since first valid IV'],
        licensing='Research attachments retained privately. AKShare decoder supplied by installed MIT-licensed dependency; original data redistribution rights not assumed.'),
        records=summaries,primary=primary,event_count_across_variants=len(all_events),
        limitations=['46 rule/market comparisons; confidence intervals not corrected for selection/multiple comparisons',
                     'Small and cycle-dependent samples; no stable >70% system verified',
                     'Price indexes exclude dividends; simulated friction excludes tax/leverage/tracking/idle capital',
                     'Old history uses vendor observed sessions; raw option-chain IV not reconstructed',
                     'Historical subperiod checks are NOT prospective validation'])),clean(all_events)
