"""Isolated public feed adapters with raw response hashes and explicit switches."""
import hashlib,io,json,logging,re,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
from urllib.parse import quote
import numpy as np
import pandas as pd
import requests
from .engine import MARKETS,US_MARKETS,bar_end,expected_session,clean,venue

LOG=logging.getLogger(__name__)
ADAPTER_VERSION='quant-sources-1.1.0'


def response(store,key,url,params=None):
    start=time.monotonic()
    r=requests.get(url,params=params,headers={'User-Agent':'Trade-db-research/1.0','Referer':'https://finance.sina.com.cn'},timeout=15)
    r.raise_for_status()
    fetched=datetime.now(timezone.utc).isoformat();digest=hashlib.sha256(r.content).hexdigest()
    directory=store.root/'raw'/key;directory.mkdir(parents=True,exist_ok=True)
    path=directory/(digest+'.bin')
    if not path.exists():path.write_bytes(r.content)
    meta=dict(source=key,adapter_version=ADAPTER_VERSION,fetched_at=fetched,available_at=fetched,
              availability_basis='first observed at HTTP completion; publisher release time unavailable',
              raw_response_ref='sha256:'+digest,data_hash=digest,endpoint=url.split('?')[0])
    LOG.info('quant source=%s duration=%.3f quality_status=received raw_sha256=%s',key,time.monotonic()-start,digest)
    return r,meta


def normalize_prices(rows,market,now):
    expected=expected_session(market,now);result={}
    for day,bar in rows.items():
        day=str(day)[:10]
        if day>str(expected) or bar_end(market,day)>now:continue
        if MARKETS[market]['year']==252 and day>='2026-01-01':
            from .calendar import closed
            if closed(venue(market),pd.Timestamp(day).date()):continue
        try:
            item={k:float(bar[k]) for k in ('open','high','low','close')}
            if not all(np.isfinite(v) and v>0 for v in item.values()):continue
            tolerance=max(item.values())*1e-6
            if item['high']+tolerance<max(item.values()) or item['low']-tolerance>min(item.values()):continue
        except (KeyError,TypeError,ValueError):continue
        result[day]=item
    if not result:raise ValueError('no completed valid OHLC bars')
    return result


def sina(store,market,now):
    symbol={'CYB':'sz399006','CSI300':'sh000300','HSTECH':'HSTECH','HSI':'HSI','VHSI':'VHSI'}[market]
    hk=market in ('HSI','HSTECH','VHSI')
    url=f'https://finance.sina.com.cn/stock/hkstock/{symbol}/klc2_kl.js' if hk else f'https://finance.sina.com.cn/realstock/company/{symbol}/hisdata/klc_kl.js'
    r,meta=response(store,'Sina-'+symbol,url)
    encoded=re.search(r'=\s*"([^"]+)"',r.text)
    if not encoded:raise ValueError('encoded Sina daily series missing')
    from py_mini_racer import MiniRacer
    from akshare.stock.cons import hk_js_decode
    ctx=MiniRacer();ctx.eval(hk_js_decode);decoded=ctx.call('d',encoded.group(1))
    rows={str(b['date'])[:10]:b for b in decoded}
    target='HSI' if market=='VHSI' else market
    rows=normalize_prices(rows,target,now)
    lower='2010-06-01' if market=='CYB' else '2005-04-08' if market=='CSI300' else '2021-03-18' if market=='VHSI' else '1900-01-01'
    return {k:v for k,v in rows.items() if k>=lower},meta


def yahoo(store,market,now,history=False):
    symbol=MARKETS[market]['symbol'] if market in US_MARKETS else '^HSI' if market=='HSI' else market+'-USD'
    params=dict(period1=631152000,period2=int(now.timestamp()),interval='1d') if history or (market in US_MARKETS and not store.series(market,'price')) else dict(range='2y',interval='1d')
    r,meta=response(store,'Yahoo-'+symbol,'https://query1.finance.yahoo.com/v8/finance/chart/'+quote(symbol,safe=''),params)
    data=r.json()['chart']['result'][0]
    if market in US_MARKETS and (data['meta'].get('symbol')!=symbol or data['meta'].get('exchangeTimezoneName')!='America/New_York'):
        raise ValueError('US index symbol/timezone conflict; no ETF substitution')
    if data.get('meta',{}).get('currency') not in (None,'HKD' if market=='HSI' else 'USD'):raise ValueError('quote currency conflict')
    q=data['indicators']['quote'][0];zone=MARKETS[market]['zone']
    from zoneinfo import ZoneInfo
    rows={datetime.fromtimestamp(stamp,ZoneInfo(zone)).date().isoformat():{key:q[key][i] for key in ('open','high','low','close')}
          for i,stamp in enumerate(data['timestamp'])}
    return normalize_prices(rows,market,now),meta


def coinbase(store,market,now):
    r,meta=response(store,'Coinbase-'+market+'-USD',f'https://api.exchange.coinbase.com/products/{market}-USD/candles',dict(granularity=86400))
    rows={datetime.fromtimestamp(b[0],timezone.utc).date().isoformat():dict(low=b[1],high=b[2],open=b[3],close=b[4]) for b in r.json()}
    return normalize_prices(rows,market,now),meta


def price(store,market,now):
    adapters=[yahoo] if market in US_MARKETS else [sina] if market in ('CYB','CSI300','HSTECH') else [yahoo,sina] if market=='HSI' else [yahoo,coinbase]
    failures=[]
    for adapter in adapters:
        try:
            rows,meta=adapter(store,market,now)
            if max(rows)<str(expected_session(market,now)):
                raise ValueError('pending: provider has not published latest completed session')
            old=store.series(market,'price');overlap=sorted(set(old)&set(rows))[-60:]
            differences=[abs(rows[d]['close']/old[d]['close']-1) for d in overlap]
            previous_source=old[max(old)].get('source') if old else None
            changed_source=bool(previous_source and previous_source!=meta['source'])
            # Frozen seed source names deliberately match the normal adapter.
            if changed_source and (len(overlap)<10 or max(differences,default=1)>.02):
                raise ValueError('source_conflict: fewer than 10 overlap sessions or max close deviation >2%')
            meta.update(source_switch=changed_source,previous_source=previous_source,
                        overlap_comparison=dict(sample_count=len(overlap),start=overlap[0] if overlap else None,end=overlap[-1] if overlap else None,
                                                max_relative_close_difference=max(differences,default=None)),attempt_failures=failures)
            if changed_source:LOG.warning('quant source switch market=%s previous=%s current=%s',market,previous_source,meta['source'])
            return rows,meta
        except Exception as exc:
            failures.append(dict(adapter=adapter.__name__,error_type=type(exc).__name__,reason=str(exc)[:160]))
    raise ValueError(json.dumps(failures,ensure_ascii=False))


def cboe_iv(store,market,now):
    symbol={'SPX':'VIX','NDX':'VXN'}[market]
    r,meta=response(store,'Cboe-'+symbol,f'https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv')
    data=pd.read_csv(io.BytesIO(r.content))
    if not {'DATE','CLOSE'}<=set(data.columns):raise ValueError('Cboe daily CSV schema changed')
    days=pd.to_datetime(data.DATE,format='%m/%d/%Y',errors='coerce')
    values=pd.to_numeric(data.CLOSE,errors='coerce')
    if days.duplicated().any():raise ValueError('duplicate Cboe daily dates')
    expected=str(expected_session(market,now))
    rows={str(d.date()):dict(value=float(v)) for d,v in zip(days,values) if pd.notna(d) and pd.notna(v) and np.isfinite(v) and v>0 and str(d.date())<=expected}
    if not rows:raise ValueError('Cboe IV has no completed valid sessions')
    meta.update(index_symbol=symbol,underlying=MARKETS[market]['symbol'],unit='annualized volatility percent',
                session_scope='Cboe daily close; distinct from equity close',first_session=min(rows))
    return rows,meta


def us_iv(store,market,now):
    # Official historical-download endpoint, not scraping the restricted quote dashboard.
    return cboe_iv(store,market,now)


def indicators(store,now):
    results={};errors={}
    try:
        r,meta=response(store,'Optbbs-QVIX','https://1.optbbs.com/d/csv/d/k.csv')
        q=pd.read_csv(io.StringIO(r.content.decode('gbk')))
        if len(q.columns)<75 or str(q.columns[12])!='13' or str(q.columns[74])!='75':
            # Frozen file header is 2..74 after its leading date field; validate below.
            expected=pd.read_csv(store.root/'research_input/raw/qvix.txt',encoding='gbk',nrows=0).columns
            if list(q.columns[:75])!=list(expected[:75]):raise ValueError('QVIX column identity changed')
        index=pd.to_datetime(q.iloc[:,0],errors='coerce')
        for market,col in (('CSI300',12),('CYB',74)):
            lower='2019-12-23' if market=='CSI300' else '2022-09-19'
            rows={str(day.date()):dict(value=float(v) if pd.notna(v) and float(v)>0 else None)
                  for day,v in zip(index,pd.to_numeric(q.iloc[:,col],errors='coerce'))
                  if pd.notna(day) and lower<=str(day.date())<=str(expected_session(market,now))}
            results[market]=(rows,meta)
    except Exception as exc:
        for market in ('CSI300','CYB'):errors[market]=dict(error_type=type(exc).__name__,reason=str(exc)[:150])
    try:
        rows,meta=sina(store,'VHSI',now)
        for market in ('HSI','HSTECH'):results[market]=({k:dict(value=v['close']) for k,v in rows.items()},meta)
    except Exception as exc:
        for market in ('HSI','HSTECH'):errors[market]=dict(error_type=type(exc).__name__,reason=str(exc)[:150])
    for market in ('SPX','NDX'):
        try:results[market]=us_iv(store,market,now)
        except Exception as exc:errors[market]=dict(error_type=type(exc).__name__,reason=str(exc)[:150])
    for market in ('BTC','ETH'):
        try:
            r,meta=response(store,'Deribit-'+market+'-DVOL','https://www.deribit.com/api/v2/public/get_volatility_index_data',
                 dict(currency=market,start_timestamp=int((now-timedelta(days=90)).timestamp()*1000),end_timestamp=int(now.timestamp()*1000),resolution='1D'))
            rows={datetime.fromtimestamp(b[0]/1000,timezone.utc).date().isoformat():dict(value=float(b[4])) for b in r.json()['result']['data']
                  if datetime.fromtimestamp(b[0]/1000,timezone.utc).date()<=expected_session(market,now) and float(b[4])>0}
            if not rows:raise ValueError('DVOL completed daily bars empty')
            results[market]=(rows,meta)
        except Exception as exc:errors[market]=dict(error_type=type(exc).__name__,reason=str(exc)[:150])
    try:
        r,meta=response(store,'Alternative.me-FNG','https://api.alternative.me/fng/',dict(limit=90,format='json'))
        rows={(datetime.fromtimestamp(int(b['timestamp']),timezone.utc).date()+timedelta(days=1)).isoformat():dict(value=float(b['value'])) for b in r.json()['data']}
        rows={k:v for k,v in rows.items() if k<=str(expected_session('BTC',now)) and 0<=v['value']<=100}
        results['fng']=(rows,dict(meta,publication_lag='one full UTC day, frozen research policy'))
    except Exception as exc:errors['fng']=dict(error_type=type(exc).__name__,reason=str(exc)[:150])
    return results,errors
