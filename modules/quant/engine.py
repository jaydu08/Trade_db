"""One formula implementation for research, API, UI and Agent exports."""
import hashlib
import json
import math
from datetime import datetime,date,time,timedelta,timezone
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

FORMULA_VERSION='quant-1.0.0'
CATALOG_VERSION='quant-catalog-1.1.0'
FROZEN_MARKETS=('CYB','CSI300','HSI','HSTECH','BTC','ETH')
US_MARKETS=('SPX','NDX','SOX')
MARKETS={
 'CYB':dict(name='创业板指',symbol='399006',kind='PRICE_INDEX',year=252,zone='Asia/Shanghai',iv_name='创业板ETF QVIX（第三方，ETF/指数有跟踪差异）',source='Sina'),
 'CSI300':dict(name='沪深300',symbol='000300',kind='PRICE_INDEX',year=252,zone='Asia/Shanghai',iv_name='300ETF QVIX（第三方，ETF/指数有跟踪差异）',source='Sina'),
 'HSI':dict(name='恒生指数',symbol='HSI',kind='PRICE_INDEX',year=252,zone='Asia/Hong_Kong',iv_name='VHSI（恒指期权）',source='Yahoo'),
 'HSTECH':dict(name='恒生科技',symbol='HSTECH',kind='PRICE_INDEX',year=252,zone='Asia/Hong_Kong',iv_name='VHSI（恒指隐波，恒科跨指数参考）',source='Sina'),
 'BTC':dict(name='比特币',symbol='BTC-USD',kind='SPOT_USD',year=365,zone='UTC',iv_name='Deribit BTC DVOL',source='Yahoo'),
 'ETH':dict(name='以太坊',symbol='ETH-USD',kind='SPOT_USD',year=365,zone='UTC',iv_name='Deribit ETH DVOL',source='Yahoo'),
 'SPX':dict(name='标普500',symbol='^GSPC',kind='PRICE_INDEX',year=252,zone='America/New_York',iv_name='VIX（标普500期权隐波）',source='Yahoo',venue='US',iv_applicable=True),
 'NDX':dict(name='纳斯达克100',symbol='^NDX',kind='PRICE_INDEX',year=252,zone='America/New_York',iv_name='VXN（纳斯达克100期权隐波）',source='Yahoo',venue='US',iv_applicable=True),
 'SOX':dict(name='费城半导体',symbol='^SOX',kind='PRICE_INDEX',year=252,zone='America/New_York',iv_name='不接入隐波；独立使用SOX自身RV',source='Yahoo',venue='US',iv_applicable=False)}
RULES={
 'IV90_DOWN':'高隐波且20日下跌','RV90_DOWN':'高实际波动且20日下跌',
 'RSI_BELOW30':'RSI首次跌破30','RSI_RECOVER30':'RSI上穿30',
 'DD20_RECOVER_MA20':'年内回撤≥20%后上穿MA20','RECOVER_MA200':'收盘上穿MA200',
 'BREAK20_ABOVE_MA200':'MA200上方突破前20日收盘高点',
 'VHSI_GE30_DOWN':'VHSI≥30且目标指数20日下跌',
 'FNG_LE20':'BTC延迟恐贪首次≤20','FNG_RECOVER25':'BTC恐贪低位后上穿25'}
FOCUS={('BTC','BREAK20_ABOVE_MA200'):'重点纸面跟踪',('BTC','RV90_DOWN'):'重点纸面跟踪',
       ('HSTECH','RV90_DOWN'):'小样本观察',('CYB','IV90_DOWN'):'小样本观察',
       ('SPX','IV90_DOWN'):'美股独立研究',('NDX','IV90_DOWN'):'美股独立研究',('SOX','RV90_DOWN'):'半导体独立研究'}


def venue(market):
    return MARKETS[market].get('venue','CN' if market in ('CYB','CSI300') else 'HK')


def signal_names(market):
    names=list(RULES)[:7]
    if not MARKETS[market].get('iv_applicable',True):names.remove('IV90_DOWN')
    if market in ('HSI','HSTECH'):names.append('VHSI_GE30_DOWN')
    if market=='BTC':names+=['FNG_LE20','FNG_RECOVER25']
    return names


def clean(value):
    if isinstance(value,dict):return {str(k):clean(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [clean(v) for v in value]
    if isinstance(value,(datetime,date,pd.Timestamp)):return value.isoformat()
    if isinstance(value,(np.bool_,)):return bool(value)
    if isinstance(value,(np.integer,)):return int(value)
    if isinstance(value,(float,np.floating)):return float(value) if math.isfinite(value) else None
    return value


def expected_session(market,now):
    meta=MARKETS[market]
    local=now.astimezone(ZoneInfo(meta['zone']))
    if meta['year']==365:return local.date()-timedelta(days=1)
    from .calendar import last_session,close_time
    exchange=venue(market)
    day=local.date()
    cutoff=close_time(exchange,day,availability=True)
    if local.time().replace(tzinfo=None)<cutoff:day-=timedelta(days=1)
    return last_session(exchange,day)


def bar_end(market,day):
    day=date.fromisoformat(str(day)[:10]);meta=MARKETS[market]
    if meta['year']==365:return datetime.combine(day+timedelta(days=1),time(),timezone.utc)
    from .calendar import close_time
    return datetime.combine(day,close_time(venue(market),day),ZoneInfo(meta['zone']))


def validate_prices(frame):
    if frame.index.has_duplicates:raise ValueError('duplicate price sessions')
    frame=frame.sort_index().copy()
    cols=['open','high','low','close']
    a=frame[cols].astype(float)
    if not np.isfinite(a.to_numpy()).all() or not (a>0).all().all():raise ValueError('nonpositive/nonfinite OHLC')
    tolerance=a.abs().max(axis=1)*1e-6
    if ((a.high+tolerance<a[['open','close','low']].max(axis=1))|(a.low-tolerance>a[['open','close','high']].min(axis=1))).any():raise ValueError('inconsistent OHLC envelope')
    return frame


def compute(market,prices,iv=None,fng=None):
    d=validate_prices(prices);c=d.close;year=MARKETS[market]['year']
    if not MARKETS[market].get('iv_applicable',True):iv=None
    x=(iv if iv is not None else pd.Series(dtype=float)).reindex(d.index)
    x=pd.to_numeric(x,errors='coerce').where(lambda s:s>0)
    r=np.log(c).diff();rv=r.rolling(20,min_periods=20).std(ddof=1)*math.sqrt(year)*100
    prior=x.shift(1).rolling(year,min_periods=math.ceil(year*.95))
    ivq=prior.quantile(.9)
    first=x.first_valid_index()
    if first is not None:
        # A full bar window AND a full calendar-year span, not merely 239/346 points.
        ivq[(np.arange(len(d))<d.index.get_loc(first)+year)|((d.index-first).days<365)]=np.nan
    else:ivq[:]=np.nan
    delta=c.diff();up=delta.clip(lower=0).ewm(alpha=1/14,adjust=False,min_periods=100).mean()
    down=(-delta.clip(upper=0)).ewm(alpha=1/14,adjust=False,min_periods=100).mean()
    f=pd.DataFrame(dict(close=c,ret_20=c/c.shift(20)-1,ma_20=c.rolling(20).mean(),ma_200=c.rolling(200).mean(),
                        prior_20_close_high=c.shift(1).rolling(20).max(),drawdown_1y=c/c.rolling(year).max()-1,
                        rv_20=rv,rv_q90_1y=rv.shift(1).rolling(year,min_periods=year).quantile(.9),
                        iv=x,iv_q90_1y=ivq,rsi_14=100-100/(1+up/down),
                        iv_window_count=x.shift(1).rolling(year,min_periods=1).count(),
                        rv_window_count=rv.shift(1).rolling(year,min_periods=1).count()),index=d.index)
    f['iv_percentile_1y']=np.nan
    f['rv_percentile_1y']=np.nan
    for key,target in (('iv','iv_percentile_1y'),('rv_20','rv_percentile_1y')):
        vals=f[key].to_numpy()
        for i in range(year,len(f)):
            window=vals[i-year:i];window=window[np.isfinite(window)]
            threshold='iv_q90_1y' if key=='iv' else 'rv_q90_1y'
            if pd.notna(f[threshold].iloc[i]) and np.isfinite(vals[i]):f.iloc[i,f.columns.get_loc(target)]=float(np.mean(window<=vals[i]))
    fg=(fng if fng is not None else pd.Series(dtype=float)).reindex(d.index)
    f['fng_delayed']=fg
    f['fng_prior30_min']=fg.shift(1).rolling(30).min()
    for key in ('close','ma_20','ma_200','rsi_14','fng_delayed'):f['previous_'+key]=f[key].shift(1)
    cross=lambda a,b:(a>b)&(a.shift(1)<=b.shift(1) if isinstance(b,pd.Series) else a.shift(1)<=b)
    valid=lambda *names:f[list(names)].notna().all(axis=1)
    rules={
      'IV90_DOWN':((x>=ivq)&(f.ret_20<0),valid('iv','iv_q90_1y','ret_20')),
      'RV90_DOWN':((rv>=f.rv_q90_1y)&(f.ret_20<0),valid('rv_20','rv_q90_1y','ret_20')),
      'RSI_BELOW30':(f.rsi_14<30,valid('rsi_14')),
      'RSI_RECOVER30':(cross(f.rsi_14,30),valid('rsi_14')&f.rsi_14.shift(1).notna()),
      'DD20_RECOVER_MA20':((f.drawdown_1y<=-.2)&cross(c,f.ma_20),valid('drawdown_1y','ma_20')&f.ma_20.shift(1).notna()),
      'RECOVER_MA200':(cross(c,f.ma_200),valid('ma_200')&f.ma_200.shift(1).notna()),
      'BREAK20_ABOVE_MA200':((c>f.ma_200)&(c>f.prior_20_close_high),valid('ma_200','prior_20_close_high'))}
    if market in ('HSI','HSTECH'):rules['VHSI_GE30_DOWN']=((x>=30)&(f.ret_20<0),valid('iv','ret_20'))
    if market=='BTC':
        rules['FNG_LE20']=(fg<=20,fg.notna())
        rules['FNG_RECOVER25']=(cross(fg,25)&(fg.shift(1).rolling(30).min()<=20),fg.notna()&fg.shift(1).notna()&fg.shift(1).rolling(30).count().eq(30))
    if not MARKETS[market].get('iv_applicable',True):rules.pop('IV90_DOWN')
    return f,rules


def edges(condition,valid):
    return condition.fillna(False)&valid&~(condition&valid).shift(1,fill_value=False)&valid.shift(1,fill_value=False)


def contiguous(market,previous,current):
    prev=pd.Timestamp(previous).date();cur=pd.Timestamp(current).date()
    if MARKETS[market]['year']==365:return (cur-prev).days==1
    if cur.year>=2026:
        from .calendar import last_session
        try:return last_session(venue(market),cur-timedelta(days=1))==prev
        except ValueError:return False
    return True  # Frozen legacy research uses source-observed bars; explicitly disclosed.


def observations(market,features,rules,now,price_quality='ok'):
    f=features;day=f.index[-1];row=f.iloc[-1];result=[]
    for name,(condition,valid) in rules.items():
        known=bool(valid.iloc[-1]) and price_quality=='ok'
        previous_known=len(f)>1 and bool(valid.iloc[-2]) and contiguous(market,f.index[-2],day)
        state=bool(condition.iloc[-1]) if known else None
        new=bool(state and not bool(condition.iloc[-2])) if known and previous_known else None
        start=None;duration=None;uncertain=False
        if state:
            i=len(f)-1
            while i>0 and bool(valid.iloc[i-1]) and bool(condition.iloc[i-1]) and contiguous(market,f.index[i-1],f.index[i]):i-=1
            start=str(f.index[i].date());duration=len(f)-i
            uncertain=i==0 or not bool(valid.iloc[i-1]) or not contiguous(market,f.index[i-1],f.index[i])
            if uncertain and i==len(f)-1:new=None
        quality=price_quality if price_quality!='ok' else 'ok' if known else 'missing' if name.startswith(('IV','VHSI','FNG')) else 'insufficient_history'
        event_id=hashlib.sha256(f'{market}|{name}|{day.date()}|{FORMULA_VERSION}'.encode()).hexdigest()[:28]
        definitions={
            'IV90_DOWN':[('iv','>=','iv_q90_1y','volatility_pct'),('ret_20','<',0,'ratio')],
            'RV90_DOWN':[('rv_20','>=','rv_q90_1y','volatility_pct'),('ret_20','<',0,'ratio')],
            'RSI_BELOW30':[('rsi_14','<',30,'index')],
            'RSI_RECOVER30':[('previous_rsi_14','<=',30,'index'),('rsi_14','>',30,'index')],
            'DD20_RECOVER_MA20':[('drawdown_1y','<=',-.2,'ratio'),('previous_close','<=','previous_ma_20','price'),('close','>','ma_20','price')],
            'RECOVER_MA200':[('previous_close','<=','previous_ma_200','price'),('close','>','ma_200','price')],
            'BREAK20_ABOVE_MA200':[('close','>','ma_200','price'),('close','>','prior_20_close_high','price')],
            'VHSI_GE30_DOWN':[('iv','>=',30,'volatility_pct'),('ret_20','<',0,'ratio')],
            'FNG_LE20':[('fng_delayed','<=',20,'index')],
            'FNG_RECOVER25':[('fng_prior30_min','<=',20,'index'),('previous_fng_delayed','<=',25,'index'),('fng_delayed','>',25,'index')]}
        thresholds=[]
        for field,op,target,unit in definitions[name]:
            current=row[field];threshold=row[target] if isinstance(target,str) else target
            thresholds.append(dict(metric=field,value=current,operator=op,threshold=threshold,
                                   threshold_metric=target if isinstance(target,str) else None,unit=unit,
                                   distance=current-threshold))
        result.append(clean(dict(market=market,signal_id=name,label=RULES[name],tier=FOCUS.get((market,name),'对照规则'),
                      session_date=str(day.date()),bar_end_at=bar_end(market,day).isoformat(),formula_version=FORMULA_VERSION,
                      state=state,new_event=new,episode_start=start,episode_duration_bars=duration,event_start_uncertain=uncertain,
                      event_id=event_id if new else None,quality=quality,
                      missing_reason=None if known else '价格未就绪或所需特征缺失；不可判定不等于未触发',
                      current_values={k:row[k] for k in f.columns},
                      thresholds=thresholds,
                      threshold_distance_units='absolute feature units, not event probability')))
    return result


def outcome(prices,index,horizon,cost):
    entry=index+1;exit_=entry+horizon-1
    if exit_>=len(prices):return None
    price=float(prices.open.iloc[entry])
    return dict(signal_date=str(prices.index[index].date()),entry_date=str(prices.index[entry].date()),exit_date=str(prices.index[exit_].date()),
                entry=price,net_return=float(prices.close.iloc[exit_]/price-1-cost),mae=float(min(0,prices.low.iloc[entry:exit_+1].min()/price-1)))


def wilson(wins,n):
    if not n:return [None,None]
    z=1.96;p=wins/n;den=1+z*z/n;mid=(p+z*z/(2*n))/den;delta=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return [(mid-delta)*100,(mid+delta)*100]
