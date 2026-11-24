"""Private point-in-time storage and idempotent event/forward ledgers."""
import hashlib,json,sqlite3
from pathlib import Path
from datetime import datetime,timezone
import pandas as pd
from .engine import clean,FORMULA_VERSION,MARKETS,edges,contiguous,bar_end


class Store:
    def __init__(self,root):
        self.root=Path(root)/'data/quant';self.root.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(self.root/'quant.sqlite',timeout=20)
        self.db.row_factory=sqlite3.Row
        self.inserted_event_ids=set()
        self.db.executescript('''PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS series(market TEXT,kind TEXT,day TEXT,payload TEXT,PRIMARY KEY(market,kind,day));
        CREATE TABLE IF NOT EXISTS feed(key TEXT PRIMARY KEY,payload TEXT);
        CREATE TABLE IF NOT EXISTS revisions(id INTEGER PRIMARY KEY,recorded_at TEXT,market TEXT,kind TEXT,day TEXT,previous TEXT,current TEXT);
        CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,market TEXT,signal TEXT,day TEXT,formula TEXT,status TEXT,origin TEXT,first_seen_at TEXT,payload TEXT);
        CREATE TABLE IF NOT EXISTS event_revisions(id INTEGER PRIMARY KEY,event_id TEXT,recorded_at TEXT,previous TEXT,current TEXT);
        CREATE TABLE IF NOT EXISTS observations(id TEXT PRIMARY KEY,payload TEXT);
        CREATE TABLE IF NOT EXISTS forward(event_id TEXT,horizon INTEGER,payload TEXT,PRIMARY KEY(event_id,horizon));
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY,payload TEXT);
        ''')
        self.db.commit()

    def setting(self,key,value=None):
        row=self.db.execute('SELECT value FROM settings WHERE key=?',(key,)).fetchone()
        if row:return row[0]
        if value is not None:
            self.db.execute('INSERT INTO settings VALUES(?,?)',(key,value));self.db.commit()
        return value

    def feed(self,key,payload=None):
        if payload is None:
            row=self.db.execute('SELECT payload FROM feed WHERE key=?',(key,)).fetchone()
            return json.loads(row[0]) if row else {}
        previous=self.feed(key);history=list(previous.get('source_switch_history',[]))
        if not history and previous.get('source_switch'):
            history.append(dict(previous_source=previous.get('previous_source'),source=previous.get('source'),
                                detected_at=previous.get('fetched_at'),overlap_comparison=previous.get('overlap_comparison')))
        if payload.get('source_switch') and (not history or history[-1].get('source')!=payload.get('source')):
            history.append(dict(previous_source=payload.get('previous_source'),source=payload.get('source'),
                                detected_at=payload.get('fetched_at'),overlap_comparison=payload.get('overlap_comparison')))
        payload=dict(payload,source_switch_history=history[-20:])
        self.db.execute('INSERT OR REPLACE INTO feed VALUES(?,?)',(key,json.dumps(clean(payload),ensure_ascii=False,allow_nan=False)));self.db.commit()

    def merge(self,market,kind,rows,meta):
        changed=[]
        with self.db:
            for day,values in rows.items():
                values=clean(values);existing=self.db.execute('SELECT payload FROM series WHERE market=? AND kind=? AND day=?',(market,kind,day)).fetchone()
                previous=json.loads(existing[0]) if existing else None
                item=dict(values,**meta,session_date=day)
                value_changed=bool(previous and any(previous.get(k)!=v for k,v in values.items()))
                if previous and not value_changed and previous.get('source')==meta.get('source'):
                    # A repeated fetch is not a new first-availability observation.
                    item['available_at']=previous.get('available_at') or meta.get('available_at')
                if value_changed:
                    self.db.execute('INSERT INTO revisions(recorded_at,market,kind,day,previous,current) VALUES(?,?,?,?,?,?)',
                       (meta.get('fetched_at'),market,kind,day,json.dumps(previous),json.dumps(item)));changed.append(day)
                self.db.execute('INSERT OR REPLACE INTO series VALUES(?,?,?,?)',(market,kind,day,json.dumps(item,allow_nan=False)))
        return dict(changed_rows=len(changed),first_changed_session=min(changed) if changed else None)

    def series(self,market,kind):
        rows=self.db.execute('SELECT day,payload FROM series WHERE market=? AND kind=? ORDER BY day',(market,kind)).fetchall()
        return {r['day']:json.loads(r['payload']) for r in rows}

    def frames(self,market,cutoff):
        prices=self.series(market,'price');ivs=self.series(market,'iv');fng=self.series('BTC','fng') if market=='BTC' else {}
        prices={k:v for k,v in prices.items() if k<=str(cutoff)}
        if not prices:return None,None,None
        p=pd.DataFrame({k:{c:v[c] for c in ('open','high','low','close')} for k,v in prices.items()}).T
        p.index=pd.to_datetime(p.index);p=p.sort_index().astype(float)
        value=lambda rows:pd.Series({pd.Timestamp(k):v.get('value') for k,v in rows.items()},dtype=float).sort_index()
        return p,value(ivs),value(fng)

    def data_version(self,market):
        rows=self.db.execute('SELECT kind,day,payload FROM series WHERE market=? OR (market=? AND kind=?) ORDER BY market,kind,day',(market,'BTC','fng' if market=='BTC' else '__unused')).fetchall()
        # Source response hash + values, not fetch wall-clock, defines data version.
        content=[(r[0],r[1],{k:v for k,v in json.loads(r[2]).items() if k not in ('fetched_at','available_at')}) for r in rows]
        return hashlib.sha256(json.dumps(content,sort_keys=True).encode()).hexdigest()

    def update_events(self,market,prices,features,rules,now,data_version,launched_at):
        visible=set();changes=0
        self.inserted_event_ids=set()
        source_series={kind:self.series(market,kind) for kind in ('price','iv')}
        with self.db:
            for signal,(condition,valid) in rules.items():
                for i in list(edges(condition,valid).to_numpy().nonzero()[0]):
                    if i==0 or not contiguous(market,features.index[i-1],features.index[i]):continue
                    day=str(features.index[i].date())
                    event_id=hashlib.sha256(f'{market}|{signal}|{day}|{FORMULA_VERSION}'.encode()).hexdigest()[:28]
                    visible.add(event_id);end=bar_end(market,day)
                    prior_day=str(features.index[i-1].date())
                    prior=self.db.execute('SELECT payload FROM observations WHERE id=?',(f'{market}:{signal}:{prior_day}:{FORMULA_VERSION}',)).fetchone()
                    prior_unknown=(json.loads(prior[0]).get('state') is None) if prior else bar_end(market,prior_day)>=launched_at
                    origin='prospective' if end>=launched_at and i==len(features)-1 and not prior_unknown else 'historical_backfill'
                    old=self.db.execute('SELECT * FROM events WHERE id=?',(event_id,)).fetchone()
                    values=clean(features.iloc[i].to_dict())
                    payload=dict(id=event_id,market=market,signal_id=signal,session_date=day,bar_end_at=end.isoformat(),
                                 features=values,formula_version=FORMULA_VERSION,data_version=data_version,
                                 source_records={k:source_series[k].get(day) for k in ('price','iv')},
                                 status='active',origin=old['origin'] if old else origin,
                                 first_seen_at=old['first_seen_at'] if old else now.isoformat())
                    text=json.dumps(clean(payload),ensure_ascii=False,allow_nan=False)
                    if old:
                        oldp=json.loads(old['payload'])
                        # Full-run data hash changes do not erase the as-seen event.
                        if oldp.get('features')!=values or old['status']=='revised_out':
                            self.db.execute('INSERT INTO event_revisions(event_id,recorded_at,previous,current) VALUES(?,?,?,?)',(event_id,now.isoformat(),old['payload'],text))
                            self.db.execute('UPDATE events SET status=?,payload=? WHERE id=?',('active',text,event_id));changes+=1
                    else:
                        self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?)',(event_id,market,signal,day,FORMULA_VERSION,'active',origin,now.isoformat(),text));changes+=1
                        if origin=='prospective':self.inserted_event_ids.add(event_id)
            for old in self.db.execute('SELECT * FROM events WHERE market=? AND formula=? AND status=?',(market,FORMULA_VERSION,'active')).fetchall():
                if old['id'] not in visible:
                    payload=json.loads(old['payload']);payload.update(status='revised_out',revision_reason='event no longer reproducible from current source version',revised_at=now.isoformat())
                    text=json.dumps(payload,ensure_ascii=False)
                    self.db.execute('INSERT INTO event_revisions(event_id,recorded_at,previous,current) VALUES(?,?,?,?)',(old['id'],now.isoformat(),old['payload'],text))
                    self.db.execute('UPDATE events SET status=?,payload=? WHERE id=?',('revised_out',text,old['id']));changes+=1
        self.evaluate_forward(market,prices,now)
        return changes

    def evaluate_forward(self,market,prices,now):
        from .engine import outcome
        horizons=[30,90,180] if MARKETS[market]['year']==365 else [21,63,126]
        cost=.004 if MARKETS[market]['year']==365 else .002
        with self.db:
            for event in self.db.execute("SELECT * FROM events WHERE market=? AND origin='prospective'",(market,)).fetchall():
                day=pd.Timestamp(event['day']);position=prices.index.get_indexer([day])[0]
                if position<0:continue
                for h in horizons:
                    key=(event['id'],h);old=self.db.execute('SELECT payload FROM forward WHERE event_id=? AND horizon=?',key).fetchone()
                    if old and json.loads(old[0]).get('status')=='completed':continue # Preserve original as-seen matured evaluation.
                    out=outcome(prices,position,h,cost)
                    entry=position+1
                    gap=any(not contiguous(market,prices.index[i-1],prices.index[i]) for i in range(entry,min(entry+h,len(prices))))
                    if gap:out=None
                    status='data_gap' if gap else 'completed' if out else 'evaluating' if entry<len(prices) else 'awaiting_entry_bar'
                    oldp=json.loads(old[0]) if old else {}
                    entry_known=entry<len(prices) and contiguous(market,prices.index[position],prices.index[entry])
                    fixed_entry=oldp.get('entry_price') or (float(prices.open.iloc[entry]) if entry_known else None)
                    if out and fixed_entry:
                        out.update(entry=fixed_entry,net_return=float(prices.close.iloc[entry+h-1]/fixed_entry-1-cost),
                                   mae=float(min(0,prices.low.iloc[entry:entry+h].min()/fixed_entry-1)))
                    payload=dict(event_id=event['id'],market=market,signal_id=event['signal'],horizon=h,status=status,
                                 source_event_status=event['status'],
                                 validation_type='prospective',formula_version=event['formula'],
                                 cost_assumption=cost,observed_holding_bars=max(0,min(h,len(prices)-entry)),
                                 entry_date=oldp.get('entry_date') or (str(prices.index[entry].date()) if entry_known else None),
                                 entry_price=fixed_entry,
                                 entry_price_observed_at=oldp.get('entry_price_observed_at') or (now.isoformat() if entry_known else None),
                                 missing_reason='holding-window trading bars missing or calendar unverified' if gap else None,
                                 updated_at=now.isoformat())
                    payload.update(out or {})
                    self.db.execute('INSERT OR REPLACE INTO forward VALUES(?,?,?)',(*key,json.dumps(clean(payload),allow_nan=False)))

    def events(self,market,limit=300):
        # Bounded view; complete event files are exported separately.
        rows=self.db.execute('SELECT payload FROM events WHERE market=? ORDER BY day DESC,id LIMIT ?',(market,limit)).fetchall()
        events=[json.loads(r[0]) for r in rows]
        for event in events:
            event['forward']=[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM forward WHERE event_id=? ORDER BY horizon',(event['id'],))]
            event['revision_count']=self.db.execute('SELECT count(*) FROM event_revisions WHERE event_id=?',(event['id'],)).fetchone()[0]
        return events

    def forward_summary(self):
        all_=[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM forward')]
        completed=[r for r in all_ if r['status']=='completed']
        groups={}
        for row in all_:
            key=(row['market'],row['signal_id'],row['horizon'],row['formula_version'])
            group=groups.setdefault(key,dict(market=key[0],signal_id=key[1],horizon=key[2],formula_version=key[3],completed=0,wins=0,evaluating=0))
            if row['status']=='completed':group['completed']+=1;group['wins']+=int(row['net_return']>0)
            else:group['evaluating']+=1
        return dict(validation_type='prospective',completed_evaluations=len(completed),evaluating_count=sum(r['status']!='completed' for r in all_),
                    groups=list(groups.values()),
                    grouping_warning='Different horizons are separate evaluations, not independent trials; compare by market/signal/horizon/formula',
                    evaluations=all_[-500:])
