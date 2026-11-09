"""Verified last-complete US close preparation; never sends TG or overwrites bars."""
import json
import math
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.market_sessions import us_last_completed, verified_us_quote, CALENDAR_SOURCE
from generate_agent_snapshot import readonly_db, atomically_write, read_json
from agent_security_identity import canonical_symbol


def collect(root, now, getter=None):
    expected = us_last_completed(now)
    path = root / f'data/agent_us_closes/{expected}.json'
    existing = read_json(path, {})
    if (existing.get('session_date') == str(expected) and existing.get('quality') == 'ok'
            and existing.get('verified_count', 0) >= 10):
        return existing
    with readonly_db(root / 'data/ledger.db') as db:
        cutoff = (now.date() - timedelta(days=120)).isoformat()
        rows = db.execute("SELECT symbol,name FROM trendseedpool WHERE market='US' AND date>=? "
                          "UNION SELECT symbol,name FROM trenddailybar WHERE market='US' AND date>=?", (cutoff,cutoff)).fetchall()
    names = {str(r['symbol']): str(r['name'] or '') for r in rows}
    if getter is None:
        from modules.ingestion.tencent_quote import fetch_quotes
        getter = fetch_quotes
    quotes = getter(sorted({canonical_symbol('US',s) for s in names}), 'US')
    items, excluded = [], {}
    for symbol in sorted(names):
        quote = verified_us_quote(quotes.get(symbol) or quotes.get(canonical_symbol('US',symbol)), now, expected)
        try:
            valid = (quote and all(math.isfinite(float(quote.get(k))) for k in ('price','prev_close','pct_chg'))
                     and float(quote['prev_close']) > 0)
        except (ValueError,TypeError):
            valid = False
        if not valid:
            excluded['no_verified_expected_regular_close'] = excluded.get('no_verified_expected_regular_close',0) + 1
            continue
        items.append(dict(symbol=symbol,name=names[symbol] or quote['name'],price=quote['price'],
                          prev_close=quote['prev_close'],change_pct=quote['pct_chg'],
                          amount=quote.get('amount'),volume=quote.get('volume'),
                          open=quote.get('open'),high=quote.get('high'),low=quote.get('day_low'),
                          provider=quote['provider'],observed_at=quote['observed_at'],
                          collected_at=now.isoformat(), session_date=str(expected)))
    if len(items) < 10 or len(items) < len(names)*.5:
        raise ValueError(f'US verified close coverage insufficient: {len(items)}/{len(names)} expected {expected}')
    archive = (expected + timedelta(days=1)).isoformat()
    # Preserve existing daily/UI archive keys. Source time is stored separately.
    with sqlite3.connect(root / 'data/ledger.db', timeout=15) as db:
        for item in items:
            previous = db.execute("SELECT close FROM trenddailybar WHERE market='US' AND date=? AND symbol=?",
                                  (archive,item['symbol'])).fetchone()
            if previous:
                continue
            db.execute('INSERT INTO trenddailybar '
                       '(date,market,symbol,name,open,high,low,close,amount,turnover_rate,source,catalyst_tags,updated_at) '
                       'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (archive,'US',item['symbol'],item['name'],item['open'] or 0,item['high'] or 0,item['low'] or 0,
                        item['price'],item['amount'] or 0,0,
                        'agent_us_verified_close','',now.astimezone(timezone.utc).replace(tzinfo=None).isoformat()))
    payload = dict(schema_version='1.0',session_date=str(expected),archive_date=archive,
                   generated_at=now.isoformat(),source='Tencent regular close with provider NY observation time',
                   calendar_source=CALENDAR_SOURCE,source_universe_count=len(names),verified_count=len(items),
                   quality='ok',coverage_scope='rolling 120d US Trend seed/bar catalog, not full market',
                   excluded_reason_counts=excluded,items=items)
    atomically_write(path,(json.dumps(payload,ensure_ascii=False,allow_nan=False)+'\n').encode())
    print(json.dumps({'us_close_session':str(expected),'verified':len(items),'pool':len(names)}),flush=True)
    return payload
