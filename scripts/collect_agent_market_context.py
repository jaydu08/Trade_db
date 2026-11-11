"""Prepare dated index/breadth records before immutable Agent generation."""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from agent_market_calendar import last_session
from generate_agent_snapshot import atomically_write, read_json, stock_snapshot


def collect(root, now):
    from collect_agent_us_closes import collect as collect_us
    from modules.market_sessions import us_last_completed
    collect_us(root, now)
    sys.path.insert(0, str(root))
    from modules.ingestion.market_context import qualify_breadth
    from modules.ingestion.breadth_fallback import fetch_breadth,VERSION
    cached = read_json(root / 'data/agent_market_context' / (now.date().isoformat() + '.json'), {})
    try:
        age = (now - datetime.fromisoformat(cached['generated_at'])).total_seconds()
        if (0 <= age < 900 and cached.get('breadth_pipeline_version')==VERSION
                and all(not s.get('breadth',{}).get('stale',True) for s in cached.get('markets',{}).values())
                and set(cached.get('markets', {})) == {'CN', 'HK', 'US'} and not cached.get('errors')
                and cached['markets']['US'].get('session_date') == str(us_last_completed(now))):
            for market, segment in cached['markets'].items():
                if not segment.get('breadth',{}).get('fallback_used'):
                    segment['breadth'] = qualify_breadth(market, segment.get('breadth', {}))
            atomically_write(root / 'data/agent_market_context' / (now.date().isoformat() + '.json'),
                             (json.dumps(cached, ensure_ascii=False, allow_nan=False) + '\n').encode())
            return cached
    except (KeyError, TypeError, ValueError):
        pass
    sys.path.insert(0, str(root))
    from modules.ingestion.market_context import fetch_indices
    from modules.ingestion.akshare_client import AkShareClient
    from modules.monitor.market_activity import record_stock_turnover
    # Restore the existing turnover input from real full-catalog quotes; no ranking or TG calls.
    activity = read_json(root / 'data/market_activity_stock.json', [])
    errors = {}
    for market in ('CN', 'HK'):
        if last_session(market, now.date()) != now.date():
            continue
        if any(r.get('market') == market and r.get('day') == now.date().isoformat() for r in activity):
            continue
        try:
            frame = AkShareClient._fetch_bulk_sina(market)
            if '行情时间' not in frame:
                raise ValueError('full quote observation timestamps unavailable')
            frame = frame[frame['行情时间'].astype(str).str.startswith(now.date().isoformat())]
            if record_stock_turnover(market, frame) is None:
                raise ValueError('turnover coverage guard rejected full quote frame')
        except Exception as exc:
            errors[market + '_turnover'] = type(exc).__name__ + ': ' + str(exc)[:120]
    markets, *_ = stock_snapshot(root, now)
    sessions = {r['market']: r.get('session_date') for r in markets}
    payload = dict(generated_at=now.isoformat(), markets={}, errors=errors,breadth_pipeline_version=VERSION)
    for market in ('CN', 'HK', 'US'):
        expected = sessions.get(market)
        if market != 'US':
            expected = last_session(market, now.date()).isoformat()
        if not expected:
            errors[market] = 'no completed session in source archive'
            continue
        indices = fetch_indices(market, expected, now)
        breadth = fetch_breadth(root,market,expected,datetime.now(ZoneInfo('Asia/Shanghai')))
        payload['markets'][market] = dict(session_date=expected, indices=indices, breadth=breadth)
    atomically_write(root / 'data/agent_market_context' / (now.date().isoformat() + '.json'),
                     (json.dumps(payload, ensure_ascii=False, allow_nan=False) + '\n').encode())
    return payload
