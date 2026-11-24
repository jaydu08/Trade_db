"""Quant-only exchange calendar; unannounced years fail closed, not as weekdays."""
import json
from datetime import date,time,timedelta
from pathlib import Path

HK2027={'2027-01-01','2027-02-08','2027-02-09','2027-03-26','2027-03-29','2027-04-05',
        '2027-05-13','2027-06-09','2027-07-01','2027-09-16','2027-10-01','2027-10-08','2027-12-27'}
HK2027_SOURCE='https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/Participant-and-Members-Circulars/SEHK/2026/cc_SEHK_CT_077_2026.pdf'


def override(venue,year):
    path=Path(__file__).resolve().parents[2]/'data/quant/calendar_overrides.json'
    try:
        entry=json.loads(path.read_text()).get(venue,{}).get(str(year),{})
        return entry if entry.get('verified') is True and entry.get('source_url') and isinstance(entry.get('holidays'),list) else {}
    except (OSError,ValueError):return {}


def closed(venue,day):
    if venue=='US':
        from modules.market_sessions import us_is_session
        return not us_is_session(day)
    if day.year==2026:
        from scripts.agent_market_calendar import closed_reason
        return bool(closed_reason(venue,day))
    if venue=='HK' and day.year==2027:return day.weekday()>=5 or str(day) in HK2027
    entry=override(venue,day.year)
    if not entry:raise ValueError(f'{venue} {day.year} 交易所日历尚未核验；该市场暂停信号，其他市场不受影响')
    return day.weekday()>=5 or str(day) in entry['holidays']


def last_session(venue,day):
    while closed(venue,day):day-=timedelta(days=1)
    return day


def close_time(venue,day,availability=False):
    if venue=='US':
        from modules.market_sessions import us_close_at
        return us_close_at(day).time().replace(minute=20 if availability else 0)
    half=venue=='HK' and day in {date(2026,2,16),date(2026,12,24),date(2026,12,31),date(2027,2,5),date(2027,12,24),date(2027,12,31)}
    half=half or str(day) in override(venue,day.year).get('half_sessions',[])
    if half:return time(12,20 if availability else 10)
    return time(15,10 if availability else 0) if venue=='CN' else time(16,20 if availability else 10)
