"""Exchange-verified full-day closures for Agent-only A/H snapshot readiness.

Annual dates are deliberately explicit: an unknown closure must fail the quote
freshness checks instead of being silently treated as a completed session.
"""

from __future__ import annotations

from datetime import date, timedelta

SOURCES = {
    'CN': 'https://www.sse.com.cn/disclosure/dealinstruc/closed/',
    'HK': ('https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/'
           'Participant-and-Members-Circulars/SEHK/2025/ce_SEHK_CT_075_2025.pdf'),
}


def _days(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    return {first + timedelta(days=offset) for offset in range((last - first).days + 1)}


CLOSURES = {
    'CN': {
        **{day: '元旦' for day in _days('2026-01-01', '2026-01-03')},
        **{day: '春节' for day in _days('2026-02-15', '2026-02-23')},
        **{day: '清明节' for day in _days('2026-04-04', '2026-04-06')},
        **{day: '劳动节' for day in _days('2026-05-01', '2026-05-05')},
        **{day: '端午节' for day in _days('2026-06-19', '2026-06-21')},
        **{day: '中秋节' for day in _days('2026-09-25', '2026-09-27')},
        **{day: '国庆节' for day in _days('2026-10-01', '2026-10-07')},
    },
    'HK': {
        **{day: name for day, name in (
            (date(2026, 1, 1), 'New Year'),
            (date(2026, 2, 17), 'Lunar New Year'),
            (date(2026, 2, 18), 'Lunar New Year'),
            (date(2026, 2, 19), 'Lunar New Year'),
            (date(2026, 4, 3), 'Good Friday'),
            (date(2026, 4, 6), 'Ching Ming substitute'),
            (date(2026, 4, 7), 'Easter substitute'),
            (date(2026, 5, 1), 'Labour Day'),
            (date(2026, 5, 25), 'Buddha Birthday substitute'),
            (date(2026, 6, 19), 'Tuen Ng Festival'),
            (date(2026, 7, 1), 'HKSAR Establishment Day'),
            (date(2026, 10, 1), 'National Day'),
            (date(2026, 10, 19), 'Chung Yeung Festival substitute'),
            (date(2026, 12, 25), 'Christmas Day'),
        )},
    },
}


def closed_reason(market: str, day: date):
    if day.weekday() >= 5:
        return 'weekend'
    return CLOSURES.get(market, {}).get(day)


def last_session(market: str, day: date):
    candidate = day
    while closed_reason(market, candidate):
        candidate -= timedelta(days=1)
    return candidate
