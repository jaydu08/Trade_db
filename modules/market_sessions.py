"""NYSE cash-equity sessions: explicit verified calendar, never bank/H.15 days."""
from datetime import date, datetime, timedelta
import math
from zoneinfo import ZoneInfo

NY = ZoneInfo('America/New_York')
CALENDAR_SOURCE = 'https://www.nyse.com/trade/hours-calendars'
HOLIDAYS = {
    2026: {'01-01','01-19','02-16','04-03','05-25','06-19','07-03','09-07','11-26','12-25'},
    2027: {'01-01','01-18','02-15','03-26','05-31','06-18','07-05','09-06','11-25','12-24'},
    2028: {'01-17','02-21','04-14','05-29','06-19','07-04','09-04','11-23','12-25'},
}
EARLY = {date(2026,11,27), date(2026,12,24), date(2027,11,26),
         date(2028,7,3), date(2028,11,24)}


def us_is_session(day):
    if day.year not in HOLIDAYS:
        raise ValueError('NYSE calendar not verified for year ' + str(day.year))
    return day.weekday() < 5 and day.strftime('%m-%d') not in HOLIDAYS[day.year]


def us_close_at(day):
    return datetime(day.year, day.month, day.day, 13 if day in EARLY else 16, tzinfo=NY)


def us_last_completed(now):
    if now.tzinfo is None:
        raise ValueError('calendar requires timezone-aware clock')
    local = now.astimezone(NY)
    day = local.date()
    if not us_is_session(day) or local < us_close_at(day):
        day -= timedelta(days=1)
    while not us_is_session(day):
        day -= timedelta(days=1)
    return day


def us_archive_session(archive_day, source=None):
    day = date.fromisoformat(str(archive_day))
    if source == 'trend_pool_history_backfill':
        return day  # Vendor daily history already uses the exchange session date.
    day -= timedelta(days=1)  # Existing EOD UI/archive key is next Beijing/UTC day.
    while not us_is_session(day):
        day -= timedelta(days=1)
    return day


def verified_us_quote(quote, now, expected=None):
    if not quote or quote.get('session_scope') != 'regular':
        return None
    expected = expected or us_last_completed(now)
    try:
        stamp = datetime.fromisoformat(str(quote['timestamp']))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=ZoneInfo(quote['timestamp_timezone']))
        stamp = stamp.astimezone(NY)
        if (stamp.date() != expected or stamp < us_close_at(expected) or
                stamp > now.astimezone(NY) + timedelta(minutes=5)):
            return None
        if not math.isfinite(float(quote['price'])) or float(quote['price']) <= 0:
            return None
    except (KeyError, ValueError, TypeError):
        return None
    return dict(quote, session_date=expected.isoformat(), observed_at=stamp.isoformat())
