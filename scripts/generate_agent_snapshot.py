"""Freeze a small, public-only Trend market snapshot from existing local archives.

No network calls, migrations, business API calls, or source-database writes.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import tempfile
from collections import Counter
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
NEW_YORK = ZoneInfo("America/New_York")
MAX_BYTES = 220_000
CRYPTO_STALE_SECONDS = 900
ONCHAIN_MIN_LIQUIDITY = 50_000
ONCHAIN_MIN_VOLUME = 50_000
CURRENCY = {"CN": "CNY", "HK": "HKD", "US": "USD"}
LEVERAGED_TICKERS = {"SOXL", "SOXS", "TQQQ", "SQQQ", "SPXL", "SPXS", "NVDL", "NVDS",
                     "TSLL", "TSLS", "LABU", "LABD", "FNGU", "FNGD", "YINN", "YANG"}


def _nth_weekday(year, month, weekday, occurrence):
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year, month, weekday):
    first_next = date(year + (month == 12), month % 12 + 1, 1)
    last = first_next - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed_holiday(year, month, day):
    actual = date(year, month, day)
    return actual - timedelta(days=1) if actual.weekday() == 5 else actual + timedelta(days=1) if actual.weekday() == 6 else actual


def _us_h15_holidays(year):
    return {_observed_holiday(year, 1, 1), _nth_weekday(year, 1, 0, 3),
            _nth_weekday(year, 2, 0, 3), _last_weekday(year, 5, 0),
            _observed_holiday(year, 6, 19), _observed_holiday(year, 7, 4),
            _nth_weekday(year, 9, 0, 1), _nth_weekday(year, 10, 0, 2),
            _observed_holiday(year, 11, 11), _nth_weekday(year, 11, 3, 4),
            _observed_holiday(year, 12, 25), _observed_holiday(year + 1, 1, 1)}


def expected_h15_observation(now):
    """Latest US business date whose standard 16:15 ET H.15 release has passed."""
    ny = now.astimezone(NEW_YORK)
    candidate = ny.date() if (ny.hour, ny.minute) >= (16, 45) else ny.date() - timedelta(days=1)
    while candidate.weekday() >= 5 or candidate in _us_h15_holidays(candidate.year):
        candidate -= timedelta(days=1)
    return candidate


def macro_freshness(item_id, observed, checked, now, category='macro'):
    """Observation freshness is independent of how recently the collector ran."""
    if not observed:
        return 'missing', True, 'observation date unavailable', None
    try:
        observed_day = date.fromisoformat(str(observed)[:10])
    except ValueError:
        return 'missing', True, 'invalid observation date', None
    if observed_day > now.date():
        return 'error', True, 'observation date is in the future', None
    if category == 'macro' and item_id in {'DGS2', 'DGS10', 'DFII10', 'T10YIE'}:
        expected = expected_h15_observation(now)
        if observed_day < expected:
            return 'stale', True, f'H.15 expected observation {expected}; source still {observed_day}', expected.isoformat()
        return 'ok', False, None, expected.isoformat()
    if category == 'macro' and item_id == 'DTWEXBGS':
        # H.10 dollar-index publication cadence differs from H.15; do not
        # label the last business day as current without release-date evidence.
        if (now.date() - observed_day).days > 10:
            return 'stale', True, 'broad dollar index observation older than 10 calendar days', None
        return 'unknown', False, 'H.10 publication-date mapping not stored', None
    if category == 'macro' and item_id == 'DFEDTARU':
        return 'unknown', False, 'policy target changes on announcement, not each trading day', None
    if category == 'futures':
        age = (now.date() - observed_day).days
        return ('stale', True, 'futures quote is not from the current session', now.date().isoformat()) if age > 1 else ('ok', False, None, now.date().isoformat())
    if category == 'global_assets':
        expected = expected_h15_observation(now)
        return ('stale', True, f'last completed US session {expected} newer than quote', expected.isoformat()) if observed_day < expected else ('ok', False, None, expected.isoformat())
    return 'unknown', False, 'no confirmed publication policy', None


def classify_stock_asset(symbol, name, catalog_type=None):
    """Keep a visible ETF/leveraged-ETF distinction even if meta.asset says STOCK."""
    ticker = str(symbol or "").split(".")[-1].upper()
    label = str(name or "")
    lower = label.lower()
    leverage = (ticker in LEVERAGED_TICKERS or bool(re.search(r"(?<!\d)[23](?:\.0)?\s*[xX倍]", label))
                or any(word in label for word in ("两倍", "三倍", "反向", "做空ETF"))
                or any(word in lower for word in ("leveraged", "inverse", "ultra bull", "ultra bear")))
    etf = ("etf" in lower or "交易型开放式" in label or ticker in LEVERAGED_TICKERS
           or str(catalog_type or "").upper() in {"ETF", "LEVERAGED_ETF"})
    if etf and leverage:
        return "LEVERAGED_ETF", "name_or_ticker_heuristic"
    if etf:
        return "ETF", "name_or_catalog"
    if str(catalog_type or "").upper() == "STOCK":
        return "STOCK", "meta.asset"
    return "UNKNOWN", "insufficient_asset_metadata"


def number(value):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def epoch_iso(value):
    value = number(value)
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds") if value is not None else None


def utc_naive_iso(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat(timespec="seconds")
    except ValueError:
        return None


def read_json(path, fallback):
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return fallback


def readonly_db(path):
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def atomically_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=path.parent, prefix=".agent-", delete=False) as handle:
            temporary = handle.name
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def immutably_create(path, data):
    """Publish a complete archive with an atomic no-replace hard link."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=path.parent, prefix=".archive-", delete=False) as handle:
            temporary = handle.name
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def bar_returns(db, market, symbol, latest_date, latest_close):
    rows = db.execute(
        "SELECT date, close FROM trenddailybar WHERE market=? AND symbol=? AND date<? "
        "AND close>0 ORDER BY date DESC LIMIT 50", (market, symbol, latest_date)
    ).fetchall()
    unique = []
    seen = set()
    for row in rows:
        if row["date"] not in seen:
            unique.append(row)
            seen.add(row["date"])
    result = {}
    for count, max_days, label in ((1, 4, "1d"), (3, 8, "3d"), (7, 16, "7d")):
        if len(unique) >= count:
            baseline = unique[count - 1]
            gap = (date.fromisoformat(latest_date) - date.fromisoformat(baseline["date"])).days
            previous = number(baseline["close"])
            result[label] = round((latest_close / previous - 1) * 100, 3) if previous and gap <= max_days else None
        else:
            result[label] = None
    return result


def stock_snapshot(root, now):
    ledger = root / "data/ledger.db"
    activity = read_json(root / "data/market_activity_stock.json", [])
    activity = activity if isinstance(activity, list) else []
    markets, up, down, health, limitations = [], [], [], [], []
    if not ledger.exists():
        for market in CURRENCY:
            markets.append(dict(market=market, session_date=None, data_asof=None, source="Trade_db ledger.db",
                                quality="missing", stale=True, missing_reason="ledger.db unavailable",
                                turnover=None, turnover_currency=CURRENCY[market], breadth=None, major_index=None))
        return markets, up, down, health, ["股票日线数据库不可用"]
    catalog = {}
    meta = root / "data/meta.db"
    if meta.exists():
        with closing(readonly_db(meta)) as meta_db:
            for row in meta_db.execute("SELECT symbol,asset_type FROM asset"):
                catalog[str(row["symbol"])] = row["asset_type"]
    with closing(readonly_db(ledger)) as db:
        for market in CURRENCY:
            row = db.execute("SELECT MAX(date) AS day FROM trenddailybar WHERE market=?", (market,)).fetchone()
            source_date = row["day"] if row else None
            if not source_date:
                markets.append(dict(market=market, session_date=None, data_asof=None, source="TrendDailyBar",
                                    quality="missing", stale=True, missing_reason="no daily bars",
                                    turnover=None, turnover_currency=CURRENCY[market], breadth=None, major_index=None))
                continue
            bars = db.execute("SELECT symbol,name,close,amount,turnover_rate,source,catalyst_tags,updated_at "
                              "FROM trenddailybar WHERE market=? AND date=? AND close>0",
                              (market, source_date)).fetchall()
            archive_source_counts = dict(Counter(str(bar['source']) for bar in bars))
            preference = {"heatmap": 3, "daily_hot": 2, "trend_pool_refresh_eod": 1}
            latest = {}
            for bar in bars:
                key = str(bar["symbol"])
                if key not in latest or preference.get(bar["source"], 0) > preference.get(latest[key]["source"], 0):
                    latest[key] = bar
            rank_rows = db.execute("SELECT symbol,change_pct FROM dailyrank WHERE market=? AND date=? "
                                   "ORDER BY id DESC", (market, source_date)).fetchall()
            rank_change = {}
            for rank_row in rank_rows:
                rank_change.setdefault(str(rank_row["symbol"]), number(rank_row["change_pct"]))
            board = read_json(root / "data/tg_daily_boards" / f"{market}_{source_date}.json", {})
            board_items = board.get("items", []) if isinstance(board, dict) else []
            board_items = board_items if isinstance(board_items, list) else []
            board_by_symbol = {str(item.get("symbol")): item for item in board_items if isinstance(item, dict)}
            last_asof = max((utc_naive_iso(b["updated_at"]) for b in latest.values()), default=None)
            if market == "US" and last_asof:
                # SQLModel updated_at is UTC-naive; date in this archive is Beijing ingestion day.
                session = datetime.fromisoformat(last_asof).astimezone(NEW_YORK).date().isoformat()
            else:
                session = source_date
            old = (now.date() - date.fromisoformat(source_date)).days > 4
            activity_row = next((a for a in activity if a.get("market") == market and a.get("day") == source_date), None)
            markets.append(dict(market=market, session_date=session, archive_date=source_date,
                                data_asof=last_asof, source="TrendDailyBar; TG daily board when present",
                                quality="partial", stale=old, missing_reason=None if latest else "no valid close",
                                turnover=number(activity_row.get("amount")) if activity_row else None,
                                turnover_currency=CURRENCY[market], breadth=None, major_index=None,
                                candidate_universe=len(latest), archive_source_counts=archive_source_counts))
            health.append(dict(domain=f"stocks_{market}", session_date=session, data_asof=last_asof,
                               source="Trade_db ledger.db", quality="partial", stale=old,
                               missing_reason=None if latest else "no valid close"))
            candidates = []
            for symbol, bar in latest.items():
                price = number(bar["close"])
                if not price:
                    continue
                returns = bar_returns(db, market, symbol, source_date, price)
                if symbol in rank_change:
                    returns["1d"] = rank_change[symbol]
                board_item = board_by_symbol.get(symbol)
                if board_item and number(board_item.get("change_pct")) is not None:
                    returns["1d"] = number(board_item["change_pct"])
                asset_type, type_source = classify_stock_asset(symbol, bar["name"], catalog.get(symbol))
                item = dict(market=market, symbol=symbol, name=str(bar["name"] or ""),
                            asset_type=asset_type, asset_type_source=type_source,
                            session_date=session, data_asof=utc_naive_iso(bar["updated_at"]),
                            price=price, price_type="close_or_archived_latest", currency=CURRENCY[market],
                            price_adjustment="unknown_from_existing_archive",
                            coverage_pool="TrendDailyBar candidates, not full market",
                            returns_pct=returns, amount=number(bar["amount"]) if (number(bar["amount"]) or 0) > 0 else None,
                            volume=None, turnover_rate=number(bar["turnover_rate"]) if (number(bar["turnover_rate"]) or 0) > 0 else None,
                            industry=(board_item or {}).get("industry_label") or None,
                            trend_score=number((board_item or {}).get("trend_score")),
                            catalyst_tags=(board_item or {}).get("catalyst_tags") or None,
                            source="TG daily selected board" if board_item else f"TrendDailyBar:{bar['source']}",
                            quality="partial", stale=old,
                            ranking_method="observed_return_not_volatility_standardized")
                candidates.append((item, symbol in board_by_symbol))
            def primary_return(item):
                return next((item["returns_pct"][key] for key in ("1d", "3d", "7d")
                             if item["returns_pct"][key] is not None), None)
            positives = [(i, selected) for i, selected in candidates if primary_return(i) is not None and primary_return(i) > 0]
            negatives = [(i, selected) for i, selected in candidates if primary_return(i) is not None and primary_return(i) < 0]
            positives.sort(key=lambda pair: (pair[1], pair[0]["returns_pct"]["1d"] or 0,
                                             pair[0]["returns_pct"]["3d"] or 0), reverse=True)
            negatives.sort(key=lambda pair: primary_return(pair[0]))
            up.extend(i for i, _ in positives[:10])
            down.extend(i for i, _ in negatives[:10])
            if not negatives:
                limitations.append(f"{market}: 当前趋势候选池无可验证负向区间收益，stocks_down 为空；并非全市场无下跌股票")
            if not board_items:
                limitations.append(f"{market}: {source_date} 无 TG 精选归档，正向候选来自趋势日线池")
    return markets, up, down, health, limitations


def stock_anomaly_snapshot(root, now):
    """Independent bidirectional 1d ranking over observed prices, not TG selection."""
    ledger = root / 'data/ledger.db'
    scan_dir = Path(os.getenv('AGENT_MARKET_SCAN_DIR') or root / 'data/agent_market_scan')
    scan = read_json(scan_dir / f'{now.date().isoformat()}.json', {})
    scanned = scan.get('markets', {}) if isinstance(scan, dict) else {}
    up, down, relative_up, relative_down, coverage = [], [], [], [], []
    if not ledger.exists():
        return up, down, relative_up, relative_down, [dict(market=m, quality='missing',
            negative_coverage_status='insufficient', missing_reason='ledger.db unavailable') for m in CURRENCY]
    catalog = {}
    meta = root / 'data/meta.db'
    if meta.exists():
        with closing(readonly_db(meta)) as db:
            for row in db.execute('SELECT symbol,asset_type FROM asset'):
                catalog[str(row['symbol'])] = row['asset_type']
    with closing(readonly_db(ledger)) as db:
        for market in CURRENCY:
            archive = db.execute('SELECT MAX(date) AS day FROM trenddailybar WHERE market=?', (market,)).fetchone()['day']
            bars = db.execute('SELECT symbol,name,close,amount,turnover_rate,source,updated_at FROM trenddailybar '
                              'WHERE market=? AND date=? AND close>0', (market, archive)).fetchall() if archive else []
            by_symbol = {}
            preference = {'heatmap': 3, 'daily_hot': 2, 'trend_pool_refresh_eod': 1}
            for bar in bars:
                sym = str(bar['symbol'])
                if sym not in by_symbol or preference.get(bar['source'], 0) > preference.get(by_symbol[sym]['source'], 0):
                    by_symbol[sym] = bar
            excluded = {}
            available = len(by_symbol) if market == 'US' else 0
            rows = []
            if market in ('CN', 'HK'):
                segment = scanned.get(market, {})
                if segment.get('session_date') == now.date().isoformat():
                    rows = segment.get('items') or []
                    excluded.update(segment.get('excluded_reason_counts') or {})
                    source_count = segment.get('source_universe_count')
                else:
                    source_count = None
                    excluded['agent_scan_unavailable'] = 1
            else:
                source_date = date.fromisoformat(archive) if archive else None
                seed_cutoff = (source_date - timedelta(days=120)).isoformat() if source_date else None
                seeds = {str(r['symbol']) for r in db.execute(
                    'SELECT symbol FROM trendseedpool WHERE market=? AND date>=?', (market, seed_cutoff))} if seed_cutoff else set()
                source_count = len(seeds | set(by_symbol))
                for symbol, bar in by_symbol.items():
                    price = number(bar['close'])
                    change = bar_returns(db, market, symbol, archive, price)['1d'] if price else None
                    if change is None:
                        excluded['insufficient_1d_history'] = excluded.get('insufficient_1d_history', 0) + 1
                        continue
                    rows.append(dict(symbol=symbol, name=bar['name'], price=price,
                                     change_pct=change, observed_at=None,
                                     source_checked_at=utc_naive_iso(bar['updated_at']),
                                     provider=f"TrendDailyBar:{bar['source']}", price_type='archived_latest'))
            items, contributions = [], {}
            for row in rows:
                symbol = str(row.get('symbol') or '')
                price, change = number(row.get('price')), number(row.get('change_pct'))
                if not symbol or not price or change is None:
                    excluded['invalid_ranking_input'] = excluded.get('invalid_ranking_input', 0) + 1
                    continue
                bar = by_symbol.get(symbol)
                asset_type, type_source = classify_stock_asset(symbol, row.get('name'), catalog.get(symbol))
                canonical = (symbol.split('.', 1)[1].upper()
                             if market == 'US' and re.fullmatch(r'\d{3}\.[A-Za-z][A-Za-z0-9.\-]*', symbol)
                             else symbol)
                leverage = {'SOXL': 3, 'SOXS': -3, 'TQQQ': 3, 'SQQQ': -3,
                            'SPXL': 3, 'SPXS': -3}.get(canonical.upper())
                amount = number(bar['amount']) if bar else None
                amount = amount if amount is not None and amount > 0 else None
                source = row.get('provider') or 'unknown'
                contributions[source] = contributions.get(source, 0) + 1
                session = archive
                item = dict(market=market, symbol=symbol, raw_symbol=symbol,
                            canonical_symbol=canonical, asset_id=f'{market}:{symbol}', exchange=None,
                            name=str(row.get('name') or ''), asset_type=asset_type,
                            instrument_type=asset_type, asset_type_source=type_source,
                            leverage_factor=leverage, underlying_asset=None,
                            direction='up' if change > 0 else 'down' if change < 0 else 'flat',
                            change_pct=round(change, 3), returns_pct={'1d': round(change, 3)},
                            price=price, price_type=row.get('price_type') or 'unknown',
                            price_adjustment='unknown_from_existing_source', currency=CURRENCY[market],
                            amount=amount, amount_unit=CURRENCY[market],
                            turnover_rate=number(bar['turnover_rate']) if bar and (number(bar['turnover_rate']) or 0) > 0 else None,
                            volatility_zscore=None, zscore_missing_reason='20 comparable daily returns not verified',
                            trend_score=None, relative_strength_pct=None,
                            relative_strength_basis='same-market eligible candidate median; not an index',
                            session_date=session, observed_at=row.get('observed_at'),
                            source_checked_at=row.get('source_checked_at'),
                            data_asof=row.get('observed_at') or row.get('source_checked_at'),
                            source=source, coverage_pool='independent Agent market scan',
                            ranking_method='observed_1d_return_not_zscore',
                            quality='partial', stale=False,
                            missing_reason='price adjustment or exact exchange not confirmed')
                items.append(item)
            sorted_returns = sorted(i['change_pct'] for i in items)
            median = sorted_returns[len(sorted_returns) // 2] if sorted_returns else None
            for item in items:
                item['relative_strength_pct'] = round(item['change_pct'] - median, 3) if median is not None else None
            positive = sorted((i for i in items if i['change_pct'] > 0), key=lambda i: i['change_pct'], reverse=True)
            negative = sorted((i for i in items if i['change_pct'] < 0), key=lambda i: i['change_pct'])
            up.extend(positive[:10]); down.extend(negative[:10])
            relative_up.extend(sorted(items, key=lambda i: i['relative_strength_pct'], reverse=True)[:3])
            relative_down.extend(sorted(items, key=lambda i: i['relative_strength_pct'])[:3])
            if market != 'US':
                available = len(rows)
            eligible = len(items)
            ratio = round(available / source_count, 3) if source_count else None
            enough = bool(source_count and ratio is not None and ratio >= 0.5 and eligible >= 10)
            coverage.append(dict(market=market, session_date=items[0]['session_date'] if items else None,
                                 data_asof=max((i['data_asof'] for i in items if i['data_asof']), default=None),
                                 source_universe_count=source_count, available_universe_count=available,
                                 eligible_universe_count=eligible,
                                 selected_count=len(positive[:10]) + len(negative[:10]),
                                 positive_eligible_count=len(positive), negative_eligible_count=len(negative),
                                 flat_eligible_count=eligible - len(positive) - len(negative),
                                 coverage_scope='rolling_120d_TrendSeedPool_union_current_archived_bars',
                                 selection_version='agent_anomaly_v1', coverage_ratio=ratio,
                                 excluded_reason_counts=excluded, source_contributions=contributions,
                                 negative_coverage_status=('insufficient' if not enough else
                                                           'none_observed' if not negative else 'complete'),
                                 negative_coverage_reason=(
                                     'independent scan coverage insufficient' if not enough else
                                     'no negative returns in eligible sample' if not negative else None),
                                 quality='partial' if enough else 'missing', stale=not enough,
                                 missing_reason=None if enough else 'independent quote scan absent or covered under half the candidate pool'))
    return up, down, relative_up, relative_down, coverage


def bar_return(db, item, window, fixed_hour=None):
    source = "dex:" + str(item.get("pair") or "") if item.get("market") == "onchain" else "binance"
    target = (fixed_hour if fixed_hour is not None else item["updated"]) - window
    baseline = db.execute("SELECT ts,price FROM bars WHERE id=? AND source=? AND ts<=? "
                          "ORDER BY ts DESC LIMIT 1", (item["id"], source, target)).fetchone()
    if not baseline or not number(baseline["price"]) or target - baseline["ts"] > 3700:
        return None
    current = number(item.get("price"))
    if fixed_hour is not None:
        current_row = db.execute("SELECT ts,price FROM bars WHERE id=? AND source=? AND ts<=? "
                                 "ORDER BY ts DESC LIMIT 1", (item["id"], source, fixed_hour)).fetchone()
        if not current_row or fixed_hour - current_row["ts"] > 3600:
            return None
        current = number(current_row["price"])
    return round((current / baseline["price"] - 1) * 100, 3) if current else None


def crypto_snapshot(db_path, now):
    result, health, limitations = [], [], []
    if not db_path.exists():
        return result, [dict(domain="crypto", session_date=now.date().isoformat(), data_asof=None,
                             source="alpha_news crypto_trend.sqlite", quality="missing", stale=True,
                             missing_reason="crypto database unavailable")], ["加密趋势数据库不可用"]
    with closing(readonly_db(db_path)) as db:
        for market in ("spot", "futures", "onchain"):
            hr = db.execute("SELECT updated,payload FROM health WHERE component=?", (market,)).fetchone()
            health_payload = json.loads(hr["payload"]) if hr else {}
            selected = set(health_payload.get("selected_symbols") or []) if market != "onchain" else None
            age = now.timestamp() - hr["updated"] if hr and hr["updated"] else None
            stale = age is None or age > CRYPTO_STALE_SECONDS
            health.append(dict(domain=f"crypto_{market}", session_date=now.date().isoformat(),
                               data_asof=epoch_iso(hr["updated"]) if hr else None,
                               source="alpha_news crypto_trend.sqlite/health",
                               quality="partial" if market == "onchain" or health_payload.get("status") != "ok" else "ok",
                               stale=stale, missing_reason="health unavailable" if not hr else None,
                               coverage="discovered contracts" if market == "onchain" else "Binance 24h quote-volume Top 150",
                               universe_count=health_payload.get("count")))
            rows = db.execute("SELECT a.payload,s.payload AS security_payload FROM assets a "
                              "LEFT JOIN security s ON a.id=s.id WHERE a.market=?", (market,)).fetchall()
            candidates = []
            for row in rows:
                item = json.loads(row["payload"])
                if selected is not None and item.get("symbol") not in selected:
                    continue
                updated = number(item.get("updated"))
                if not updated or now.timestamp() - updated > CRYPTO_STALE_SECONDS:
                    continue
                if market == "onchain":
                    security = json.loads(row["security_payload"]) if row["security_payload"] else {}
                    status = security.get("status", "unknown")
                    if status == "blocked":
                        continue
                    if status == "pass" and (number(security.get("expires_at")) or 0) < now.timestamp():
                        status = "unknown"
                    if status == "pass" and security.get("pair") != item.get("pair"):
                        status = "unknown"
                    if (number(item.get("liquidity")) or 0) < ONCHAIN_MIN_LIQUIDITY or (number(item.get("volume_24h")) or 0) < ONCHAIN_MIN_VOLUME:
                        continue
                    fixed_hour = int(now.timestamp() // 3600) * 3600 - 3600
                    change = bar_return(db, item, 86400, fixed_hour)
                    window = "24h_fixed_complete_hour"
                else:
                    status = None
                    change = bar_return(db, item, 43200)
                    window = "12h_hourly_history"
                if change is None:
                    continue
                oi_change = None
                if market == "futures":
                    history = item.get("oi_history") or []
                    if history:
                        end = history[-1]
                        target = end.get("ts", 0) - 43200
                        bases = [v for v in history if target - 7300 <= v.get("ts", 0) <= target]
                        old = number(bases[-1].get("quantity")) if bases else None
                        new = number(end.get("quantity"))
                        if old and new is not None:
                            oi_change = round((new / old - 1) * 100, 3)
                candidates.append(dict(market=market, exchange="Binance" if market != "onchain" else None,
                                       asset_type={"spot": "SPOT", "futures": "PERPETUAL", "onchain": "ONCHAIN_TOKEN"}[market],
                                       chain=item.get("chain") if market == "onchain" else None,
                                       symbol=item.get("symbol"), contract_address=item.get("address") if market == "onchain" else None,
                                       direction="up" if change > 0 else "down" if change < 0 else "flat",
                                       change_pct=change, window=window, price=number(item.get("price")),
                                       volume_24h=number(item.get("volume_24h")),
                                       liquidity=number(item.get("liquidity")) if market == "onchain" else None,
                                       security_status=status, oi_quantity_change_pct=oi_change,
                                       funding_rate_percent=number(item.get("funding_rate_percent")) if market == "futures" else None,
                                       data_asof=epoch_iso(updated), session_date=now.date().isoformat(),
                                       source=item.get("price_source") or "alpha_news crypto trend worker",
                                       coverage_pool="discovered contracts" if market == "onchain" else "Binance 24h quote-volume Top 150",
                                       price_adjustment="not_applicable",
                                       quality="partial" if status in ("unknown", "review") else "ok", stale=False))
            ups = sorted((v for v in candidates if v["direction"] == "up"), key=lambda v: v["change_pct"], reverse=True)
            downs = sorted((v for v in candidates if v["direction"] == "down"), key=lambda v: v["change_pct"])
            result.extend(ups[:5] + downs[:5])
            if not candidates:
                limitations.append(f"{market}: 无满足历史基准和新鲜度的加密候选")
    return result, health, limitations


def macro_snapshot(root, now):
    payload = read_json(root / "data/macro_dashboard_snapshot.json", {})
    if not isinstance(payload, dict):
        payload = {}
    observations = []
    for category, ids in (("macro", {"DGS2", "DGS10", "DFII10", "DFEDTARU", "DTWEXBGS"}),
                          ("futures", {"CL", "OIL", "GC", "SI", "HG"}),
                          ("global_assets", {"SOX", "XBI", "IBB", "EEM", "VNM", "EWZ", "INDA", "EWJ", "KWEB", "XLE"})):
        for item in payload.get(category, []):
            if not isinstance(item, dict) or item.get("id") not in ids:
                continue
            asof = item.get("asof")
            checked = number(payload.get(category + '_checked_at'))
            status, stale, reason, expected_day = macro_freshness(item.get('id'), asof, checked, now, category)
            ttl = 7200 if category == 'futures' else 86400
            collector_stale = checked is None or now.timestamp() - checked > ttl
            if collector_stale:
                stale = True
                reason = (reason + '; ' if reason else '') + 'source collector has not checked recently'
            quality = 'stale' if stale else 'partial' if status == 'unknown' else status
            observations.append(dict(category=category, id=item.get("id"), name=item.get("name"),
                                     value=number(item.get("value")), unit=item.get("unit"),
                                     change_pct=number(item.get("change_pct")), observed_at=asof,
                                     data_asof=asof, session_date=str(asof)[:10] if asof else None,
                                     source_checked_at=epoch_iso(checked),
                                     expected_observation_date=expected_day,
                                     freshness_status=status, source=item.get("source"), quality=quality,
                                     stale=stale, stale_reason=reason if stale else None,
                                     missing_reason=reason if status in ('missing', 'unknown') else None))
    activity = payload.get("crypto_activity") or {}
    crypto_activity = {}
    for key in ("spot", "perp", "dex", "sentiment"):
        entry = activity.get(key) or {}
        crypto_activity[key] = dict(value=number(entry.get("value")),
                                    previous_24h=number(entry.get("previous_24h")),
                                    pairs=entry.get("pairs"), classification=entry.get("classification"),
                                    data_asof=epoch_iso(entry.get("checked_at")),
                                    source="Binance" if key in ("spot", "perp") else "DefiLlama" if key == "dex" else "Alternative.me",
                                    quality="ok" if entry.get("value") is not None else "missing",
                                    stale=not entry.get("checked_at") or now.timestamp() - entry["checked_at"] > 86400,
                                    missing_reason=None if entry.get("value") is not None else "not in macro cache")
    checked = epoch_iso(payload.get("refreshed_at"))
    cache_stale = not payload.get("refreshed_at") or now.timestamp() - payload["refreshed_at"] > 86400
    stale_count = sum(bool(row['stale']) for row in observations)
    health = dict(domain="macro", session_date=now.date().isoformat(), data_asof=checked,
                  source_checked_at=checked, source="Trade_db macro_dashboard_snapshot.json",
                  quality="stale" if cache_stale else "partial" if observations else "missing",
                  stale=cache_stale, stale_indicator_count=stale_count,
                  missing_reason="macro cache unavailable" if not observations else
                  f'{stale_count} macro observations stale' if stale_count else None)
    return observations, crypto_activity, health


def build_snapshot(root, crypto_db, now):
    markets, legacy_up, legacy_down, stock_health, limitations = stock_snapshot(root, now)
    stocks_up, stocks_down, relative_up, relative_down, coverage = stock_anomaly_snapshot(root, now)
    coverage_by_market = {row['market']: row for row in coverage}
    for market in markets:
        row = coverage_by_market.get(market['market'], {})
        market.update({key: row.get(key) for key in (
            'source_universe_count', 'available_universe_count', 'eligible_universe_count',
            'selected_count', 'coverage_scope', 'coverage_ratio', 'excluded_reason_counts',
            'negative_coverage_status', 'selection_version')})
        market['candidate_universe_definition'] = 'current-day valid TrendDailyBar symbols at generation time; not full market'
        market['observed_at'] = row.get('data_asof')
        market['source_checked_at'] = row.get('data_asof')
    trend_selected = []
    for market in markets:
        board = read_json(root / 'data/tg_daily_boards' /
                          f"{market['market']}_{market.get('archive_date')}.json", {})
        for item in (board.get('items') or [])[:10] if isinstance(board, dict) else []:
            if not isinstance(item, dict):
                continue
            trend_selected.append(dict(market=market['market'], symbol=item.get('symbol'),
                                       name=item.get('name'), change_pct=number(item.get('change_pct')),
                                       trend_score=number(item.get('trend_score')),
                                       selection_source='TG daily selected board',
                                       session_date=market['session_date']))
    crypto, crypto_health, crypto_limits = crypto_snapshot(crypto_db, now)
    macro, crypto_activity, macro_health = macro_snapshot(root, now)
    limitations.extend(crypto_limits)
    limitations.extend(["Trend Selection与独立Market Anomaly Scan分开；后者仅为可观测样本的真实1日涨跌幅，不是全市场或Z-score",
                        "加密 CEX 仅覆盖 Binance USDT 现货/永续按 24h 成交额各前 150；链上仅覆盖已发现合约，安全通过不保证可卖出",
                        "主要综合指数与完整市场广度当前无可靠归档，保持 null；20:00 北京时间美股采用上一已完成纽约交易日",
                        "跨市场因果归因与交易评级未实现"])
    anomaly_health = [dict(domain=f"agent_anomaly_{row['market']}", session_date=row['session_date'],
                           data_asof=row['data_asof'], quality=row['quality'], stale=row['stale'],
                           source='independent Agent market scan', missing_reason=row['missing_reason']) for row in coverage]
    all_health = stock_health + anomaly_health + crypto_health + [macro_health]
    return dict(schema_version="1.2", snapshot_id=f"{now.date().isoformat()}-{int(now.timestamp())}",
                generated_at=now.isoformat(timespec="seconds"), timezone="Asia/Shanghai",
                snapshot_generated_at=now.isoformat(timespec="seconds"),
                session_date={m['market']: m['session_date'] for m in markets},
                data_asof={h['domain']: h['data_asof'] for h in all_health},
                stale=any(h['stale'] for h in all_health) or any(o['stale'] for o in macro), quality="partial",
                missing_reason="major-index/breadth and some baseline/adjustment fields unavailable; see limitations",
                overview=dict(markets=markets, macro=macro, crypto_activity=crypto_activity),
                trend_selected=trend_selected,
                market_anomalies=dict(stocks_up=stocks_up, stocks_down=stocks_down,
                                      relative_strong=relative_up, relative_weak=relative_down,
                                      coverage=coverage),
                anomalies=dict(stocks_up=stocks_up, stocks_down=stocks_down, crypto=crypto,
                               relative_strong=relative_up, relative_weak=relative_down,
                               cross_market=[]),
                source_health=all_health, limitations=limitations)


def ensure_daily_ready(snapshot, now, require_market_scan=True):
    """Refuse a weekday 'complete' publication when finished upstream sessions are absent."""
    if now.weekday() >= 5:
        raise ValueError("daily publication is for Monday-Friday only")
    today = now.date().isoformat()
    markets = {m["market"]: m for m in snapshot["overview"]["markets"]}
    for market in ("CN", "HK"):
        row = markets[market]
        if row["session_date"] != today or not row.get("candidate_universe"):
            raise ValueError(f"{market} completed session not ready for {today}")
        if row.get("turnover") is None:
            raise ValueError(f"{market} current-day turnover archive not ready")
        asof = row.get("data_asof")
        if not asof or datetime.fromisoformat(asof).astimezone(SHANGHAI).hour < 16:
            raise ValueError(f"{market} current-day data has no post-close observation")
        counts = row.get('archive_source_counts')
        if require_market_scan and counts is not None and not counts.get('trend_pool_refresh_eod'):
            raise ValueError(f"{market} current-day trend pool refresh not ready")
    us = markets["US"]
    us_asof = us.get("data_asof")
    if not us_asof or not us.get("candidate_universe"):
        raise ValueError("US completed session unavailable")
    ny_asof = datetime.fromisoformat(us_asof).astimezone(NEW_YORK)
    if (us["session_date"] != ny_asof.date().isoformat() or ny_asof.hour < 16
            or ny_asof.date() >= now.astimezone(NEW_YORK).date()
            or (now.astimezone(NEW_YORK).date() - ny_asof.date()).days > 4):
        raise ValueError("US archive is not a recent completed New York session")
    health = {h["domain"]: h for h in snapshot["source_health"]}
    for domain in ("crypto_spot", "crypto_futures", "crypto_onchain", "macro"):
        if health[domain]["stale"] or health[domain]["quality"] == "missing":
            raise ValueError(f"{domain} source not ready")
    if require_market_scan:
        for market in ('CN', 'HK', 'US'):
            row = next((r for r in snapshot.get('market_anomalies', {}).get('coverage', [])
                        if r.get('market') == market), None)
            if not row or row['negative_coverage_status'] == 'insufficient':
                raise ValueError(f'{market} independent anomaly scan coverage insufficient')


def generate(root, crypto_db, output, now=None, require_daily_ready=False):
    from agent_snapshot_health import audit, write_health
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("generation time must be timezone-aware")
    archive = output / "history" / f"{now.date().isoformat()}.json"
    if archive.exists():
        data = archive.read_bytes()
        existing = json.loads(data)
        if require_daily_ready:
            ensure_daily_ready(existing, now)
            failures = audit(existing, output, now)['failures']
            if failures:
                raise ValueError(f'existing archive failed Agent quality audit: {list(dict.fromkeys(failures))[:3]}')
        if len(data) > MAX_BYTES:
            raise ValueError("existing archive exceeds 220 KB")
    else:
        snapshot = build_snapshot(root, crypto_db, now)
        if require_daily_ready:
            ensure_daily_ready(snapshot, now)
            failures = audit(snapshot, output, now)['failures']
            if failures:
                raise ValueError(f'Agent quality audit failed: {list(dict.fromkeys(failures))[:3]}')
        data = (json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
        if len(data) > MAX_BYTES:
            raise ValueError("snapshot exceeds 220 KB")
        if not snapshot["overview"]["macro"] and not any(x["candidate_universe"] for x in snapshot["overview"]["markets"]):
            raise ValueError("no usable market data; latest not updated")
        immutably_create(archive, data)
        data = archive.read_bytes()
        json.loads(data)
    write_health(json.loads(data), output, now)
    atomically_write(output / "latest.json", data)
    return json.loads(data), len(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--crypto-db", type=Path, default=Path("/root/alpha_news/data/crypto_trend.sqlite"))
    parser.add_argument("--output", type=Path, required=True, help="directory containing latest.json and history/")
    args = parser.parse_args()
    snapshot, size = generate(args.root, args.crypto_db, args.output)
    print(f"{snapshot['snapshot_id']} {size} bytes -> {args.output}")


if __name__ == "__main__":
    main()
