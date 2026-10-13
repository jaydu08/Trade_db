"""Freeze a small, public-only Trend market snapshot from existing local archives.

No network calls, migrations, business API calls, or source-database writes.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import tempfile
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
NEW_YORK = ZoneInfo("America/New_York")
MAX_BYTES = 100_000
CRYPTO_STALE_SECONDS = 900
ONCHAIN_MIN_LIQUIDITY = 50_000
ONCHAIN_MIN_VOLUME = 50_000
CURRENCY = {"CN": "CNY", "HK": "HKD", "US": "USD"}


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
                                candidate_universe=len(latest)))
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
                item = dict(market=market, symbol=symbol, name=str(bar["name"] or ""),
                            session_date=session, data_asof=utc_naive_iso(bar["updated_at"]),
                            price=price, price_type="close_or_archived_latest", currency=CURRENCY[market],
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
            try:
                stale = (now.date() - date.fromisoformat(str(asof)[:10])).days > 7
            except ValueError:
                stale = True
            observations.append(dict(category=category, id=item.get("id"), name=item.get("name"),
                                     value=number(item.get("value")), unit=item.get("unit"),
                                     change_pct=number(item.get("change_pct")), observed_at=asof,
                                     data_asof=asof, session_date=str(asof)[:10] if asof else None,
                                     source=item.get("source"), quality="partial" if stale else "ok",
                                     stale=stale, missing_reason="observed_at unavailable" if not asof else None))
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
    health = dict(domain="macro", session_date=now.date().isoformat(), data_asof=checked,
                  source="Trade_db macro_dashboard_snapshot.json", quality="partial" if observations else "missing",
                  stale=not payload.get("refreshed_at") or now.timestamp() - payload["refreshed_at"] > 86400,
                  missing_reason=None if observations else "macro cache unavailable")
    return observations, crypto_activity, health


def build_snapshot(root, crypto_db, now):
    markets, stocks_up, stocks_down, stock_health, limitations = stock_snapshot(root, now)
    crypto, crypto_health, crypto_limits = crypto_snapshot(crypto_db, now)
    macro, crypto_activity, macro_health = macro_snapshot(root, now)
    limitations.extend(crypto_limits)
    limitations.extend(["股票候选池不是全市场；负向候选仅为可验证区间跌幅排序，未做波动率标准化",
                        "加密 CEX 仅覆盖 Binance USDT 现货/永续按 24h 成交额各前 150；链上仅覆盖已发现合约，安全通过不保证可卖出",
                        "主要综合指数与完整市场广度当前无可靠归档，保持 null；20:00 北京时间美股采用上一已完成纽约交易日",
                        "跨市场因果归因与交易评级未实现"])
    return dict(schema_version="1.0", snapshot_id=f"{now.date().isoformat()}-{int(now.timestamp())}",
                generated_at=now.isoformat(timespec="seconds"), timezone="Asia/Shanghai",
                overview=dict(markets=markets, macro=macro, crypto_activity=crypto_activity),
                anomalies=dict(stocks_up=stocks_up, stocks_down=stocks_down, crypto=crypto, cross_market=[]),
                source_health=stock_health + crypto_health + [macro_health], limitations=limitations)


def generate(root, crypto_db, output, now=None):
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("generation time must be timezone-aware")
    archive = output / "history" / f"{now.date().isoformat()}.json"
    if archive.exists():
        data = archive.read_bytes()
        json.loads(data)
        if len(data) > MAX_BYTES:
            raise ValueError("existing archive exceeds 100 KB")
    else:
        snapshot = build_snapshot(root, crypto_db, now)
        data = (json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
        if len(data) > MAX_BYTES:
            raise ValueError("snapshot exceeds 100 KB")
        if not snapshot["overview"]["macro"] and not any(x["candidate_universe"] for x in snapshot["overview"]["markets"]):
            raise ValueError("no usable market data; latest not updated")
        immutably_create(archive, data)
        data = archive.read_bytes()
        json.loads(data)
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
