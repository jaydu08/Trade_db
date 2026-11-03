"""Date-bound broad-index returns for the stock daily shortlist."""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import statistics
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = ROOT / "data" / "market_relative_strength"
INDEXES = {"CN": ("sh000300", "沪深300"), "HK": ("hkHSI", "恒生指数"), "US": ("gb_inx", "标普500")}
FALLBACK_AMOUNT = {"CN": 20_000_000, "HK": 10_000_000, "US": 5_000_000}
FALLBACK_MIN_ROWS = {"CN": 300, "HK": 100, "US": 250}


def _finite(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def fetch_index_return(market: str, target_date: dt.date):
    """Use the same Sina quote source as the daily stock scan, only if fresh."""
    symbol, name = INDEXES[market]
    response = requests.get(
        f"http://hq.sinajs.cn/list={symbol}",
        headers={"Referer": "http://finance.sina.com.cn"},
        timeout=8,
    )
    response.raise_for_status()
    response.encoding = "gbk"
    raw = response.text.split('="', 1)[1].split('";', 1)[0]
    parts = raw.split(",")
    if market == "CN":
        previous, current = _finite(parts[2]), _finite(parts[3])
        pct = (current / previous - 1) * 100 if previous and current else None
        asof = f"{parts[30]} {parts[31]}"
    elif market == "HK":
        pct = _finite(parts[8])
        asof = f"{parts[17]} {parts[18]}"
    else:
        pct = _finite(parts[2])
        asof = parts[3]
    if pct is None or str(target_date) not in asof.replace("/", "-"):
        raise ValueError(f"stale or invalid {market} benchmark quote: {asof}")
    return {"return_pct": round(pct, 4), "basis": name, "source": "Sina index quote", "asof": asof}


def liquid_stock_median(frame, market: str):
    """Fallback only: same-session median of liquid ordinary shares, not an index."""
    amount_floor = FALLBACK_AMOUNT[market]
    returns = []
    for row in frame.to_dict("records"):
        name = str(row.get("名称", "") or "").lower()
        if any(word in name for word in ("etf", "基金", "两倍", "三倍", "杠杆", "反向", "做空", "做多")):
            continue
        amount = _finite(row.get("成交额"))
        price = _finite(row.get("最新价"))
        pct = _finite(row.get("涨跌幅"))
        if amount is not None and amount >= amount_floor and price and price > 0 and pct is not None and abs(pct) <= 40:
            returns.append(pct)
    if len(returns) < FALLBACK_MIN_ROWS[market]:
        return None
    return {
        "return_pct": round(statistics.median(returns), 4),
        "basis": "同日流动性普通股涨幅中位数（非指数）",
        "source": "Sina stock quote cross-section",
        "sample_size": len(returns),
    }


def build_benchmark(frame, market: str, target_date: dt.date):
    try:
        result = fetch_index_return(market, target_date)
    except (requests.RequestException, ValueError, IndexError) as exc:
        result = liquid_stock_median(frame, market)
        if result is None:
            return None
        result["index_error"] = str(exc)[:160]
    return {"market": market, "date": target_date.isoformat(), **result}


def save_benchmark(payload: dict):
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    path = BENCHMARK_DIR / f"{payload['market']}_{payload['date']}.json"
    tmp = path.with_suffix(f".json.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def load_benchmark(market: str, target_date: dt.date):
    path = BENCHMARK_DIR / f"{market}_{target_date.isoformat()}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("market") == market and payload.get("date") == target_date.isoformat() and _finite(payload.get("return_pct")) is not None:
            return payload
    except (OSError, ValueError, TypeError):
        pass
    return None


def qualifies(market: str, symbol: str, stock_return_pct: float, benchmark_pct: float) -> bool:
    """Require positive absolute performance and genuine outperformance."""
    stock_return_pct = _finite(stock_return_pct)
    benchmark_pct = _finite(benchmark_pct)
    if stock_return_pct is None or benchmark_pct is None:
        return False
    if market == "CN":
        if str(symbol).startswith(("30", "688")):
            absolute_floor, relative_floor = 3.5, 4.0
        elif str(symbol).startswith(("8", "4")):
            absolute_floor, relative_floor = 4.0, 5.0
        else:
            absolute_floor, relative_floor = 2.0, 3.0
    elif market == "HK":
        absolute_floor, relative_floor = 1.0, 2.5
    elif market == "US":
        absolute_floor, relative_floor = 1.5, 2.5
    else:
        return False
    return stock_return_pct >= absolute_floor and stock_return_pct - benchmark_pct >= relative_floor
