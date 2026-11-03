import concurrent.futures
import csv
import io
import json
import re
import zipfile
import datetime as dt
import logging
import os
import threading
import time
from typing import Dict, List, Tuple
from pathlib import Path
from urllib.parse import urljoin

import requests

from core.cache import get_cache, set_cache
from config.settings import DATA_DIR

logger = logging.getLogger(__name__)

_TUSHARE_API = "http://api.tushare.pro"
_INST_SNAPSHOT_TTL_SEC = int(os.getenv("INST_SNAPSHOT_TTL_SEC", "43200") or 43200)  # 12h
_INST_SNAPSHOT_STALE_SEC = int(os.getenv("INST_SNAPSHOT_STALE_SEC", "172800") or 172800)  # 48h stale fallback
_INST_US_CACHE_TTL_SEC = int(os.getenv("INST_US_CACHE_TTL_SEC", "21600") or 21600)  # 6h
_INST_US_LOOKBACK_DAYS = int(os.getenv("INST_US_LOOKBACK_DAYS", "45") or 45)
_INST_US_MAX_SYMBOLS = int(os.getenv("TREND_INST_US_MAX_SYMBOLS", "8") or 8)

# hk_hold 接口流控（低频账号常见 2 次/分钟）
_HK_HOLD_CALL_LIMIT = int(os.getenv("HK_HOLD_CALL_LIMIT_PER_MIN", "2") or 2)
_HK_HOLD_WINDOW_SEC = int(os.getenv("HK_HOLD_WINDOW_SEC", "62") or 62)
_HK_HOLD_CALLS: List[float] = []
_HK_HOLD_LOCK = threading.Lock()
_SNAP_REFRESHING: set[str] = set()
_SNAP_LOCK = threading.Lock()

_CAPITAL_SIGNAL_TTL_SEC = int(os.getenv("CAPITAL_SIGNAL_TTL_SEC", "86400") or 86400)  # 24h
_CAPITAL_SIGNAL_STALE_SEC = int(os.getenv("CAPITAL_SIGNAL_STALE_SEC", "1209600") or 1209600)  # 14d stale fallback
_CAPITAL_REFRESH_MAX_SYMBOLS = int(os.getenv("CAPITAL_REFRESH_MAX_SYMBOLS", "4") or 4)
_CAPITAL_REFRESHING: set[str] = set()
_CAPITAL_LOCK = threading.Lock()

_INSTITUTIONAL_CACHE_DIR = Path(os.getenv("INSTITUTIONAL_CACHE_DIR", str(DATA_DIR / "institutional")))
_SEC13F_ENABLED = os.getenv("SEC13F_ENABLED", "1") == "1"
_SEC13F_PAGE = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
_SEC13F_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
_SEC13F_CACHE_TTL_SEC = int(os.getenv("SEC13F_CACHE_TTL_SEC", "604800") or 604800)  # 7d page/ticker refresh
_SEC13F_REFRESH_MAX_PERIODS = int(os.getenv("SEC13F_REFRESH_MAX_PERIODS", "2") or 2)
_SEC13F_REFRESHING = False
_SEC13F_LOCK = threading.Lock()

_HK_CCASS_ENABLED = os.getenv("HK_CCASS_ENABLED", "1") == "1"
_HK_CCASS_CACHE_TTL_SEC = int(os.getenv("HK_CCASS_CACHE_TTL_SEC", "86400") or 86400)  # 1d
_HK_CCASS_STALE_SEC = int(os.getenv("HK_CCASS_STALE_SEC", "1209600") or 1209600)  # 14d fallback
_HK_CCASS_REFRESHING = False
_HK_CCASS_LOCK = threading.Lock()


def _to_float(v) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return 0.0


def _fmt_date_ymd(raw: str) -> str:
    s = str(raw or "").strip().replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return ""


def _fmt_cn_ts_code(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    if "." in s:
        head, tail = s.split(".", 1)
        if tail in {"SH", "SZ", "BJ"}:
            return f"{head}.{tail}"
        s = head
    if s.startswith(("6", "9")):
        return f"{s}.SH"
    if s.startswith(("8", "4")):
        return f"{s}.BJ"
    return f"{s}.SZ"


def _fmt_hk_ts_code(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".HK"):
        s = s[:-3]
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    return f"{digits.zfill(5)}.HK"


def _fmt_us_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if "." in s:
        s = s.split(".")[-1].strip()
    return s


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _allow_hk_hold_call() -> bool:
    now = time.time()
    with _HK_HOLD_LOCK:
        while _HK_HOLD_CALLS and (now - _HK_HOLD_CALLS[0]) > _HK_HOLD_WINDOW_SEC:
            _HK_HOLD_CALLS.pop(0)
        if len(_HK_HOLD_CALLS) >= _HK_HOLD_CALL_LIMIT:
            return False
        _HK_HOLD_CALLS.append(now)
        return True


def _call_tushare_hk_hold(params: Dict, fields: str = "ts_code,trade_date,ratio,exchange") -> Dict:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        return {}
    if not _allow_hk_hold_call():
        logger.info("hk_hold local rate gate hit; use cached snapshot")
        return {}

    payload = {
        "api_name": "hk_hold",
        "token": token,
        "params": params,
        "fields": fields,
    }
    try:
        resp = requests.post(_TUSHARE_API, json=payload, timeout=6)
        data = resp.json() if resp is not None else {}
        if not isinstance(data, dict):
            return {}

        code = int(data.get("code", -1))
        if code != 0:
            msg = str(data.get("msg", "") or "")
            if "频率超限" in msg:
                logger.warning("hk_hold rate limited by tushare: %s", msg)
            else:
                logger.info("hk_hold unavailable: code=%s msg=%s", code, msg)
            return {}

        raw = data.get("data") or {}
        fields = raw.get("fields") or []
        items = raw.get("items") or []
        if not fields or not items:
            return {}
        return {"fields": fields, "items": items}
    except Exception as e:
        logger.debug("hk_hold request failed: %s", e)
        return {}


def _snapshot_from_rows(fields: List[str], rows: List[List]) -> Tuple[str, Dict[str, float]]:
    out: Dict[str, Tuple[str, float]] = {}
    try:
        idx_code = fields.index("ts_code")
    except Exception:
        idx_code = 0
    try:
        idx_date = fields.index("trade_date")
    except Exception:
        idx_date = 1
    try:
        idx_ratio = fields.index("ratio")
    except Exception:
        idx_ratio = 2

    for r in rows:
        try:
            code = str(r[idx_code] or "").strip().upper()
            d = str(r[idx_date] or "").strip()
            ratio = _to_float(r[idx_ratio])
            if not code or not d:
                continue
            prev = out.get(code)
            if prev is None or d > prev[0]:
                out[code] = (d, ratio)
        except Exception:
            continue

    if not out:
        return "", {}

    latest_date = max(v[0] for v in out.values())
    ratio_map = {k: float(v[1]) for k, v in out.items()}
    return latest_date, ratio_map


def _read_snapshot(market: str) -> Dict:
    return get_cache(f"inst_snapshot:{market}") or {}


def _write_snapshot(market: str, payload: Dict):
    set_cache(f"inst_snapshot:{market}", payload, ttl=max(_INST_SNAPSHOT_STALE_SEC, _INST_SNAPSHOT_TTL_SEC))


def _refresh_cn_snapshot() -> Dict:
    today = dt.date.today()
    start = (today - dt.timedelta(days=190)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    combined_map: Dict[str, float] = {}
    latest_dates = []

    for exch in ("SH", "SZ"):
        res = _call_tushare_hk_hold(
            {"start_date": start, "end_date": end, "exchange": exch},
            fields="ts_code,trade_date,ratio,exchange",
        )
        if not res:
            continue
        d, ratio_map = _snapshot_from_rows(res.get("fields", []), res.get("items", []))
        if d and ratio_map:
            latest_dates.append(d)
            combined_map.update(ratio_map)

    if not combined_map:
        return {}

    asof = max(latest_dates) if latest_dates else ""
    return {
        "date": _fmt_date_ymd(asof),
        "map": combined_map,
        "updated_ts": time.time(),
        "source": "tushare.hk_hold",
    }


def _refresh_hk_snapshot() -> Dict:
    today = dt.date.today()
    dates = [today, today - dt.timedelta(days=1)]

    for d in dates:
        res = _call_tushare_hk_hold(
            {"trade_date": d.strftime("%Y%m%d"), "exchange": "HK"},
            fields="ts_code,trade_date,ratio,exchange",
        )
        if not res:
            continue
        asof, ratio_map = _snapshot_from_rows(res.get("fields", []), res.get("items", []))
        if asof and ratio_map:
            return {
                "date": _fmt_date_ymd(asof),
                "map": ratio_map,
                "updated_ts": time.time(),
                "source": "tushare.hk_hold",
            }
    return {}


def _trigger_snapshot_refresh(market: str):
    mkt = str(market or "").strip().upper()
    if mkt not in {"CN", "HK"}:
        return

    with _SNAP_LOCK:
        if mkt in _SNAP_REFRESHING:
            return
        _SNAP_REFRESHING.add(mkt)

    def _worker():
        try:
            old = _read_snapshot(mkt)
            fresh = _refresh_cn_snapshot() if mkt == "CN" else _refresh_hk_snapshot()
            if fresh and fresh.get("map"):
                prev = {
                    "date": str((old or {}).get("date", "") or ""),
                    "map": (old or {}).get("map") or {},
                }
                fresh["prev"] = prev
                _write_snapshot(mkt, fresh)
        except Exception as e:
            logger.debug("inst snapshot async refresh failed: market=%s err=%s", mkt, e)
        finally:
            with _SNAP_LOCK:
                _SNAP_REFRESHING.discard(mkt)

    threading.Thread(target=_worker, name=f"inst-snapshot-{mkt.lower()}", daemon=True).start()


def _ensure_snapshot(market: str) -> Dict:
    cache = _read_snapshot(market)
    now = time.time()
    ts = float((cache or {}).get("updated_ts", 0) or 0)

    if cache and (now - ts) <= _INST_SNAPSHOT_TTL_SEC:
        return cache

    _trigger_snapshot_refresh(market)

    # Trend page must stay responsive; refresh slow institutional snapshots in background.
    if cache and (now - ts) <= _INST_SNAPSHOT_STALE_SEC:
        return cache
    return {}


def _delta_to_factor(delta_abs: float) -> float:
    # 0.2 percentage-point change is roughly one factor point, clipped to [-5, 5].
    return round(_clip(delta_abs / 0.2, -5.0, 5.0), 2)


def _relative_delta(start_value: float, end_value: float) -> float:
    base = abs(float(start_value or 0.0))
    if base <= 1e-9:
        return 0.0
    return ((float(end_value or 0.0) - float(start_value or 0.0)) / base) * 100.0


def _build_item(start_value: float, end_value: float, start_date: str, end_date: str, source: str, mode: str) -> Dict:
    start_value = float(start_value or 0.0)
    end_value = float(end_value or 0.0)
    delta_abs = end_value - start_value
    delta_pct = _relative_delta(start_value, end_value)
    factor = _delta_to_factor(delta_abs)

    if delta_abs > 0.03:
        direction = "增持"
    elif delta_abs < -0.03:
        direction = "减持"
    else:
        direction = "持平"

    label = "北向" if mode == "northbound" else ("南向" if mode == "southbound" else "机构")
    prefix = f"{label}持股"
    text = f"{prefix}{direction} {delta_abs:+.2f}个百分点"
    if start_value > 0 or end_value > 0:
        text += f"，{start_value:.2f}%→{end_value:.2f}%"
        if abs(delta_pct) > 0:
            text += f"，相对{delta_pct:+.1f}%"
    if end_date:
        text += f" ({end_date})"

    display_value = f"{delta_abs:+.2f}%" if abs(delta_abs) >= 0.005 else f"{end_value:.2f}%"
    item = {
        "key": f"{mode}_holding",
        "label": label,
        "value": display_value,
        "direction": direction,
        "score": factor,
        "source": source,
        "asof": end_date,
        "delta_abs": round(delta_abs, 4),
        "delta_pct": round(delta_pct, 2),
        "current": round(end_value, 4),
        "tooltip": text,
    }

    return {
        "inst_factor": factor,
        "inst_label": label,
        "inst_change_pp": round(delta_abs, 4),
        "inst_delta_abs": round(delta_abs, 4),
        "inst_delta_pct": round(delta_pct, 2),
        "inst_start_value": round(start_value, 4),
        "inst_end_value": round(end_value, 4),
        "inst_start_date": start_date,
        "inst_end_date": end_date,
        "inst_metric_unit": "percentage_point",
        "inst_date": end_date,
        "inst_source": source,
        "inst_text": text,
        "inst_direction": direction,
        "inst_holding": {
            "metric": "holding_ratio",
            "label": label,
            "start_value": round(start_value, 4),
            "end_value": round(end_value, 4),
            "delta_abs": round(delta_abs, 4),
            "delta_pct": round(delta_pct, 2),
            "start_date": start_date,
            "end_date": end_date,
            "unit": "percent",
            "source": source,
        },
        "capital_signal": {
            "score": factor,
            "items": [item] if end_value > 0 else [],
        },
    }


def _get_cn_hk_changes(market: str, symbols: List[str]) -> Dict[str, Dict]:
    snapshot = _ensure_snapshot(market)
    if not snapshot:
        return {}

    curr_map = snapshot.get("map") or {}
    prev_pack = snapshot.get("prev") or {}
    prev_map = prev_pack.get("map") or {}
    asof = str(snapshot.get("date", "") or "")
    prev_date = str(prev_pack.get("date", "") or "")
    source = str(snapshot.get("source", "tushare.hk_hold") or "tushare.hk_hold")

    out: Dict[str, Dict] = {}
    for sym in symbols:
        raw = str(sym or "").strip()
        if not raw:
            continue
        ts_code = _fmt_cn_ts_code(raw) if market == "CN" else _fmt_hk_ts_code(raw)
        if not ts_code:
            continue

        curr = _to_float(curr_map.get(ts_code, 0))
        prev = _to_float(prev_map.get(ts_code, curr))
        if curr <= 0 and prev <= 0:
            continue

        mode = "northbound" if market == "CN" else "southbound"
        out[raw] = _build_item(
            start_value=prev,
            end_value=curr,
            start_date=prev_date,
            end_date=asof,
            source=source,
            mode=mode,
        )
    return out



def _quiet_call(fn, *args, **kwargs):
    import contextlib
    import io

    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return fn(*args, **kwargs)


def _capital_cache_key(market: str, symbol: str) -> str:
    return f"capital_signal:{str(market or '').upper()}:{str(symbol or '').strip().upper()}"


def _is_capital_fresh(payload: Dict) -> bool:
    ts = float((payload or {}).get("updated_ts", 0) or 0)
    return bool(payload) and (time.time() - ts) <= _CAPITAL_SIGNAL_TTL_SEC


def _is_capital_usable(payload: Dict) -> bool:
    ts = float((payload or {}).get("updated_ts", 0) or 0)
    return bool(payload) and (time.time() - ts) <= _CAPITAL_SIGNAL_STALE_SEC


def _fmt_pct(v: float, digits: int = 2) -> str:
    return f"{float(v or 0):+.{digits}f}%"


def _signal_item(key: str, label: str, value: str, direction: str, score: float, source: str, asof: str = "", tooltip: str = "", **extra) -> Dict:
    item = {
        "key": key,
        "label": label,
        "value": value,
        "direction": direction,
        "score": round(float(score or 0.0), 2),
        "source": source,
        "asof": asof,
        "tooltip": tooltip,
    }
    item.update(extra)
    return item


def _capital_coverage(market: str, payload: Dict) -> Dict:
    mkt = str(market or '').strip().upper()
    sig = payload.get('capital_signal') if isinstance(payload, dict) else {}
    items = sig.get('items') if isinstance(sig, dict) else []
    items = [x for x in (items or []) if isinstance(x, dict)]
    keys = {str(x.get('key') or x.get('source') or x.get('label') or '') for x in items}
    labels = {str(x.get('label') or '') for x in items}

    expected = {
        'CN': [('户数', '股东户数'), ('基金', '基金持仓'), ('机构', '机构持仓'), ('北向', '北向持仓'), ('主力', '主力资金')],
        'HK': [('CCASS', 'CCASS变动'), ('集中', 'CCASS集中度'), ('南向', '南向持仓')],
        'US': [('13F', 'SEC 13F'), ('内部人', '内部人交易')],
    }.get(mkt, [])

    hit = []
    missing = []
    for label, title in expected:
        ok = label in labels or any(label.lower() in k.lower() for k in keys)
        (hit if ok else missing).append(title)

    if not expected:
        status = 'unknown'
        text = '未知'
        reason = '该市场暂未定义筹码源覆盖规则'
    elif len(hit) >= len(expected):
        status = 'complete'
        text = '完整'
        reason = '已覆盖' + '、'.join(hit)
    elif hit:
        status = 'partial'
        text = '部分'
        reason = '已覆盖' + '、'.join(hit) + '；缺' + '、'.join(missing)
    else:
        status = 'missing'
        text = '缺失'
        reason = '暂无可用筹码披露；缺' + '、'.join(missing) if missing else '暂无可用筹码披露'

    asof_values = [str(x.get('asof') or '') for x in items if str(x.get('asof') or '').strip()]
    return {
        'status': status,
        'text': text,
        'reason': reason,
        'hit': hit,
        'missing': missing,
        'item_count': len(items),
        'latest_asof': max(asof_values) if asof_values else '',
    }


def _merge_payload(base: Dict, extra: Dict) -> Dict:
    out = dict(base or {})
    items: List[Dict] = []
    score = 0.0
    for payload in (base or {}, extra or {}):
        sig = payload.get("capital_signal") if isinstance(payload, dict) else None
        if isinstance(sig, dict):
            score += float(sig.get("score", 0) or 0)
            raw_items = sig.get("items") or []
            if isinstance(raw_items, list):
                items.extend([x for x in raw_items if isinstance(x, dict)])
    seen = set()
    deduped = []
    for item in items:
        key = str(item.get("key") or item.get("label") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    deduped.sort(key=lambda x: (abs(float(x.get("score", 0) or 0)), 0 if str(x.get("direction", "")) == "持平" else 1), reverse=True)
    out["capital_signal"] = {"score": round(_clip(score, -12.0, 12.0), 2), "items": deduped}
    return out


def _cn_market_code(symbol: str) -> str:
    s = str(symbol or "").strip()
    if s.startswith(("6", "9")):
        return "sh"
    if s.startswith(("8", "4")):
        return "bj"
    return "sz"


def _recent_quarter_codes(limit: int = 8) -> List[str]:
    today = dt.date.today()
    q = ((today.month - 1) // 3) + 1
    year = today.year
    out = []
    for _ in range(max(1, limit)):
        out.append(f"{year}{q}")
        q -= 1
        if q <= 0:
            q = 4
            year -= 1
    return out


def _latest_two_groups(df, date_col: str):
    if df is None or getattr(df, "empty", True) or date_col not in df.columns:
        return []
    try:
        work = df.copy()
        work["__date"] = work[date_col].astype(str)
        dates = sorted([d for d in work["__date"].dropna().unique() if str(d).strip()])
        return [(d, work[work["__date"] == d]) for d in dates[-2:]]
    except Exception:
        return []


def _cn_holder_count_item(ak, symbol: str) -> Dict:
    try:
        df = _quiet_call(ak.stock_zh_a_gdhs_detail_em, symbol=symbol)
        if df is None or df.empty:
            return {}
        df = df.sort_values("股东户数统计截止日")
        latest = df.iloc[-1]
        curr = _to_float(latest.get("股东户数-本次", 0))
        prev = _to_float(latest.get("股东户数-上次", 0))
        if prev <= 0 and len(df) >= 2:
            prev = _to_float(df.iloc[-2].get("股东户数-本次", 0))
        if curr <= 0 or prev <= 0:
            return {}
        delta_pct = _to_float(latest.get("股东户数-增减比例", 0))
        if not delta_pct:
            delta_pct = ((curr - prev) / prev) * 100.0
        asof = _fmt_date_ymd(str(latest.get("股东户数统计截止日", "")))
        direction = "集中" if delta_pct < -0.5 else ("分散" if delta_pct > 0.5 else "持平")
        score = _clip(-delta_pct / 5.0, -4.0, 4.0)
        tooltip = f"股东户数 {prev:.0f}→{curr:.0f}，变化{delta_pct:+.1f}%"
        if asof:
            tooltip += f" ({asof})"
        return _signal_item("holder_count", "户数", _fmt_pct(delta_pct, 1), direction, score, "akshare.stock_zh_a_gdhs_detail_em", asof, tooltip, delta_pct=round(delta_pct, 2), start_value=round(prev, 2), end_value=round(curr, 2))
    except Exception as e:
        logger.debug("cn holder count failed: %s %s", symbol, e)
        return {}


def _cn_fund_holder_item(ak, symbol: str) -> Dict:
    try:
        df = _quiet_call(ak.stock_fund_stock_holder, symbol=symbol)
        groups = _latest_two_groups(df, "截止日期")
        if not groups:
            return {}
        latest_date, latest_df = groups[-1]
        prev_date, prev_df = groups[-2] if len(groups) >= 2 else ("", None)
        curr = sum(_to_float(v) for v in latest_df.get("占流通股比例", []))
        prev = sum(_to_float(v) for v in prev_df.get("占流通股比例", [])) if prev_df is not None else curr
        if curr <= 0 and prev <= 0:
            return {}
        delta_abs = curr - prev
        delta_pct = _relative_delta(prev, curr)
        direction = "增持" if delta_abs > 0.03 else ("减持" if delta_abs < -0.03 else "持平")
        score = _clip(delta_abs / 0.5, -4.0, 4.0)
        asof = _fmt_date_ymd(str(latest_date))
        tooltip = f"基金持股 {prev:.2f}%→{curr:.2f}%"
        if asof:
            tooltip += f" ({asof})"
        tooltip += f"，基金数{len(latest_df)}"
        return _signal_item("fund_holding", "基金", _fmt_pct(delta_abs, 2) if abs(delta_abs) >= 0.005 else f"{curr:.2f}%", direction, score, "akshare.stock_fund_stock_holder", asof, tooltip, delta_abs=round(delta_abs, 4), delta_pct=round(delta_pct, 2), start_value=round(prev, 4), end_value=round(curr, 4), count=int(len(latest_df)))
    except Exception as e:
        logger.debug("cn fund holder failed: %s %s", symbol, e)
        return {}


def _cn_institute_holder_item(ak, symbol: str) -> Dict:
    try:
        found = []
        for q in _recent_quarter_codes(8):
            try:
                df = _quiet_call(ak.stock_institute_hold_detail, stock=symbol, quarter=q)
            except Exception:
                df = None
            if df is not None and not df.empty:
                ratio = sum(_to_float(v) for v in df.get("占流通股比例", []))
                types = sorted(set(str(x) for x in df.get("持股机构类型", []) if str(x).strip()))
                found.append((q, ratio, len(df), types))
            if len(found) >= 2:
                break
        if not found:
            return {}
        latest_q, curr, cnt, types = found[0]
        prev = found[1][1] if len(found) >= 2 else curr
        delta_abs = curr - prev
        delta_pct = _relative_delta(prev, curr)
        direction = "增持" if delta_abs > 0.03 else ("减持" if delta_abs < -0.03 else "持平")
        score = _clip(delta_abs / 0.5, -4.0, 4.0)
        tooltip = f"机构持股 {prev:.2f}%→{curr:.2f}% ({latest_q})，机构数{cnt}"
        if types:
            tooltip += "，" + "/".join(types[:4])
        return _signal_item("institution_holding", "机构", _fmt_pct(delta_abs, 2) if abs(delta_abs) >= 0.005 else f"{curr:.2f}%", direction, score, "akshare.stock_institute_hold_detail", latest_q, tooltip, delta_abs=round(delta_abs, 4), delta_pct=round(delta_pct, 2), start_value=round(prev, 4), end_value=round(curr, 4), count=int(cnt))
    except Exception as e:
        logger.debug("cn institute holder failed: %s %s", symbol, e)
        return {}


def _cn_main_flow_item(ak, symbol: str) -> Dict:
    try:
        df = _quiet_call(ak.stock_individual_fund_flow, stock=symbol, market=_cn_market_code(symbol))
        if df is None or df.empty:
            return {}
        latest = df.iloc[-1]
        pct = _to_float(latest.get("主力净流入-净占比", 0))
        amount = _to_float(latest.get("主力净流入-净额", 0))
        asof = _fmt_date_ymd(str(latest.get("日期", "")))
        direction = "流入" if pct > 0.3 else ("流出" if pct < -0.3 else "持平")
        score = _clip(pct / 5.0, -3.0, 3.0)
        tooltip = f"主力净流入占比{pct:+.1f}%"
        if amount:
            tooltip += f"，净额{amount / 100000000:+.2f}亿"
        if asof:
            tooltip += f" ({asof})"
        return _signal_item("main_flow", "主力", _fmt_pct(pct, 1), direction, score, "akshare.stock_individual_fund_flow", asof, tooltip, delta_pct=round(pct, 2), amount=round(amount, 2))
    except Exception as e:
        logger.debug("cn main flow failed: %s %s", symbol, e)
        return {}


def _refresh_cn_capital_payload(symbol: str) -> Dict:
    try:
        import akshare as ak
    except Exception:
        return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}
    items = []
    for fn in (_cn_holder_count_item, _cn_fund_holder_item, _cn_institute_holder_item, _cn_main_flow_item):
        item = fn(ak, symbol)
        if item:
            items.append(item)
    score = sum(float(x.get("score", 0) or 0) for x in items)
    return {"updated_ts": time.time(), "capital_signal": {"score": round(_clip(score, -12.0, 12.0), 2), "items": items}}



def _institutional_dir() -> Path:
    _INSTITUTIONAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _INSTITUTIONAL_CACHE_DIR


def _load_json_file(path: Path) -> Dict:
    try:
        if not path.exists():
            return {}
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.debug('json cache read failed: path=%s err=%s', path, e)
        return {}


def _write_json_file(path: Path, payload: Dict):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + '.tmp')
        with tmp.open('w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        tmp.replace(path)
    except Exception as e:
        logger.debug('json cache write failed: path=%s err=%s', path, e)


def _is_path_fresh(path: Path, ttl_sec: int) -> bool:
    try:
        return path.exists() and (time.time() - path.stat().st_mtime) <= max(1, int(ttl_sec or 1))
    except Exception:
        return False


def _source_user_agent() -> str:
    ua = os.getenv('SEC_USER_AGENT', '').strip()
    if ua:
        return ua
    contact = os.getenv('SEC_CONTACT_EMAIL', '').strip()
    if contact:
        return f'Trade_db/1.0 ({contact})'
    return 'Trade_db/1.0 admin@example.com'


def _source_headers() -> Dict[str, str]:
    return {
        'User-Agent': _source_user_agent(),
        'Accept-Encoding': 'gzip, deflate',
        'Accept': 'text/html,application/json,text/plain,*/*',
    }


def _sec13f_index_path() -> Path:
    return _institutional_dir() / 'sec13f_index.json'


def _sec13f_period_path(period: str) -> Path:
    safe = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(period or '').strip())
    return _institutional_dir() / f'sec13f_{safe}.json'


def _sec_tickers_path() -> Path:
    return _institutional_dir() / 'sec_company_tickers_exchange.json'


def _parse_sec13f_links(html: str, limit: int) -> List[Dict]:
    out = []
    seen = set()
    for href in re.findall(r'href=["\']([^"\']+_form13f\.zip)["\']', html or '', flags=re.I):
        name = href.rsplit('/', 1)[-1]
        period = name.replace('_form13f.zip', '').strip()
        if not period or period in seen:
            continue
        seen.add(period)
        out.append({'period': period, 'url': urljoin(_SEC13F_PAGE, href)})
        if len(out) >= max(1, int(limit or 1)):
            break
    return out


def _sec13f_dataset_links(limit: int = 2) -> List[Dict]:
    try:
        resp = requests.get(_SEC13F_PAGE, headers=_source_headers(), timeout=20)
        if resp.status_code != 200:
            logger.info('SEC 13F page unavailable: status=%s', resp.status_code)
            return []
        return _parse_sec13f_links(resp.text, limit)
    except Exception as e:
        logger.debug('SEC 13F page fetch failed: %s', e)
        return []


def _download_sec_zip(url: str, period: str) -> Path | None:
    tmp = _institutional_dir() / f'.sec13f_{period}.zip'
    try:
        with requests.get(url, headers=_source_headers(), timeout=(10, 180), stream=True) as resp:
            if resp.status_code != 200:
                logger.info('SEC 13F zip unavailable: period=%s status=%s', period, resp.status_code)
                return None
            with tmp.open('wb') as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return tmp
    except Exception as e:
        logger.debug('SEC 13F zip download failed: period=%s err=%s', period, e)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return None


_COMMON_ISSUER_WORDS = {
    'THE', 'INC', 'INCORPORATED', 'CORP', 'CORPORATION', 'CO', 'COMPANY', 'LTD', 'LIMITED',
    'PLC', 'LLC', 'LP', 'L P', 'HLDG', 'HLDGS', 'HOLDING', 'HOLDINGS', 'GROUP', 'GRP',
    'CLASS', 'CL', 'COM', 'ORD', 'NEW', 'DE', 'NV', 'SA', 'AG', 'ADR', 'ADS', 'SPONSORED',
}


def _norm_issuer_name(name: str) -> str:
    s = str(name or '').upper().replace('&', ' AND ')
    s = re.sub(r'\b(CL|CLASS)\s+[A-Z0-9]+\b', ' ', s)
    s = re.sub(r'[^A-Z0-9 ]+', ' ', s)
    words = [w for w in re.split(r'\s+', s) if w and w not in _COMMON_ISSUER_WORDS]
    return ' '.join(words).strip()


def _parse_sec13f_zip(zip_path: Path, period: str, source_url: str) -> Dict:
    by_name: Dict[str, Dict] = {}
    rows = 0
    with zipfile.ZipFile(zip_path) as zf:
        members = zf.namelist()
        member = next((n for n in members if n.lower().endswith('infotable.tsv')), '')
        if not member:
            raise ValueError('INFOTABLE.tsv not found in SEC 13F zip')
        with zf.open(member) as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8', errors='replace', newline=''), delimiter='\t')
            for row in reader:
                rows += 1
                upper = {str(k or '').upper(): v for k, v in (row or {}).items()}
                issuer = str(upper.get('NAMEOFISSUER') or '').strip()
                key = _norm_issuer_name(issuer)
                if not key:
                    continue
                value_usd = _to_float(upper.get('VALUE', 0)) * 1000.0
                shares = _to_float(upper.get('SSHPRNAMT', 0))
                if value_usd <= 0 and shares <= 0:
                    continue
                rec = by_name.setdefault(key, {'issuer': issuer, 'value_usd': 0.0, 'shares': 0.0, 'rows': 0})
                rec['value_usd'] = round(float(rec.get('value_usd', 0) or 0) + value_usd, 2)
                rec['shares'] = round(float(rec.get('shares', 0) or 0) + shares, 2)
                rec['rows'] = int(rec.get('rows', 0) or 0) + 1
                if issuer and len(issuer) > len(str(rec.get('issuer') or '')):
                    rec['issuer'] = issuer
    return {
        'period': period,
        'source': 'sec.gov.form13f_dataset',
        'source_url': source_url,
        'updated_ts': time.time(),
        'row_count': rows,
        'issuer_count': len(by_name),
        'by_name': by_name,
    }


def _load_sec_ticker_map(allow_refresh: bool = True) -> Dict:
    path = _sec_tickers_path()
    if (not allow_refresh) or _is_path_fresh(path, _SEC13F_CACHE_TTL_SEC):
        return _load_json_file(path)
    try:
        resp = requests.get(_SEC13F_TICKERS_URL, headers=_source_headers(), timeout=20)
        if resp.status_code != 200:
            return _load_json_file(path)
        payload = resp.json() if resp is not None else {}
        if isinstance(payload, dict) and payload.get('fields') and payload.get('data'):
            payload['updated_ts'] = time.time()
            payload['source'] = 'sec.gov.company_tickers_exchange'
            _write_json_file(path, payload)
            return payload
    except Exception as e:
        logger.debug('SEC ticker map refresh failed: %s', e)
    return _load_json_file(path)


def _sec_company_name_for_symbol(symbol: str) -> str:
    sym = _fmt_us_symbol(symbol).replace('.', '-').upper()
    if not sym:
        return ''
    payload = _load_sec_ticker_map(allow_refresh=False)
    fields = payload.get('fields') or []
    data = payload.get('data') or []
    try:
        idx_name = fields.index('name')
        idx_ticker = fields.index('ticker')
    except Exception:
        return ''
    aliases = {sym, sym.replace('-', '.')}
    for row in data:
        try:
            ticker = str(row[idx_ticker] or '').upper()
            if ticker in aliases:
                return str(row[idx_name] or '').strip()
        except Exception:
            continue
    return ''


def refresh_us_sec13f_cache(force: bool = False, max_periods: int | None = None) -> Dict:
    if not _SEC13F_ENABLED:
        return {'enabled': False, 'periods': []}
    max_periods = max(1, int(max_periods or _SEC13F_REFRESH_MAX_PERIODS or 2))
    links = _sec13f_dataset_links(max_periods)
    if not links:
        return {'enabled': True, 'periods': [], 'error': 'no_links'}

    _load_sec_ticker_map(allow_refresh=True)
    periods = []
    for link in links:
        period = str(link.get('period') or '').strip()
        url = str(link.get('url') or '').strip()
        if not period or not url:
            continue
        out_path = _sec13f_period_path(period)
        existing = _load_json_file(out_path)
        if existing and not force:
            periods.append({
                'period': period,
                'issuer_count': int(existing.get('issuer_count', 0) or 0),
                'row_count': int(existing.get('row_count', 0) or 0),
                'updated_ts': float(existing.get('updated_ts', 0) or 0),
                'source_url': existing.get('source_url') or url,
            })
            continue
        zip_path = _download_sec_zip(url, period)
        if not zip_path:
            continue
        try:
            parsed = _parse_sec13f_zip(zip_path, period, url)
            _write_json_file(out_path, parsed)
            periods.append({
                'period': period,
                'issuer_count': int(parsed.get('issuer_count', 0) or 0),
                'row_count': int(parsed.get('row_count', 0) or 0),
                'updated_ts': float(parsed.get('updated_ts', 0) or 0),
                'source_url': url,
            })
            logger.info('SEC 13F cache updated: period=%s issuers=%s rows=%s', period, parsed.get('issuer_count'), parsed.get('row_count'))
        except Exception as e:
            logger.warning('SEC 13F parse failed: period=%s err=%s', period, e)
        finally:
            try:
                zip_path.unlink(missing_ok=True)
            except Exception:
                pass
    index = {'updated_ts': time.time(), 'source': 'sec.gov.form13f_dataset', 'periods': periods}
    _write_json_file(_sec13f_index_path(), index)
    return index


def _trigger_sec13f_refresh():
    global _SEC13F_REFRESHING
    if not _SEC13F_ENABLED:
        return
    with _SEC13F_LOCK:
        if _SEC13F_REFRESHING:
            return
        _SEC13F_REFRESHING = True

    def _worker():
        global _SEC13F_REFRESHING
        try:
            refresh_us_sec13f_cache(force=False, max_periods=2)
        except Exception as e:
            logger.debug('SEC 13F async refresh failed: %s', e)
        finally:
            with _SEC13F_LOCK:
                _SEC13F_REFRESHING = False

    threading.Thread(target=_worker, name='sec13f-cache-refresh', daemon=True).start()


def _sec13f_latest_cached_periods() -> List[Dict]:
    index = _load_json_file(_sec13f_index_path())
    periods = [p for p in (index.get('periods') or []) if isinstance(p, dict) and _sec13f_period_path(str(p.get('period') or '')).exists()]
    if len(periods) < 2 or not _is_path_fresh(_sec13f_index_path(), _SEC13F_CACHE_TTL_SEC):
        _trigger_sec13f_refresh()
    return periods[:2]


def _sec13f_record_for_alias(data: Dict, aliases: List[str]) -> Tuple[Dict, str]:
    by_name = data.get('by_name') or {}
    for alias in aliases:
        if alias and alias in by_name:
            return by_name.get(alias) or {}, alias
    for alias in aliases:
        if len(alias or '') < 5:
            continue
        for key, rec in by_name.items():
            if key.startswith(alias + ' ') or alias.startswith(key + ' '):
                return rec if isinstance(rec, dict) else {}, key
    return {}, ''


def _fmt_usd_value(v: float) -> str:
    n = float(v or 0.0)
    if abs(n) >= 1_000_000_000:
        return f'{n / 1_000_000_000:.1f}B美元'
    if abs(n) >= 1_000_000:
        return f'{n / 1_000_000:.1f}M美元'
    return f'{n:.0f}美元'


def _refresh_sec13f_us_payload(symbol: str) -> Dict:
    periods = _sec13f_latest_cached_periods()
    if len(periods) < 2:
        return {'updated_ts': time.time(), 'capital_signal': {'score': 0.0, 'items': []}}
    curr_meta, prev_meta = periods[0], periods[1]
    curr = _load_json_file(_sec13f_period_path(str(curr_meta.get('period') or '')))
    prev = _load_json_file(_sec13f_period_path(str(prev_meta.get('period') or '')))
    company_name = _sec_company_name_for_symbol(symbol)
    aliases = []
    for raw in (company_name, symbol):
        norm = _norm_issuer_name(raw)
        if norm and norm not in aliases:
            aliases.append(norm)
    if not aliases:
        return {'updated_ts': time.time(), 'capital_signal': {'score': 0.0, 'items': []}}

    curr_rec, matched = _sec13f_record_for_alias(curr, aliases)
    prev_rec, _ = _sec13f_record_for_alias(prev, aliases)
    curr_value = float(curr_rec.get('value_usd', 0) or 0)
    prev_value = float(prev_rec.get('value_usd', curr_value) or 0)
    if curr_value <= 0 and prev_value <= 0:
        return {'updated_ts': time.time(), 'capital_signal': {'score': 0.0, 'items': []}}

    delta_abs = curr_value - prev_value
    delta_pct = _relative_delta(prev_value, curr_value)
    direction = '增持' if delta_pct > 5 else ('减持' if delta_pct < -5 else '持平')
    score = _clip(delta_pct / 20.0, -4.0, 4.0)
    asof = str(curr_meta.get('period') or '')
    tooltip = (
        f'SEC 13F聚合持仓市值 {_fmt_usd_value(prev_value)}→{_fmt_usd_value(curr_value)}，'
        f'变化{delta_pct:+.1f}% ({asof})，匹配发行人:{curr_rec.get("issuer") or matched or company_name}'
    )
    item = _signal_item(
        'sec13f_local', '13F', _fmt_pct(delta_pct, 1), direction, score,
        'sec.gov.form13f_dataset', asof, tooltip,
        delta_abs=round(delta_abs, 2), delta_pct=round(delta_pct, 2),
        start_value=round(prev_value, 2), end_value=round(curr_value, 2), unit='usd',
        issuer=curr_rec.get('issuer') or matched or company_name,
        previous_period=str(prev_meta.get('period') or ''),
    )
    return {'updated_ts': time.time(), 'capital_signal': {'score': round(score, 2), 'items': [item]}}


def _hk_symbol_key(symbol: str) -> str:
    digits = ''.join(ch for ch in str(symbol or '') if ch.isdigit())
    return digits.lstrip('0') or digits


def _hk_ccass_cache_path() -> Path:
    return _institutional_dir() / 'hk_ccass_webb_cache.json'


def _webb_get(path: str) -> Tuple[str, str]:
    bases = [
        os.getenv('HK_CCASS_SOURCE_BASE', '').strip(),
        'https://webb-database.com',
        'https://webbsite.renavon.com',
    ]
    for base in [b for b in bases if b]:
        url = base.rstrip('/') + path
        try:
            resp = requests.get(url, headers={'User-Agent': 'Trade_db/1.0 institutional-cache'}, timeout=30)
            if resp.status_code == 200 and len(resp.text or '') > 1000:
                return resp.text, url
        except Exception as e:
            logger.debug('Webb CCASS fetch failed: url=%s err=%s', url, e)
    return '', ''


def _parse_webb_ccass_bigchanges(html: str, source_url: str) -> Tuple[str, Dict[str, Dict]]:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return '', {}
    soup = BeautifulSoup(html or '', 'html.parser')
    title = soup.title.get_text(' ', strip=True) if soup.title else ''
    m = re.search(r'(20\d{2}-\d{2}-\d{2})', title + ' ' + soup.get_text(' ', strip=True)[:500])
    asof = m.group(1) if m else dt.date.today().isoformat()
    out: Dict[str, Dict] = {}
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if not rows:
            continue
        header = [c.get_text(' ', strip=True).lower() for c in rows[0].find_all(['td', 'th'])]
        if not any('stock code' in h for h in header) or not any('change' == h for h in header):
            continue
        for tr in rows[1:]:
            cells = [c.get_text(' ', strip=True) for c in tr.find_all(['td', 'th'])]
            if len(cells) < 5:
                continue
            code = _hk_symbol_key(cells[1])
            if not code:
                continue
            change = _to_float(cells[4].replace(',', ''))
            issue = cells[2]
            participant = cells[3]
            rec = out.setdefault(code, {'net_change': 0.0, 'max_abs_change': 0.0, 'participant': '', 'issue': issue, 'rows': 0})
            rec['net_change'] = round(float(rec.get('net_change', 0) or 0) + change, 4)
            rec['rows'] = int(rec.get('rows', 0) or 0) + 1
            if abs(change) >= abs(float(rec.get('max_abs_change', 0) or 0)):
                rec['max_abs_change'] = round(change, 4)
                rec['participant'] = participant
                rec['issue'] = issue
        break
    return asof, out


def _parse_webb_ccass_concentration(html: str, source_url: str) -> Dict[str, Dict]:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return {}
    soup = BeautifulSoup(html or '', 'html.parser')
    out: Dict[str, Dict] = {}
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if not rows:
            continue
        header = [c.get_text(' ', strip=True).lower() for c in rows[0].find_all(['td', 'th'])]
        if not any('top 10' in h for h in header) or not any('stake in ccass' in h for h in header):
            continue
        for tr in rows[1:]:
            cells = [c.get_text(' ', strip=True) for c in tr.find_all(['td', 'th'])]
            if len(cells) < 7:
                continue
            code = _hk_symbol_key(cells[1])
            if not code:
                continue
            out[code] = {
                'issue': cells[2],
                'top5_pct': _to_float(cells[3]),
                'top10_pct': _to_float(cells[4]),
                'top10_ncip_pct': _to_float(cells[5]),
                'stake_ccass_pct': _to_float(cells[6]),
            }
        break
    return out


def refresh_hk_ccass_cache(force: bool = False) -> Dict:
    if not _HK_CCASS_ENABLED:
        return {'enabled': False}
    path = _hk_ccass_cache_path()
    if (not force) and _is_path_fresh(path, _HK_CCASS_CACHE_TTL_SEC):
        return _load_json_file(path)
    big_html, big_url = _webb_get('/ccass/bigchanges.asp')
    conc_html, conc_url = _webb_get('/ccass/cconc.asp')
    if not big_html and not conc_html:
        return _load_json_file(path) or {'enabled': True, 'error': 'no_source'}
    asof, big_changes = _parse_webb_ccass_bigchanges(big_html, big_url) if big_html else ('', {})
    concentration = _parse_webb_ccass_concentration(conc_html, conc_url) if conc_html else {}
    payload = {
        'enabled': True,
        'updated_ts': time.time(),
        'asof': asof or dt.date.today().isoformat(),
        'source': 'webbsite.renavon.ccass',
        'source_urls': {'bigchanges': big_url, 'concentration': conc_url},
        'big_changes': big_changes,
        'concentration': concentration,
    }
    _write_json_file(path, payload)
    logger.info('HK CCASS cache updated: changes=%s concentration=%s asof=%s', len(big_changes), len(concentration), payload.get('asof'))
    return payload


def _trigger_hk_ccass_refresh():
    global _HK_CCASS_REFRESHING
    if not _HK_CCASS_ENABLED:
        return
    with _HK_CCASS_LOCK:
        if _HK_CCASS_REFRESHING:
            return
        _HK_CCASS_REFRESHING = True

    def _worker():
        global _HK_CCASS_REFRESHING
        try:
            refresh_hk_ccass_cache(force=False)
        except Exception as e:
            logger.debug('HK CCASS async refresh failed: %s', e)
        finally:
            with _HK_CCASS_LOCK:
                _HK_CCASS_REFRESHING = False

    threading.Thread(target=_worker, name='hk-ccass-cache-refresh', daemon=True).start()


def _get_hk_ccass_cache() -> Dict:
    path = _hk_ccass_cache_path()
    payload = _load_json_file(path)
    now = time.time()
    ts = float(payload.get('updated_ts', 0) or 0) if isinstance(payload, dict) else 0.0
    if not payload or (now - ts) > _HK_CCASS_CACHE_TTL_SEC:
        _trigger_hk_ccass_refresh()
    if payload and (now - ts) <= _HK_CCASS_STALE_SEC:
        return payload
    return {}


def _refresh_hk_ccass_payload(symbol: str) -> Dict:
    cache = _get_hk_ccass_cache()
    code = _hk_symbol_key(symbol)
    if not cache or not code:
        return {'updated_ts': time.time(), 'capital_signal': {'score': 0.0, 'items': []}}
    asof = str(cache.get('asof') or '')
    items = []
    change = (cache.get('big_changes') or {}).get(code) or {}
    if change:
        net = float(change.get('net_change', 0) or 0)
        direction = '增持' if net > 0.1 else ('减持' if net < -0.1 else '持平')
        score = _clip(net / 3.0, -3.0, 3.0)
        participant = str(change.get('participant') or '').strip()
        tooltip = f'CCASS参与者大额变动净{net:+.2f}个百分点'
        if participant:
            tooltip += f'，最大变动:{participant} {float(change.get("max_abs_change", 0) or 0):+.2f}个百分点'
        if asof:
            tooltip += f' ({asof})'
        items.append(_signal_item('hk_ccass_bigchange', 'CCASS', _fmt_pct(net, 2), direction, score, 'webb_database.ccass.bigchanges', asof, tooltip, delta_abs=round(net, 4), count=int(change.get('rows', 0) or 0)))
    conc = (cache.get('concentration') or {}).get(code) or {}
    if conc:
        top10 = float(conc.get('top10_pct', 0) or 0)
        direction = '集中' if top10 >= 70 else ('分散' if top10 <= 35 else '常态')
        tooltip = f'CCASS Top10集中度{top10:.2f}%，Top5 {float(conc.get("top5_pct", 0) or 0):.2f}%，CCASS持股{float(conc.get("stake_ccass_pct", 0) or 0):.2f}%'
        if asof:
            tooltip += f' ({asof})'
        items.append(_signal_item('hk_ccass_concentration', '集中', f'T10 {top10:.0f}%', direction, 0.0, 'webb_database.ccass.concentration', asof, tooltip, current=round(top10, 4)))
    score = sum(float(x.get('score', 0) or 0) for x in items)
    return {'updated_ts': time.time(), 'capital_signal': {'score': round(_clip(score, -8.0, 8.0), 2), 'items': items}}

def _yf_symbol(market: str, symbol: str) -> str:
    mkt = str(market or "").upper()
    s = str(symbol or "").strip().upper()
    if mkt == "HK":
        digits = "".join(ch for ch in s if ch.isdigit())
        return f"{digits.zfill(4)}.HK" if digits else s
    return _fmt_us_symbol(s)



def _parse_pct_text(v) -> float:
    return _to_float(str(v or "").replace("%", ""))


def _refresh_alpha_us_institutional_payload(symbol: str) -> Dict:
    key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip() or os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    if not key:
        return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}
    sym = _fmt_us_symbol(symbol)
    if not sym:
        return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}
    try:
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "INSTITUTIONAL_HOLDINGS", "symbol": sym, "apikey": key},
            timeout=10,
        )
        if resp.status_code != 200:
            return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}
        data = resp.json() if resp is not None else {}
        if not isinstance(data, dict) or data.get("Note") or data.get("Information") or data.get("Error Message"):
            return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}
        total_pct = _parse_pct_text(data.get("total_institutional_ownership_percentage", 0))
        total_shares = _to_float(data.get("total_institutional_shares", 0))
        inc_shares = _to_float(data.get("shares_with_increased_holdings", 0))
        dec_shares = _to_float(data.get("shares_with_decreased_holdings", 0))
        net_pct = ((inc_shares - dec_shares) / total_shares * 100.0) if total_shares > 0 else 0.0
        holders = data.get("holdings") or []
        asof = ""
        if isinstance(holders, list):
            dates = [str((x or {}).get("last_reported", ""))[:10] for x in holders if isinstance(x, dict) and (x or {}).get("last_reported")]
            asof = max(dates) if dates else ""
        direction = "增持" if net_pct > 0.3 else ("减持" if net_pct < -0.3 else "持平")
        score = _clip(net_pct / 5.0, -4.0, 4.0)
        tooltip = f"机构持仓约{total_pct:.1f}%，增持股数{inc_shares / 1000000:.1f}百万，减持股数{dec_shares / 1000000:.1f}百万，净变化{net_pct:+.1f}%"
        if asof:
            tooltip += f" ({asof})"
        item = _signal_item(
            "alpha_institutional",
            "13F",
            _fmt_pct(net_pct, 1) if abs(net_pct) >= 0.05 else f"{total_pct:.1f}%",
            direction,
            score,
            "alpha_vantage.INSTITUTIONAL_HOLDINGS",
            asof,
            tooltip,
            delta_pct=round(net_pct, 2),
            current=round(total_pct, 2),
            count=int(_to_float(data.get("total_institutional_holders", 0))),
        )
        return {"updated_ts": time.time(), "capital_signal": {"score": round(score, 2), "items": [item]}}
    except Exception as e:
        logger.debug("alpha institutional failed: symbol=%s err=%s", symbol, e)
        return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}

def _refresh_yfinance_holders_payload(market: str, symbol: str) -> Dict:
    try:
        import yfinance as yf
    except Exception:
        return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}
    items = []
    yf_sym = _yf_symbol(market, symbol)
    try:
        ticker = yf.Ticker(yf_sym)
        df = ticker.institutional_holders
        if df is not None and not getattr(df, "empty", True):
            pct_col = "% Out" if "% Out" in df.columns else None
            total_pct = sum(_to_float(v) for v in df[pct_col]) if pct_col else 0.0
            asof = ""
            if "Date Reported" in df.columns:
                vals = [str(x)[:10] for x in df["Date Reported"].dropna().tolist()]
                asof = max(vals) if vals else ""
            if total_pct > 0:
                items.append(_signal_item("yf_institutional", "机构", f"{total_pct:.1f}%", "持仓", 0.0, "yfinance.institutional_holders", asof, f"Yahoo机构持仓合计约{total_pct:.1f}%，机构数{len(df)}" + (f" ({asof})" if asof else ""), current=round(total_pct, 4), count=int(len(df))))
    except Exception as e:
        logger.debug("yfinance holders failed: market=%s symbol=%s err=%s", market, symbol, e)
    return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": items}}


def _refresh_capital_payload(market: str, symbol: str) -> Dict:
    mkt = str(market or "").strip().upper()
    sym = str(symbol or "").strip()
    if mkt == "CN":
        return _refresh_cn_capital_payload(sym)
    if mkt == "US":
        sec_payload = _refresh_sec13f_us_payload(sym)
        alpha_payload = _refresh_alpha_us_institutional_payload(sym)
        yf_payload = _refresh_yfinance_holders_payload(mkt, sym)
        return _merge_payload(_merge_payload(sec_payload, alpha_payload), yf_payload)
    if mkt == "HK":
        ccass_payload = _refresh_hk_ccass_payload(sym)
        yf_payload = _refresh_yfinance_holders_payload(mkt, sym)
        return _merge_payload(ccass_payload, yf_payload)
    return {"updated_ts": time.time(), "capital_signal": {"score": 0.0, "items": []}}


def _trigger_capital_refresh(market: str, symbols: List[str]):
    mkt = str(market or "").strip().upper()
    if mkt not in {"CN", "HK", "US"}:
        return
    targets = []
    for sym in symbols[: max(0, _CAPITAL_REFRESH_MAX_SYMBOLS)]:
        s = str(sym or "").strip()
        if not s:
            continue
        key = _capital_cache_key(mkt, s)
        with _CAPITAL_LOCK:
            if key in _CAPITAL_REFRESHING:
                continue
            _CAPITAL_REFRESHING.add(key)
        targets.append((s, key))
    if not targets:
        return

    def _worker():
        try:
            for sym, key in targets:
                try:
                    payload = _refresh_capital_payload(mkt, sym)
                    if isinstance(payload, dict):
                        set_cache(key, payload, ttl=_CAPITAL_SIGNAL_STALE_SEC)
                except Exception as e:
                    logger.debug("capital refresh failed: market=%s symbol=%s err=%s", mkt, sym, e)
                finally:
                    with _CAPITAL_LOCK:
                        _CAPITAL_REFRESHING.discard(key)
                time.sleep(0.4)
        finally:
            with _CAPITAL_LOCK:
                for _, key in targets:
                    _CAPITAL_REFRESHING.discard(key)

    threading.Thread(target=_worker, name=f"capital-signal-{mkt.lower()}", daemon=True).start()


def _get_cached_capital_map(market: str, symbols: List[str]) -> Dict[str, Dict]:
    mkt = str(market or "").strip().upper()
    out: Dict[str, Dict] = {}
    stale_or_missing = []
    for sym in symbols:
        raw = str(sym or "").strip()
        if not raw:
            continue
        payload = get_cache(_capital_cache_key(mkt, raw))
        if isinstance(payload, dict) and _is_capital_usable(payload):
            out[raw] = payload
        if not isinstance(payload, dict) or not _is_capital_fresh(payload):
            stale_or_missing.append(raw)
    _trigger_capital_refresh(mkt, stale_or_missing)
    return out

def _get_us_change_one(symbol: str) -> Dict:
    sym = _fmt_us_symbol(symbol)
    if not sym:
        return {}

    cache_key = f"inst_us_insider:{sym}"
    cached = get_cache(cache_key)
    now = time.time()
    if isinstance(cached, dict) and (now - float(cached.get("updated_ts", 0) or 0)) <= _INST_US_CACHE_TTL_SEC:
        return cached

    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        return cached if isinstance(cached, dict) else {}

    end = dt.date.today()
    start = end - dt.timedelta(days=max(10, _INST_US_LOOKBACK_DAYS))

    try:
        tx_resp = requests.get(
            "https://finnhub.io/api/v1/stock/insider-transactions",
            params={"symbol": sym, "from": start.isoformat(), "to": end.isoformat(), "token": key},
            timeout=4,
        )
        if tx_resp.status_code != 200:
            return cached if isinstance(cached, dict) else {}
        tx_data = (tx_resp.json() or {}).get("data") or []

        pf_resp = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={"symbol": sym, "token": key},
            timeout=4,
        )
        if pf_resp.status_code != 200:
            return cached if isinstance(cached, dict) else {}
        profile = pf_resp.json() if pf_resp is not None else {}
        shares_out_m = _to_float((profile or {}).get("shareOutstanding", 0))
        shares_out = shares_out_m * 1_000_000.0
        if shares_out <= 0:
            return cached if isinstance(cached, dict) else {}

        net_change = 0.0
        latest_date = ""
        for r in tx_data:
            code = str((r or {}).get("transactionCode", "") or "").strip().upper()
            if code not in {"P", "S"}:
                continue
            chg = _to_float((r or {}).get("change", 0))
            net_change += chg
            d = str((r or {}).get("transactionDate", "") or "")
            if d and d > latest_date:
                latest_date = d

        net_pct = (net_change / shares_out) * 100.0
        factor = round(_clip(net_pct / 0.01, -5.0, 5.0), 2)
        if net_pct > 0.005:
            direction = "增持"
        elif net_pct < -0.005:
            direction = "减持"
        else:
            direction = "持平"

        asof = _fmt_date_ymd(latest_date)
        text = f"内部人{direction} {net_pct:+.3f}%"
        if asof:
            text += f" ({asof})"

        payload = {
            "inst_factor": factor,
            "inst_label": "内部人",
            "inst_change_pp": round(net_pct, 6),
            "inst_delta_abs": round(net_pct, 6),
            "inst_delta_pct": 0.0,
            "inst_start_value": 0.0,
            "inst_end_value": round(net_pct, 6),
            "inst_start_date": start.isoformat(),
            "inst_end_date": asof or end.isoformat(),
            "inst_metric_unit": "percent",
            "inst_date": asof,
            "inst_source": "finnhub.insider",
            "inst_text": text,
            "inst_direction": direction,
            "inst_holding": {
                "metric": "insider_net_flow_pct",
                "label": "内部人",
                "start_value": 0.0,
                "end_value": round(net_pct, 6),
                "delta_abs": round(net_pct, 6),
                "delta_pct": 0.0,
                "start_date": start.isoformat(),
                "end_date": asof or end.isoformat(),
                "unit": "percent",
                "source": "finnhub.insider",
            },
            "capital_signal": {
                "score": factor,
                "items": [{"label": "内部人", "value": f"{net_pct:+.3f}%", "direction": direction, "tooltip": text, "source": "finnhub.insider", "asof": asof}] if abs(net_pct) > 0 else [],
            },
            "updated_ts": now,
        }
        set_cache(cache_key, payload, ttl=max(_INST_US_CACHE_TTL_SEC, 3600))
        return payload
    except Exception:
        return cached if isinstance(cached, dict) else {}


def get_institutional_change_map(market: str, symbols: List[str]) -> Dict[str, Dict]:
    mkt = str(market or "").strip().upper()
    syms = [str(s or "").strip() for s in (symbols or []) if str(s or "").strip()]
    if not syms:
        return {}

    base_map: Dict[str, Dict] = {}
    if mkt in {"CN", "HK"}:
        base_map = _get_cn_hk_changes(mkt, syms)
    elif mkt == "US":
        capped = syms[: max(1, _INST_US_MAX_SYMBOLS)]
        if capped:
            max_workers = min(4, len(capped))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {executor.submit(_get_us_change_one, sym): sym for sym in capped}
                for future in concurrent.futures.as_completed(future_map):
                    sym = future_map[future]
                    try:
                        payload = future.result()
                    except Exception:
                        payload = {}
                    if payload:
                        base_map[sym] = payload
    else:
        return {}

    capital_map = _get_cached_capital_map(mkt, syms)
    out: Dict[str, Dict] = {}
    for sym in syms:
        merged = _merge_payload(base_map.get(sym, {}), capital_map.get(sym, {}))
        if merged:
            merged['capital_signal_coverage'] = _capital_coverage(mkt, merged)
            out[sym] = merged
    return out
