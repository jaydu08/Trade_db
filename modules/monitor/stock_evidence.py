"""Recent, linkable stock disclosures/news; relevance is not price causality."""
import datetime as dt
import ipaddress
import os
import re
from urllib.parse import urlparse, urlunparse

import requests

from core.cache import get_cache, set_cache
from core.db import get_meta_session
from domain.meta import Asset
from modules.monitor.stock_narrative import _profile_symbol


MATERIAL_WORDS = re.compile(r"业绩|财报|年度报告|季度报告|半年报|收购|并购|重大|投资|合同|回购|增持|减持|分红|诉讼|停牌|复牌|earnings|revenue|acquisition|merger|guidance", re.I)
ROUTINE_WORDS = re.compile(r"翌日披露报表|证券变动月报表|董事会会议召开日期|法律意见书|股东会通知|董事会会议通知|stock price today|quotes & news|meme coins|top midday stories|global strategic business report", re.I)


def _safe_text(value, limit=180):
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _safe_link(value, market):
    try:
        parsed = urlparse(str(value or "").strip())
        host = (parsed.hostname or "").lower()
        if market in {"CN", "HK"}:
            if host not in {"cninfo.com.cn", "www.cninfo.com.cn"} or not parsed.path.startswith("/new/disclosure/detail"):
                return ""
            return urlunparse(parsed._replace(scheme="https"))
        if parsed.scheme != "https" or not host or parsed.username or parsed.password:
            return ""
        if host == "localhost" or host.endswith(".localhost"):
            return ""
        try:
            if not ipaddress.ip_address(host).is_global:
                return ""
        except ValueError:
            pass
        return parsed.geturl()[:900]
    except ValueError:
        return ""


def _priority(title):
    if ROUTINE_WORDS.search(title):
        return 0
    return 2 if MATERIAL_WORDS.search(title) else 1


def _company_name(name):
    text = _safe_text(name, 100)
    text = re.sub(r"(?i)\b(common stock|class [a-z] common stock|ordinary shares|incorporated|corporation|corp\.?|inc\.?|ltd\.?)\b", " ", text)
    return re.sub(r"\s+", " ", text).strip(" .,-")


def _us_headline_matches(title, symbol, company_name):
    title = _safe_text(title, 300)
    if ROUTINE_WORDS.search(title):
        return False
    ticker = str(symbol).split(".")[-1]
    if len(ticker) >= 2 and re.search(r"(?<![A-Za-z0-9])" + re.escape(ticker) + r"(?![A-Za-z0-9])", title):
        return True
    name = _company_name(company_name)
    if len(name) >= 4 and re.search(r"(?<![A-Za-z])" + re.escape(name) + r"(?![A-Za-z])", title, re.I):
        return True
    return False


def _cninfo_events(market, symbol, today):
    import akshare as ak

    start = today - dt.timedelta(days=45)
    try:
        frame = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=symbol, market="港股" if market == "HK" else "沪深京",
            start_date=start.strftime("%Y%m%d"), end_date=today.strftime("%Y%m%d"))
    except KeyError:
        # AkShare raises on an empty CNInfo result frame; do not invent a filing.
        return []
    events = []
    for row in frame.to_dict("records"):
        if str(row.get("代码") or "").strip() != symbol:
            continue
        title = _safe_text(row.get("公告标题"), 180)
        date = str(row.get("公告时间") or "")[:10]
        url = _safe_link(row.get("公告链接"), market)
        if not title or not url:
            continue
        try:
            day = dt.date.fromisoformat(date)
        except ValueError:
            continue
        if not start <= day <= today:
            continue
        events.append({"title": title, "date": date, "source": "巨潮资讯", "url": url,
                       "kind": "公告", "priority": _priority(title)})
    return events


def _finnhub_events(symbol, company_name, today):
    from dotenv import load_dotenv
    load_dotenv()
    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        return []
    start = today - dt.timedelta(days=14)
    response = requests.get("https://finnhub.io/api/v1/company-news", params={
        "symbol": symbol, "from": start.isoformat(), "to": today.isoformat(), "token": key,
    }, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Finnhub company-news response is not a list")
    events = []
    for row in payload:
        if not isinstance(row, dict) or symbol not in [s.strip() for s in str(row.get("related") or "").split(",")]:
            continue
        title = _safe_text(row.get("headline"), 180)
        if not _us_headline_matches(title, symbol, company_name):
            continue
        try:
            day = dt.datetime.fromtimestamp(int(row.get("datetime")), dt.timezone.utc).date()
        except (ValueError, TypeError, OverflowError):
            continue
        url = _safe_link(row.get("url"), "US")
        if not url or not start <= day <= today:
            continue
        events.append({"title": title, "date": day.isoformat(),
                       "source": _safe_text(row.get("source"), 40) or "Finnhub",
                       "url": url, "kind": "新闻", "priority": _priority(title)})
    return events


def get_stock_evidence(market, symbol):
    market = str(market or "").upper().strip()
    symbol = str(symbol or "").upper().strip()
    if market not in {"CN", "HK", "US"} or not re.fullmatch(r"[A-Z0-9.\-]{1,24}", symbol):
        raise ValueError("invalid stock identity")
    with get_meta_session() as session:
        asset = _profile_symbol(session, market, symbol)
        if asset is None:
            return {"market": market, "symbol": symbol, "events": [], "status": "not_found"}
        company_name = asset.name
    key = f"stock_evidence_v2:{market}:{symbol}"
    cached = get_cache(key)
    if isinstance(cached, dict):
        return cached
    today = dt.date.today()
    try:
        events = (_finnhub_events(symbol, company_name, today) if market == "US"
                  else _cninfo_events(market, symbol, today))
        deduped = {event["url"]: event for event in events}
        routine_count = sum(event["priority"] == 0 for event in deduped.values())
        ordered = sorted((event for event in deduped.values() if event["priority"] > 0),
                         key=lambda e: (e["priority"], e["date"]), reverse=True)[:5]
        result = {"market": market, "symbol": symbol, "events": ordered,
                  "status": "ok" if ordered else ("routine_only" if routine_count else "none"),
                  "routine_count": routine_count,
                  "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                  "note": "按公司代码或名称匹配的近期公告/新闻标题；时间接近行情不证明价格因它变动。"}
        set_cache(key, result, ttl=1800 if ordered else 900)
        return result
    except Exception:
        return {"market": market, "symbol": symbol, "events": [], "status": "unavailable",
                "note": "公告/新闻来源暂时不可用；未据此推断行情原因。"}
