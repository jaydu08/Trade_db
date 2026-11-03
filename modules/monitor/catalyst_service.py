"""
Catalyst Service — 实时催化剂(马甲)解析服务

为 heatmap 热榜标的获取市场炒作根因标签:
- A股: 问财批量涨停原因 → DuckDuckGo+LLM 兜底
- US:  Finviz 新闻标题 → DuckDuckGo+LLM 兜底
- HK:  DuckDuckGo+Google News → LLM 提纯
"""

import logging
import os
import re
import time
import datetime as dt
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── 全局缓存: {symbol: {"tags": str, "source": str}} ──
_WENCAI_CACHE: Dict[str, Dict[str, str]] = {}
_WENCAI_CACHE_DATE: Optional[dt.date] = None

_LLM_ENABLED = str(os.getenv("CATALYST_LLM_ENABLED", "1")).strip().lower() in {
    "1", "true", "yes", "on",
}

_SEARCH_INTERVAL = 2.5          # 搜索间隔(秒)
_FINVIZ_INTERVAL = 3.0          # Finviz 间隔(秒)
_TOTAL_TIMEOUT = 300            # 整体超时(秒)


# ═══════════════════════════════════════════════════════════
#  公共入口
# ═══════════════════════════════════════════════════════════

def resolve_catalysts(stocks: List[Dict], market: str) -> None:
    """
    为热榜标的批量解析催化剂, 结果写入:
      stock['catalyst_tags']   — "标签1 / 标签2"
      stock['catalyst_source'] — "wencai" | "finviz" | "search+llm" | "error:xxx"
    """
    market = str(market or "").upper()
    if not stocks:
        return

    t0 = time.time()
    try:
        if market == "CN":
            _resolve_cn(stocks)
        elif market == "US":
            _resolve_us(stocks)
        elif market == "HK":
            _resolve_hk(stocks)
        else:
            for s in stocks:
                s["catalyst_tags"] = ""
                s["catalyst_source"] = ""
    except Exception as e:
        logger.error("resolve_catalysts(%s) unexpected error: %s", market, e)
        for s in stocks:
            s.setdefault("catalyst_tags", f"催化剂解析异常")
            s.setdefault("catalyst_source", f"error:{e}")

    elapsed = time.time() - t0
    logger.info("resolve_catalysts(%s) done in %.1fs for %d stocks", market, elapsed, len(stocks))


# ═══════════════════════════════════════════════════════════
#  A股: 问财 + 搜索兜底
# ═══════════════════════════════════════════════════════════

def _resolve_cn(stocks: List[Dict]) -> None:
    # 1) 问财批量拉涨停原因
    wencai_map = _fetch_wencai_reasons()

    need_search: List[Dict] = []
    for s in stocks:
        sym = str(s.get("symbol", "")).strip()
        code = _extract_cn_code(sym)
        hit = wencai_map.get(code, "")
        if hit:
            s["catalyst_tags"] = hit
            s["catalyst_source"] = "wencai"
        else:
            need_search.append(s)

    # 2) 未命中的走搜索+LLM
    if need_search:
        _search_and_llm_batch(need_search, market="CN")


def _extract_cn_code(symbol: str) -> str:
    """从 'SZ002942' / '002942.SZ' / '002942' 提取纯6位数字"""
    s = str(symbol or "").strip().upper()
    digits = re.sub(r"[^0-9]", "", s)
    return digits[:6] if len(digits) >= 6 else digits


def _fetch_wencai_reasons() -> Dict[str, str]:
    """调用问财拉取今日涨停原因, 返回 {code: "标签1 / 标签2"}"""
    global _WENCAI_CACHE, _WENCAI_CACHE_DATE
    today = dt.date.today()

    if _WENCAI_CACHE_DATE == today and _WENCAI_CACHE:
        logger.debug("Wencai cache hit for %s", today)
        return _WENCAI_CACHE

    try:
        import pywencai
        df = pywencai.get(question="今日涨停股票涨停原因", query_type="stock")
        if df is None or not hasattr(df, "iterrows"):
            logger.warning("Wencai returned non-DataFrame: %s", type(df))
            return {}

        result: Dict[str, str] = {}
        # 涨停原因字段名包含日期, 动态查找
        reason_col = None
        for col in df.columns:
            if "涨停原因" in str(col):
                reason_col = col
                break

        code_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if col_lower in ("股票代码", "code", "代码"):
                code_col = col
                break

        if reason_col is None:
            logger.warning("Wencai DataFrame missing 涨停原因 column: %s", list(df.columns)[:10])
            return {}

        for _, row in df.iterrows():
            # 获取股票代码
            if code_col:
                raw_code = str(row[code_col])
            else:
                # 有些版本 index 就是股票代码
                raw_code = str(row.name) if hasattr(row, "name") else ""

            code = _extract_cn_code(raw_code)
            if not code:
                continue

            reason = str(row.get(reason_col, "") or "").strip()
            if not reason:
                continue

            # "华为昇腾+分销龙头+存储芯片+一季报预增" → 取前2
            parts = [p.strip() for p in reason.split("+") if p.strip()]
            tags = " / ".join(parts[:2]) if parts else reason[:30]
            result[code] = tags

        _WENCAI_CACHE = result
        _WENCAI_CACHE_DATE = today
        logger.info("Wencai fetched %d limit-up reasons", len(result))
        return result

    except Exception as e:
        logger.warning("Wencai fetch failed: %s", e)
        return {}


# ═══════════════════════════════════════════════════════════
#  美股: Finviz + DuckDuckGo + LLM
# ═══════════════════════════════════════════════════════════

def _resolve_us(stocks: List[Dict]) -> None:
    t0 = time.time()
    for s in stocks:
        if time.time() - t0 > _TOTAL_TIMEOUT:
            s["catalyst_tags"] = "超时未解析"
            s["catalyst_source"] = "error:timeout"
            continue

        ticker = _extract_us_ticker(s)
        if not ticker:
            s["catalyst_tags"] = ""
            s["catalyst_source"] = ""
            continue

        # 主路径: Finviz
        headlines = _fetch_finviz_headlines(ticker)
        if headlines:
            tags = _llm_extract_catalyst(ticker, headlines, market="US")
            s["catalyst_tags"] = tags
            s["catalyst_source"] = "finviz" if tags and "不可用" not in tags else "finviz+llm_fail"
        else:
            # 兜底: DuckDuckGo
            search_text = _ddg_search(f"{ticker} stock news why up today")
            if search_text:
                tags = _llm_extract_catalyst(ticker, search_text, market="US")
                s["catalyst_tags"] = tags
                s["catalyst_source"] = "search+llm"
            else:
                s["catalyst_tags"] = "数据源不可用"
                s["catalyst_source"] = "error:all_sources_failed"

        time.sleep(_FINVIZ_INTERVAL)


def _extract_us_ticker(stock: Dict) -> str:
    sym = str(stock.get("symbol", "")).strip().upper()
    if "." in sym:
        return sym.split(".")[-1]
    return sym


def _fetch_finviz_headlines(ticker: str, max_headlines: int = 5) -> str:
    """爬取 Finviz 个股页面新闻标题"""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logger.warning("Finviz returned %d for %s", resp.status_code, ticker)
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        news_table = soup.find("table", class_="fullview-news-outer")
        if not news_table:
            return ""
        rows = news_table.find_all("tr")
        titles = []
        for row in rows[:max_headlines]:
            a_tag = row.find("a", class_="tab-link-news")
            if a_tag:
                titles.append(a_tag.get_text(strip=True))
        return " | ".join(titles) if titles else ""
    except Exception as e:
        logger.warning("Finviz scrape failed for %s: %s", ticker, e)
        return ""


# ═══════════════════════════════════════════════════════════
#  港股: DuckDuckGo + LLM
# ═══════════════════════════════════════════════════════════

def _resolve_hk(stocks: List[Dict]) -> None:
    _search_and_llm_batch(stocks, market="HK")


# ═══════════════════════════════════════════════════════════
#  通用: 搜索 + LLM 提纯
# ═══════════════════════════════════════════════════════════

def _search_and_llm_batch(stocks: List[Dict], market: str) -> None:
    """对列表中每只股票逐个搜索+LLM, 带间隔防限流"""
    t0 = time.time()
    for s in stocks:
        if time.time() - t0 > _TOTAL_TIMEOUT:
            s.setdefault("catalyst_tags", "超时未解析")
            s.setdefault("catalyst_source", "error:timeout")
            continue

        name = str(s.get("name", "")).strip()
        sym = str(s.get("symbol", "")).strip()

        if market == "CN":
            query = f"{name} {sym} 上涨原因 异动 利好"
        elif market == "HK":
            query = f"{name} {sym} 港股 异动 暴涨原因"
        else:
            query = f"{sym} stock news why up today"

        search_text = _web_search(query)
        if search_text:
            tags = _llm_extract_catalyst(sym, search_text, market=market)
            s["catalyst_tags"] = tags
            s["catalyst_source"] = "search+llm"
        else:
            s["catalyst_tags"] = "搜索无结果"
            s["catalyst_source"] = "error:search_empty"

        time.sleep(_SEARCH_INTERVAL)


def _web_search(query: str) -> str:
    """通过 DataManager.search() 进行联网搜索"""
    try:
        from modules.ingestion.data_factory import data_manager
        result = data_manager.search(query, limit_per_source=5, timeout=15)
        if result and "未返回有效结果" not in result:
            return result
    except Exception as e:
        logger.warning("Web search failed for '%s': %s", query, e)
    return ""


def _ddg_search(query: str) -> str:
    """直接调用 DuckDuckGo (绕过 DataManager 缓存)"""
    try:
        from ddgs import DDGS
        raw = DDGS().text(query, max_results=5)
        parts = []
        for item in raw:
            title = item.get("title", "")
            body = item.get("body", "")
            parts.append(f"{title}: {body}")
        return " | ".join(parts) if parts else ""
    except Exception as e:
        logger.warning("DuckDuckGo direct search failed: %s", e)
        return ""


def _llm_extract_catalyst(identifier: str, news_text: str, market: str = "") -> str:
    """用 LLM 从新闻文本中提取催化剂标签"""
    if not _LLM_ENABLED:
        # LLM 关闭时截取原始文本
        return _truncate_raw(news_text)

    try:
        from core.llm import simple_prompt

        if market == "CN":
            prompt = (
                f"标的: {identifier}\n"
                f"新闻情报:\n{news_text[:1500]}\n\n"
                "任务: 用最多2个中文短语概括该A股今天异动/上涨的核心催化剂。"
                "格式: 标签1 / 标签2\n"
                "要求: 只输出标签, 不要解释。如无法判断输出'资金驱动'。"
            )
        elif market == "HK":
            prompt = (
                f"标的: {identifier}\n"
                f"新闻情报:\n{news_text[:1500]}\n\n"
                "任务: 用最多2个中文短语概括该港股今天异动/上涨的核心催化剂。"
                "格式: 标签1 / 标签2\n"
                "要求: 只输出标签, 不要解释。如无法判断输出'资金驱动'。"
            )
        else:
            prompt = (
                f"Ticker: {identifier}\n"
                f"News headlines:\n{news_text[:1500]}\n\n"
                "任务: 用最多2个中文短语概括该美股今天暴涨催化剂。"
                "格式: 标签1 / 标签2\n"
                "要求: 只输出中文标签, 不要解释。如无法判断输出'资金驱动'。"
            )

        result = simple_prompt(prompt, temperature=0.1)
        result = result.strip().strip('"').strip("'")
        # 清理可能的多余输出
        if "\n" in result:
            result = result.split("\n")[0].strip()
        return result[:60] if result else "资金驱动"

    except Exception as e:
        logger.warning("LLM catalyst extraction failed for %s: %s", identifier, e)
        return f"LLM不可用"


def _truncate_raw(text: str) -> str:
    """LLM 关闭时, 从原始文本截取摘要作为标签"""
    t = str(text or "").strip()
    if not t:
        return ""
    # 取第一段有意义的文字
    lines = [l.strip() for l in t.split("|") if l.strip()]
    if lines:
        return lines[0][:40]
    return t[:40]
