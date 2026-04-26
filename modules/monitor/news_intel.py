import datetime as dt
import logging
import re
import time
import os
from typing import Any, Dict, List

# ChromaDB 已禁用，不再 import get_collection
# from core.db import get_collection

logger = logging.getLogger(__name__)

# ---- ChromaDB 全局开关 ----
# 禁用后所有查询函数直接返空，不触发 HNSW 索引加载
_CHROMADB_DISABLED = True


def _parse_event_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s[:10]).date()
    except Exception:
        return None


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "." in raw:
        return raw.split(".")[-1].strip()
    return raw


def _is_related_symbol(meta_symbols: Any, symbol: str) -> bool:
    if meta_symbols is None:
        return False

    target = _normalize_symbol(symbol)
    if not target:
        return False

    if isinstance(meta_symbols, list):
        parts = {_normalize_symbol(str(x)) for x in meta_symbols}
        return target in parts

    raw = str(meta_symbols or "").upper()
    parts = {_normalize_symbol(x.strip()) for x in raw.replace(";", ",").split(",") if x.strip()}
    return target in parts or target in _normalize_symbol(raw)


def _doc_headline(text: str, limit: int = 64) -> str:
    content = str(text or "").replace("\r", "\n")
    lines = [x.strip() for x in content.split("\n") if x.strip()]
    if not lines:
        return ""

    cand = lines[0]
    if len(lines) >= 2 and "【定向新闻】" in lines[0]:
        cand = lines[1]
        if cand.startswith("场景:") and len(lines) >= 3:
            cand = lines[2]

    # 聚合搜索结果文本：优先抽第一条新闻标题
    if "搜索结果" in cand:
        for ln in lines:
            m = re.match(r"^\d+\.\s*(.+)$", ln)
            if m:
                cand = m.group(1)
                break

    if ":" in cand and not cand.startswith("["):
        cand = cand.split(":", 1)[0].strip()

    cand = re.sub(r"\s+", " ", cand).strip()
    return cand[:limit]


def get_symbol_news_events(symbol: str, start_date: dt.date, max_items: int = 24) -> List[Dict[str, Any]]:
    """ChromaDB 已禁用，直接返空。"""
    return []

    docs: List[Dict[str, Any]] = []

    def _collect_from_collection(name: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        collection = get_collection(name)
        data = collection.get(
            where={"related_symbols": sym},
            limit=max(40, max_items * 4),
            include=["metadatas"],
        )
        metas = data.get("metadatas", []) if isinstance(data, dict) else []
        for meta in metas or []:
            meta = meta or {}
            d = _parse_event_date(meta.get("event_date"))
            if not d or d < start_date:
                continue
            out.append(
                {
                    "date": d,
                    "source": str(meta.get("source", "")),
                    "document": "",
                    "headline": str(meta.get("headline", "") or "").strip(),
                }
            )
        return out

    try:
        docs.extend(_collect_from_collection(EVENT_COLLECTION))
        if EVENT_COLLECTION != "market_events" and len(docs) < max(2, max_items // 4):
            docs.extend(_collect_from_collection("market_events"))
    except Exception as e:
        _COLLECTION_COOLDOWN_UNTIL = time.time() + 300
        logger.warning("get_symbol_news_events failed for %s: %s | cooldown=300s", sym, e)
        return []

    seen = set()
    unique_docs: List[Dict[str, Any]] = []
    for x in docs:
        key = (str(x.get("date")), str(x.get("source")), str(x.get("headline", "")))
        if key in seen:
            continue
        seen.add(key)
        unique_docs.append(x)

    unique_docs.sort(
        key=lambda x: (
            0 if str(x.get("source", "")).startswith("targeted_news") else 1,
            x.get("date"),
        ),
        reverse=False,
    )
    unique_docs = sorted(unique_docs, key=lambda x: x.get("date"), reverse=True)

    return unique_docs[:max_items]

def summarize_symbol_news(symbol: str, lookback_days: int = 3, max_items: int = 24) -> Dict[str, Any]:
    """ChromaDB 已禁用，直接返回零强度。"""
    return {"total": 0, "targeted": 0, "monitor": 0, "sources": 0,
            "intensity_score": 0.0, "headline": ""}

    if not events:
        return {
            "symbol": str(symbol or ""),
            "total": 0,
            "targeted": 0,
            "monitor": 0,
            "unique_sources": 0,
            "intensity_score": 0.0,
            "headline": "",
            "highlights": [],
        }

    total = len(events)
    targeted = sum(1 for e in events if str(e.get("source", "")).startswith("targeted_news"))
    monitor = sum(1 for e in events if str(e.get("source", "")) == "monitor_scan")
    unique_sources = len({str(e.get("source", "")) for e in events if str(e.get("source", ""))})

    highlights = []
    for e in events:
        h = str(e.get("headline", "")).strip()
        if h and h not in highlights:
            highlights.append(h)
        if len(highlights) >= 3:
            break

    # 轻量强度模型：定向新闻权重更高，多源验证加分
    raw = targeted * 1.25 + (total - targeted) * 0.65 + min(unique_sources, 4) * 0.45
    intensity = round(min(1.0, raw / 8.0), 3)

    return {
        "symbol": str(symbol or ""),
        "total": total,
        "targeted": targeted,
        "monitor": monitor,
        "unique_sources": unique_sources,
        "intensity_score": intensity,
        "headline": "；".join(highlights[:2]),
        "highlights": highlights,
    }


def build_fallback_reason(symbol: str, lookback_days: int = 3) -> str:
    """ChromaDB 已禁用，返回默认文案。"""
    return "暂无新闻催化"

    if total <= 0:
        return "未检索到近端有效新闻催化，短线更可能受资金博弈或板块联动影响。"

    if headline:
        if targeted > 0:
            return f"近{lookback_days}日定向新闻催化：{headline}。"
        return f"近{lookback_days}日相关新闻摘要：{headline}。"

    return f"近{lookback_days}日有{total}条相关新闻，倾向板块情绪与资金共振驱动。"
