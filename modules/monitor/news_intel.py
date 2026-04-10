import datetime as dt
import logging
import re
import time
from typing import Any, Dict, List

from core.db import get_collection

logger = logging.getLogger(__name__)
_COLLECTION_COOLDOWN_UNTIL = 0.0


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
    """从 market_events 提取标的相关新闻事件（严格+宽松双通道）。"""
    global _COLLECTION_COOLDOWN_UNTIL
    sym = str(symbol or "").strip()
    if not sym:
        return []

    docs: List[Dict[str, Any]] = []

    # 向量库异常时进入冷却窗口，避免每个标的反复重试拖慢主流程
    if time.time() < float(_COLLECTION_COOLDOWN_UNTIL):
        return []

    try:
        collection = get_collection("market_events")

        strict = collection.query(
            query_texts=[sym],
            n_results=max(20, max_items * 2),
            where={"related_symbols": sym},
        )
        if strict and strict.get("documents"):
            for doc, meta in zip(strict["documents"][0], strict.get("metadatas", [[]])[0]):
                meta = meta or {}
                d = _parse_event_date(meta.get("event_date"))
                if not d or d < start_date:
                    continue
                docs.append(
                    {
                        "date": d,
                        "source": str(meta.get("source", "")),
                        "document": str(doc or ""),
                        "headline": _doc_headline(doc),
                    }
                )

        # 若严格通道命中很少，走宽松召回补充
        if len(docs) < max(4, max_items // 3):
            relaxed = collection.query(query_texts=[sym], n_results=max(30, max_items * 3))
            if relaxed and relaxed.get("documents"):
                for doc, meta in zip(relaxed["documents"][0], relaxed.get("metadatas", [[]])[0]):
                    meta = meta or {}
                    if not _is_related_symbol(meta.get("related_symbols"), sym):
                        continue
                    d = _parse_event_date(meta.get("event_date"))
                    if not d or d < start_date:
                        continue
                    docs.append(
                        {
                            "date": d,
                            "source": str(meta.get("source", "")),
                            "document": str(doc or ""),
                            "headline": _doc_headline(doc),
                        }
                    )
    except Exception as e:
        _COLLECTION_COOLDOWN_UNTIL = time.time() + 300
        logger.warning("get_symbol_news_events failed for %s: %s | cooldown=300s", sym, e)
        return []

    seen = set()
    unique_docs: List[Dict[str, Any]] = []
    for x in docs:
        key = (str(x.get("date")), str(x.get("source")), str(x.get("document", ""))[:120])
        if key in seen:
            continue
        seen.add(key)
        unique_docs.append(x)

    # 排序：优先 targeted_news，其次日期新
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
    """输出标的新闻强度摘要，供 trend/heatmap/复盘复用。"""
    days = max(1, int(lookback_days or 3))
    start_date = dt.date.today() - dt.timedelta(days=days)
    events = get_symbol_news_events(symbol=symbol, start_date=start_date, max_items=max_items)

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
    """LLM 不可用时，基于近端事件生成可读兜底归因。"""
    meta = summarize_symbol_news(symbol, lookback_days=lookback_days, max_items=18)
    total = int(meta.get("total", 0) or 0)
    targeted = int(meta.get("targeted", 0) or 0)
    headline = str(meta.get("headline", "")).strip()

    if total <= 0:
        return "未检索到近端有效新闻催化，短线更可能受资金博弈或板块联动影响。"

    if headline:
        if targeted > 0:
            return f"近{lookback_days}日定向新闻催化：{headline}。"
        return f"近{lookback_days}日相关新闻摘要：{headline}。"

    return f"近{lookback_days}日有{total}条相关新闻，倾向板块情绪与资金共振驱动。"
