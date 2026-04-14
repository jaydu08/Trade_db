import os
import re
import time
import logging
import requests
from typing import Dict, List, Tuple

from sqlmodel import select

from core.db import get_meta_session
from core.cache import get_cache, set_cache
from domain.meta import AssetProfile, AssetIndustryLink, Industry, AssetConceptLink, Concept

logger = logging.getLogger(__name__)

_IN_MEMORY_CACHE: Dict[str, Tuple[float, Dict[str, str]]] = {}
_CACHE_TTL = int(os.getenv("INDUSTRY_CACHE_TTL", "86400") or 86400)

_FINE_RULES: List[Tuple[str, List[str]]] = [
    ("CPO/光模块", ["cpo", "光模块", "光通信", "硅光", "光器件", "高速光模块", "光芯片"]),
    ("液冷", ["液冷", "浸没式", "冷板", "热管理", "散热系统"]),
    ("AI算力芯片", ["ai芯片", "算力芯片", "gpu", "npu", "asic", "训练芯片"]),
    ("半导体设备", ["半导体设备", "刻蚀", "沉积", "清洗设备", "量测", "光刻胶"]),
    ("先进封装", ["先进封装", "chiplet", "2.5d", "3d封装", "封装测试", "封测"]),
    ("存储芯片", ["dram", "nand", "闪存", "存储芯片", "存储器"]),
    ("服务器/交换机", ["服务器", "交换机", "交换芯片", "网络设备", "机柜"]),
    ("工业软件", ["工业软件", "工业互联网", "plm", "mes", "cad", "cae"]),
    ("机器人", ["机器人", "机械臂", "协作机器人", "减速器", "伺服系统"]),
    ("创新药", ["创新药", "单抗", "双抗", "adc", "小分子药", "药物研发"]),
    ("医疗器械", ["医疗器械", "体外诊断", "ivd", "影像设备", "监护", "耗材"]),
    ("锂电池", ["锂电", "动力电池", "电解液", "隔膜", "正极材料", "负极材料"]),
    ("光伏", ["光伏", "逆变器", "电池片", "硅片", "组件", "topcon", "hjt"]),
    ("风电", ["风电", "风机", "叶片", "塔筒", "海上风电"]),
    ("军工航空", ["航天", "航空", "导弹", "雷达", "军工", "军机", "卫星"]),
    ("航运/港口", ["航运", "集运", "港口", "码头", "船舶运输"]),
    ("煤炭", ["煤炭", "焦煤", "动力煤"]),
    ("有色金属", ["铜", "铝", "锂矿", "钴", "镍", "稀土", "黄金"]),
    ("银行", ["银行", "商业银行", "城商行", "农商行"]),
    ("保险", ["保险", "寿险", "财险", "再保险"]),
]




_GROUP_RULES: List[Tuple[str, List[str]]] = [
    ("新能源", ["新能源", "锂", "电池", "储能", "光伏", "风电", "电解液", "隔膜"]),
    ("医药", ["医药", "医疗", "药", "器械", "生物"]),
    ("半导体", ["半导体", "芯片", "封装", "光模块", "cpo", "硅光", "存储"]),
    ("军工", ["军工", "航天", "航空", "导弹", "卫星"]),
    ("金融", ["银行", "保险", "证券", "信托"]),
    ("有色", ["有色", "铜", "铝", "锂矿", "钴", "镍", "稀土", "黄金"]),
]

_LABEL_GROUP_HINT: Dict[str, str] = {
    "CPO/光模块": "半导体",
    "液冷": "半导体",
    "AI算力芯片": "半导体",
    "半导体设备": "半导体",
    "先进封装": "半导体",
    "存储芯片": "半导体",
    "锂电池": "新能源",
    "光伏": "新能源",
    "风电": "新能源",
    "创新药": "医药",
    "医疗器械": "医药",
    "军工航空": "军工",
    "银行": "金融",
    "保险": "金融",
    "有色金属": "有色",
}


def _infer_group(text: str) -> str:
    t = str(text or "").lower()
    for grp, kws in _GROUP_RULES:
        for kw in kws:
            if kw in t:
                return grp
    return ""
def _normalize_symbol(symbol: str, market: str) -> str:
    s = str(symbol or "").strip().upper()
    if market == "US" and "." in s:
        return s.split(".")[-1].strip()
    return s


def _candidate_symbols(symbol: str, market: str) -> List[str]:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return []
    cands = [raw]
    if market == "US":
        if "." in raw:
            cands.append(raw.split(".")[-1].strip())
        else:
            cands.append(f"105.{raw}")
    out = []
    seen = set()
    for x in cands:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _finnhub_candidates(symbol: str, market: str) -> List[str]:
    s = str(symbol or "").strip().upper()
    if not s:
        return []

    out: List[str] = []
    if market == "US":
        out.append(s.split(".")[-1] if "." in s else s)
    elif market == "HK":
        raw = s.split(".", 1)[0] if s.endswith(".HK") else s
        digits = "".join(ch for ch in raw if ch.isdigit())
        if digits:
            out.append(f"{digits[-4:].zfill(4)}.HK")
            out.append(f"{digits[-5:].zfill(5)}.HK")
    elif market == "CN":
        raw = s.lower()
        if raw.startswith(("sh", "sz", "bj")) and len(raw) >= 8:
            code = raw[2:]
        else:
            code = s
        code = "".join(ch for ch in code if ch.isdigit())
        if code:
            if code.startswith(("6", "9")):
                out.append(f"{code}.SS")
            else:
                out.append(f"{code}.SZ")

    uniq = []
    seen = set()
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _hk_profile_candidates(symbol: str) -> List[str]:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return []
    if raw.endswith(".HK"):
        raw = raw.split(".", 1)[0]
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return []
    out = [digits[-5:].zfill(5), digits[-4:].zfill(4).zfill(5)]
    uniq = []
    seen = set()
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _fetch_hk_industry_via_akshare(symbol: str) -> str:
    try:
        from modules.ingestion.akshare_client import akshare_client
    except Exception:
        return ""

    for code in _hk_profile_candidates(symbol):
        try:
            df = akshare_client.get_stock_profile_hk(code)
            if df is None or getattr(df, "empty", True):
                continue
            row = df.iloc[0]
            for col in ("所属行业", "行业", "industry"):
                if col in df.columns:
                    val = row.get(col)
                    label = _normalize_external_label(val)
                    if label:
                        return label
        except Exception:
            continue
    return ""


def _normalize_external_label(text: str) -> str:
    t = str(text or "").strip()
    if not t:
        return ""
    lower = t.lower()

    mapping = [
        ("半导体", ["semiconductor", "chip", "integrated circuit"]),
        ("科技", ["technology", "tech"]),
        ("软件服务", ["software", "saas", "internet software"]),
        ("互联网平台", ["internet", "interactive media", "online media"]),
        ("云计算", ["cloud", "data center"]),
        ("通信设备", ["communication", "telecom", "network"]),
        ("消费电子", ["consumer electronics", "electronic components"]),
        ("电动车", ["auto", "automotive", "ev", "electric vehicle"]),
        ("医疗健康", ["healthcare", "biotech", "pharma", "medical"]),
        ("金融", ["bank", "insurance", "financial"]),
        ("能源", ["energy", "oil", "gas", "coal"]),
    ]
    for zh, kws in mapping:
        if any(k in lower for k in kws):
            return zh
    return t[:24]


def _fetch_external_industry(symbol: str, market: str) -> str:
    m = str(market or "").upper()

    if m == "HK":
        hk_label = _fetch_hk_industry_via_akshare(symbol)
        if hk_label:
            return hk_label

    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not key:
        return ""

    for sym in _finnhub_candidates(symbol, m):
        try:
            resp = requests.get(
                "https://finnhub.io/api/v1/stock/profile2",
                params={"symbol": sym, "token": key},
                timeout=5,
            )
            data = resp.json() if resp is not None else {}
            label = _normalize_external_label((data or {}).get("finnhubIndustry", ""))
            if label:
                return label
        except Exception:
            continue
    return ""


def _score_rules(text: str, weight: float, scores: Dict[str, float]) -> None:
    t = str(text or "").lower()
    if not t:
        return
    for label, kws in _FINE_RULES:
        hits = 0
        for kw in kws:
            if kw in t:
                hits += 1
        if hits:
            scores[label] = scores.get(label, 0.0) + weight * hits






_CONCEPT_UNLOCK_LABELS = {
    "CPO/光模块",
    "液冷",
    "AI算力芯片",
    "半导体设备",
    "先进封装",
    "存储芯片",
    "服务器/交换机",
    "工业软件",
    "机器人",
}
def _label_supported_by_concepts(label: str, concepts: List[str]) -> bool:
    if not label or not concepts:
        return False
    if label not in _CONCEPT_UNLOCK_LABELS:
        return False
    blob = " ".join(str(x or "") for x in concepts).lower()
    for lb, kws in _FINE_RULES:
        if lb != label:
            continue
        for kw in kws:
            if kw and kw in blob:
                return True
    return False
def _pick_best_label(profile_text: str, industries: List[str], concepts: List[str]) -> str:
    scores: Dict[str, float] = {}

    # 主营文本权重最高，满足“以核心营收为准”
    _score_rules(profile_text, 3.0, scores)
    _score_rules(" ".join(industries), 2.0, scores)
    _score_rules(" ".join(concepts), 1.2, scores)

    best_label = ""
    best_score = 0.0
    if scores:
        best_label, best_score = sorted(scores.items(), key=lambda x: x[1], reverse=True)[0]

    concept_supported = _label_supported_by_concepts(best_label, concepts)

    # 防误判：若细分标签得分不够高，优先回退到结构化行业
    if industries and best_score < 4.0 and not concept_supported:
        industries = sorted(industries, key=lambda x: len(str(x or "")), reverse=True)
        return str(industries[0])

    # 结构化行业与细分标签发生大类冲突时，以行业表为准（更贴近核心营收）
    if industries and best_label:
        top_industry = sorted(industries, key=lambda x: len(str(x or "")), reverse=True)[0]
        g_ind = _infer_group(top_industry)
        g_lab = _LABEL_GROUP_HINT.get(best_label, _infer_group(best_label))
        if g_ind and g_lab and g_ind != g_lab and best_score < 8.0 and not concept_supported:
            return str(top_industry)

    if best_label:
        return best_label

    # 回退：优先行业表，再概念表
    if industries:
        industries = sorted(industries, key=lambda x: len(str(x or "")), reverse=True)
        return str(industries[0])
    if concepts:
        concepts = sorted(concepts, key=lambda x: len(str(x or "")), reverse=True)
        return str(concepts[0])
    return "综合"


def _cache_key(symbol: str, market: str) -> str:
    return f"industry_v5:{market}:{symbol}"


def _get_cached(symbol: str, market: str) -> Dict[str, str] | None:
    key = _cache_key(symbol, market)
    now = time.time()
    mem = _IN_MEMORY_CACHE.get(key)
    if mem and now - mem[0] <= _CACHE_TTL:
        return mem[1]

    disk = get_cache(key)
    if isinstance(disk, dict) and disk.get("label"):
        _IN_MEMORY_CACHE[key] = (now, disk)
        return disk
    return None


def _set_cached(symbol: str, market: str, payload: Dict[str, str]) -> None:
    key = _cache_key(symbol, market)
    _IN_MEMORY_CACHE[key] = (time.time(), payload)
    set_cache(key, payload, ttl=_CACHE_TTL)


def _build_symbol_meta(candidates: List[str]) -> Dict[str, Dict[str, List[str]]]:
    data: Dict[str, Dict[str, List[str]]] = {
        c: {"profiles": [], "industries": [], "concepts": []} for c in candidates
    }
    if not candidates:
        return data

    try:
        with get_meta_session() as session:
            p_rows = session.exec(
                select(
                    AssetProfile.symbol,
                    AssetProfile.main_business,
                    AssetProfile.business_scope,
                    AssetProfile.products,
                    AssetProfile.company_profile,
                ).where(AssetProfile.symbol.in_(candidates))
            ).all()
            for sym, mb, scope, prods, profile in p_rows:
                text = " ".join([
                    str(mb or ""),
                    str(scope or ""),
                    str(prods or ""),
                    str(profile or "")[:600],
                ]).strip()
                if text:
                    data.setdefault(sym, {"profiles": [], "industries": [], "concepts": []})["profiles"].append(text)

            i_rows = session.exec(
                select(
                    AssetIndustryLink.symbol,
                    Industry.name,
                    AssetIndustryLink.is_primary,
                    Industry.level,
                )
                .join(Industry, Industry.code == AssetIndustryLink.industry_code)
                .where(AssetIndustryLink.symbol.in_(candidates))
            ).all()
            for sym, iname, is_primary, level in i_rows:
                n = str(iname or "").strip()
                if not n:
                    continue
                score = (10 if bool(is_primary) else 0) + int(level or 0)
                data.setdefault(sym, {"profiles": [], "industries": [], "concepts": []})["industries"].append(f"{score}:{n}")

            c_rows = session.exec(
                select(
                    AssetConceptLink.symbol,
                    Concept.name,
                    AssetConceptLink.weight,
                    AssetConceptLink.is_primary,
                )
                .join(Concept, Concept.code == AssetConceptLink.concept_code)
                .where(AssetConceptLink.symbol.in_(candidates))
            ).all()
            for sym, cname, weight, is_primary in c_rows:
                n = str(cname or "").strip()
                if not n:
                    continue
                w = float(weight or 0)
                score = w + (0.5 if bool(is_primary) else 0)
                data.setdefault(sym, {"profiles": [], "industries": [], "concepts": []})["concepts"].append(f"{score:.3f}:{n}")
    except Exception as e:
        logger.warning("Industry meta query failed: %s", e)

    return data


def _cleanup_weighted(items: List[str]) -> List[str]:
    out: List[Tuple[float, str]] = []
    for x in items:
        m = re.match(r"^([\d\.]+):(.+)$", str(x))
        if not m:
            continue
        try:
            out.append((float(m.group(1)), m.group(2).strip()))
        except Exception:
            continue
    out.sort(key=lambda p: p[0], reverse=True)
    names = []
    seen = set()
    for _, n in out:
        if n and n not in seen:
            seen.add(n)
            names.append(n)
    return names


def resolve_industry_label(symbol: str, market: str) -> Dict[str, str]:
    market = str(market or "").upper()
    raw_symbol = str(symbol or "").strip()
    if not raw_symbol:
        return {"label": "", "source": ""}

    cached = _get_cached(raw_symbol, market)
    if cached:
        return cached

    candidates = _candidate_symbols(raw_symbol, market)
    meta = _build_symbol_meta(candidates)

    best = {"label": "综合", "source": "fallback"}
    for cand in candidates:
        m = meta.get(cand, {})
        profiles = m.get("profiles", [])
        industries = _cleanup_weighted(m.get("industries", []))
        concepts = _cleanup_weighted(m.get("concepts", []))
        label = _pick_best_label("\n".join(profiles), industries, concepts)
        source = "profile" if profiles else ("industry" if industries else ("concept" if concepts else "fallback"))
        if label and label != "综合":
            best = {"label": label[:24], "source": source}
            break

    if best.get("label") in {"", "综合"}:
        ext = _fetch_external_industry(raw_symbol, market)
        if ext:
            best = {"label": ext[:24], "source": "external"}

    _set_cached(raw_symbol, market, best)
    return best


def enrich_industry_labels(stocks: List[Dict], market: str) -> None:
    mkt = str(market or "").upper()
    if not stocks:
        return
    for s in stocks:
        sym = str(s.get("symbol", "")).strip()
        if not sym:
            continue
        data = resolve_industry_label(sym, mkt)
        s["industry_label"] = str(data.get("label", "") or "")
        s["industry_source"] = str(data.get("source", "") or "")
