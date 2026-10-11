"""Source-labeled company context for the stock Trend detail card.

This first version deliberately does not infer news catalysts from price action.
Profiles are historical reference material, not current filings or investment advice.
"""
import re
from datetime import date

from sqlmodel import select

from core.db import get_meta_session
from domain.meta import Asset, AssetProfile, AssetIndustryLink, Industry


GROWTH_WORDS = re.compile(r"计划|拟在|未来将|持续(推进|拓展|加大|投资|布局|研发)|拓展|扩张|加大研发|战略布局|新业务|海外市场")
BUSINESS_WORDS = re.compile(r"主营|业务|产品|客户|服务|销售|研发|平台|市场|游戏|内容")
BOILERPLATE_WORDS = re.compile(r"成立于|注册资本|营业执照|工商行政|认定为|高新技术企业|领取了")


def _clean(value, limit=360):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit].rstrip("，、；; ")


def growth_clue(*texts):
    """Quote an explicit direction sentence; never turn a product list into a forecast."""
    for text in texts:
        for sentence in re.split(r"(?<=[。！？；;])", str(text or "")):
            sentence = _clean(sentence, 230)
            if 8 <= len(sentence) <= 230 and GROWTH_WORDS.search(sentence):
                return sentence
    return ""


def profile_highlight(text, main_business=""):
    """Prefer a business sentence over founding and registration boilerplate."""
    sentences = [_clean(s, 260) for s in re.split(r"(?<=[。！？])", str(text or ""))]
    candidates = [s for s in sentences if len(s) >= 12 and BUSINESS_WORDS.search(s)
                  and not BOILERPLATE_WORDS.search(s)]
    if not main_business:
        specific = [s for s in candidates if "世界领先" not in s and "科技向善" not in s]
        return _clean("".join(specific[:3] or candidates[:2]), 260)
    for sentence in candidates:
        if sentence not in str(main_business or "") and str(main_business or "") not in sentence:
            return _clean(sentence, 260)
    return _clean(candidates[0], 260) if candidates else ""


def _profile_symbol(session, market, symbol):
    candidates = [symbol]
    if market == "US" and not symbol.startswith("105."):
        candidates.append("105." + symbol)
    for candidate in candidates:
        asset = session.get(Asset, candidate)
        if asset and asset.market == market and asset.asset_type == "STOCK":
            return asset
    return None


def get_stock_narrative(market, symbol):
    market = str(market or "").upper().strip()
    symbol = str(symbol or "").upper().strip()
    if market not in {"CN", "HK", "US"} or not re.fullmatch(r"[A-Z0-9.\-]{1,24}", symbol):
        raise ValueError("invalid stock identity")

    with get_meta_session() as session:
        asset = _profile_symbol(session, market, symbol)
        if not asset:
            return {"market": market, "symbol": symbol, "name": "", "available": False}
        profile = session.get(AssetProfile, asset.symbol)
        industry_rows = session.exec(
            select(Industry.name, AssetIndustryLink.is_primary, Industry.level)
            .join(AssetIndustryLink, AssetIndustryLink.industry_code == Industry.code)
            .where(AssetIndustryLink.symbol == asset.symbol)
        ).all()
        industry_rows = sorted(industry_rows, key=lambda row: (bool(row[1]), int(row[2] or 0)), reverse=True)
        industry = str(industry_rows[0][0]) if industry_rows else ""

        business = _clean(profile.main_business if profile else "", 420)
        intro = profile_highlight(profile.company_profile if profile else "", business)
        products = _clean(profile.products if profile else "", 280)
        growth = growth_clue(profile.main_business if profile else "",
                             profile.company_profile if profile else "")
        updated = profile.updated_at.date().isoformat() if profile and profile.updated_at else ""

    # Resolve only on detail click, never on the Trend list request. The existing
    # resolver uses cached profile classifications and may consult external APIs.
    industry_source = "本地行业映射" if industry else ""
    if not industry:
        try:
            from modules.monitor.industry_intel import resolve_industry_label
            resolved = resolve_industry_label(symbol, market)
            label = str(resolved.get("label") or "").strip()
            if label and label != "综合":
                industry = label
                industry_source = {"profile": "公司资料规则推断", "external": "外部行业资料",
                                   "industry": "本地行业映射", "concept": "本地概念映射"}.get(
                                       resolved.get("source"), "辅助分类")
        except Exception:
            pass

    stale = False
    if updated:
        try:
            stale = (date.today() - date.fromisoformat(updated)).days > 180
        except ValueError:
            stale = True
    return {
        "market": market, "symbol": symbol, "name": asset.name, "available": bool(profile),
        "industry": industry, "industry_source": industry_source,
        "main_business": business, "company_profile": intro, "products": products,
        "growth_clue": growth, "profile_updated": updated, "profile_stale": stale,
        "profile_source": "本地公司资料库（原始采集：AkShare）" if profile else "",
        "market_focus": "暂无带来源和时间的近期公告/新闻证据；榜单涨幅或规则标签不能证明上涨原因。",
    }
