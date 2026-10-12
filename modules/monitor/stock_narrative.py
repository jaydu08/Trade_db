"""Source-labeled company context for the stock Trend detail card.

This first version deliberately does not infer news catalysts from price action.
Profiles are historical reference material, not current filings or investment advice.
"""
import re
import os
from datetime import date

import requests

from sqlmodel import select

from core.db import get_meta_session
from core.cache import get_cache, set_cache
from domain.meta import Asset, AssetProfile, AssetIndustryLink, Industry


GROWTH_WORDS = re.compile(r"计划|拟在|未来将|持续(推进|拓展|加大|投资|布局|研发)|拓展|扩张|加大研发|战略布局|新业务|海外市场")
BUSINESS_WORDS = re.compile(r"主营|业务|产品|客户|服务|销售|研发|平台|市场|游戏|内容")
BOILERPLATE_WORDS = re.compile(r"成立于|注册资本|营业执照|工商行政|认定为|高新技术企业|领取了")


def _clean(value, limit=360):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
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


def business_detail(market, main_business, intro):
    """Enrich terse US business labels with source text, without inventing products."""
    business = _clean(main_business, 420)
    if market != 'US':
        return business
    detail = profile_highlight(intro, "")
    if not business:
        return detail
    if len(business) < 45 and detail and detail not in business and business not in detail:
        return _clean(business.rstrip('。；;') + '；' + detail, 420)
    return business


def finnhub_company_identity(symbol):
    """Supplement industry/site only; Finnhub profile2 has no business description."""
    key = 'stock_identity_finnhub_v1:' + symbol
    cached = get_cache(key)
    if isinstance(cached, dict):
        return cached
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv('FINNHUB_API_KEY', '').strip()
    if not token:
        return {}
    try:
        response = requests.get('https://finnhub.io/api/v1/stock/profile2',
                                params={'symbol': symbol, 'token': token}, timeout=8)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get('ticker', '').upper() != symbol:
            return {}
        site = str(payload.get('weburl') or '').strip()
        result = {'industry': _clean(payload.get('finnhubIndustry'), 100),
                  'website': site[:300] if site.startswith('https://') else '',
                  'company_name': _clean(payload.get('name'), 120)}
        set_cache(key, result, ttl=86400)
        return result
    except (requests.RequestException, ValueError):
        return {}


def _profile_symbol(session, market, symbol):
    candidates = [symbol]
    if market == "US" and "." not in symbol:
        # Eastmoney uses 105 (Nasdaq), 106 (NYSE) and 107 (other US listings).
        # Trend and TG bars can carry either the full code or the bare ticker.
        candidates.extend(f"{prefix}.{symbol}" for prefix in ("105", "106", "107"))
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

        raw_business = _clean(profile.main_business if profile else "", 420)
        business = business_detail(market, raw_business, profile.company_profile if profile else "")
        intro = profile_highlight(profile.company_profile if profile else "", "" if market == "US" else business)
        products = _clean(profile.products if profile else "", 280)
        growth = growth_clue(profile.main_business if profile else "",
                             profile.company_profile if profile else "")
        updated = profile.updated_at.date().isoformat() if profile and profile.updated_at else ""

    external_identity = finnhub_company_identity(asset.symbol.split('.', 1)[-1]) if market == 'US' else {}
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
    if not industry and external_identity.get('industry'):
        industry = external_identity['industry']
        industry_source = 'Finnhub 公司资料'

    stale = False
    if updated:
        try:
            stale = (date.today() - date.fromisoformat(updated)).days > 180
        except ValueError:
            stale = True
    return {
        "market": market, "symbol": symbol, "name": asset.name, "available": bool(profile or external_identity),
        "industry": industry, "industry_source": industry_source,
        "main_business": business, "company_profile": intro, "products": products,
        "website": external_identity.get('website', ''),
        "profile_quality": 'complete' if business and intro else 'partial' if business or intro or industry else 'missing',
        "growth_clue": growth, "profile_updated": updated, "profile_stale": stale,
        "profile_source": "本地公司资料库（原始采集：AkShare）" if profile else "",
        "market_focus": "暂无带来源和时间的近期公告/新闻证据；榜单涨幅或规则标签不能证明上涨原因。",
    }
