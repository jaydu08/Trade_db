"""Source-labeled company context for the stock Trend detail card.

This first version deliberately does not infer news catalysts from price action.
Profiles are historical reference material, not current filings or investment advice.
"""
import re
import os
from pathlib import Path
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


def extract_sec_item1(html):
    """Extract a bounded Item 1 excerpt, skipping the 10-K table of contents."""
    import warnings
    from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all(['script', 'style', 'ix:header']):
        tag.decompose()
    body = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    heading = re.compile(r'\bitem\s*1\s*[.\-\s]*\s*business\b', re.I)
    next_heading = re.compile(r'\bitem\s*1a\s*[.\-\s]*\s*risk\s+factors\b', re.I)
    for match in heading.finditer(body):
        tail = body[match.end():]
        end = next_heading.search(tail)
        # A contents entry leads almost immediately to Item 1A; real Item 1
        # has a substantive section before the risk factors heading.
        if end and end.start() < 800:
            continue
        section = tail[:min(end.start() if end else 12000, 12000)].strip(' .:-')
        if len(section) < 500:
            continue
        excerpt = section[:1100].strip()
        last_sentence = max(excerpt.rfind('. '), excerpt.rfind('。'))
        if last_sentence >= 400:
            excerpt = excerpt[:last_sentence + 1]
        return excerpt
    return ''


def sec_business_info(ticker):
    """Official 10-K business excerpt; exact ticker/CIK match and disk cache."""
    ticker = str(ticker or '').upper().strip()
    if not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,12}', ticker):
        return {}
    key = 'stock_sec_item1_v2:' + ticker
    cached = get_cache(key)
    if isinstance(cached, dict):
        return cached
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / '.env.sec', override=False)
    user_agent = os.getenv('SEC_USER_AGENT', '').strip()
    if not user_agent or '@' not in user_agent:
        return {}
    headers = {'User-Agent': user_agent, 'Accept-Encoding': 'gzip, deflate'}
    try:
        ticker_map = get_cache('sec_ticker_exchange_v1')
        if not isinstance(ticker_map, dict):
            response = requests.get('https://www.sec.gov/files/company_tickers_exchange.json',
                                    headers=headers, timeout=8)
            response.raise_for_status()
            payload = response.json()
            fields = payload['fields']
            ticker_map = {}
            for row in payload['data']:
                item = dict(zip(fields, row))
                ticker_map.setdefault(str(item['ticker']).upper(), set()).add(int(item['cik']))
            set_cache('sec_ticker_exchange_v1', ticker_map, ttl=7 * 86400)
        ciks = ticker_map.get(ticker, set())
        if len(ciks) != 1:
            set_cache(key, {}, ttl=86400)
            return {}
        cik = next(iter(ciks))
        response = requests.get(f'https://data.sec.gov/submissions/CIK{cik:010d}.json',
                                headers=headers, timeout=8)
        response.raise_for_status()
        submission = response.json()
        if ticker not in [str(x).upper() for x in submission.get('tickers', [])]:
            return {}
        recent = submission['filings']['recent']
        index = next((i for i, form in enumerate(recent['form']) if form == '10-K'), None)
        if index is None:
            set_cache(key, {}, ttl=86400)
            return {}
        accession = str(recent['accessionNumber'][index]).replace('-', '')
        document = str(recent['primaryDocument'][index])
        if not re.fullmatch(r'[A-Za-z0-9_.\-]+', document):
            return {}
        url = f'https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}'
        response = requests.get(url, headers=headers, timeout=12)
        response.raise_for_status()
        if len(response.content) > 12_000_000:
            return {}
        excerpt = extract_sec_item1(response.content)
        if not excerpt:
            set_cache(key, {}, ttl=86400)
            return {}
        result = {'business': excerpt, 'filing_date': recent['filingDate'][index],
                  'filing_url': url, 'form': '10-K',
                  'industry': _clean(submission.get('sicDescription'), 100)}
        set_cache(key, result, ttl=30 * 86400)
        return result
    except (requests.RequestException, ValueError, KeyError, TypeError, IndexError):
        set_cache(key, {}, ttl=600)
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

    stale = False
    if updated:
        try:
            stale = (date.today() - date.fromisoformat(updated)).days > 180
        except ValueError:
            stale = True

    us_ticker = asset.symbol.split('.', 1)[-1] if market == 'US' else ''
    external_identity = finnhub_company_identity(us_ticker) if market == 'US' else {}
    sec_info = sec_business_info(us_ticker) if market == 'US' and (not business or len(business) < 180 or stale) else {}
    local_business = bool(business)
    if not business and sec_info.get('business'):
        business = sec_info['business']
    # Resolve only on detail click, never on the Trend list request. The existing
    # resolver uses cached profile classifications and may consult external APIs.
    industry_source = "本地行业映射" if industry else ""
    if not industry and sec_info.get('industry'):
        industry = sec_info['industry']
        industry_source = 'SEC SIC 行业分类'
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

    return {
        "market": market, "symbol": symbol, "name": asset.name, "available": bool(profile or external_identity or sec_info),
        "industry": industry, "industry_source": industry_source,
        "main_business": business, "company_profile": intro, "products": products,
        "business_source": "本地中文公司资料" if local_business else "SEC 10-K 英文原文" if sec_info else "",
        "website": external_identity.get('website', ''),
        "profile_quality": 'complete' if business and intro else 'partial' if business or intro or industry else 'missing',
        "growth_clue": growth, "profile_updated": updated, "profile_stale": stale,
        "profile_source": "本地公司资料库（原始采集：AkShare）" if profile else "SEC 10-K Item 1（英文原文）" if sec_info else "",
        "sec_business": sec_info.get('business', ''),
        "sec_filing_date": sec_info.get('filing_date', ''),
        "sec_filing_url": sec_info.get('filing_url', ''),
        "market_focus": "暂无带来源和时间的近期公告/新闻证据；榜单涨幅或规则标签不能证明上涨原因。",
    }
