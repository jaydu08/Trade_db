import datetime as dt
import concurrent.futures
import logging
import json
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from modules.monitor.notifier import Notifier
from core.agent import Tools
from core.llm import simple_prompt

logger = logging.getLogger(__name__)


@dataclass
class IPOItem:
    market: str
    symbol: str
    name: str
    listing_date: str
    industry: str = ""
    main_business: str = ""
    raise_info: str = ""
    extra: str = ""


class IPOCalendarService:
    MARKET_NAMES = {"CN": "A股", "HK": "港股", "US": "美股"}

    @staticmethod
    def _tomorrow_shanghai() -> dt.date:
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date() + dt.timedelta(days=1)

    @staticmethod
    def _fmt_cn_symbol(symbol: str) -> str:
        s = str(symbol or "").strip()
        return s.zfill(6)

    @staticmethod
    def _safe_text(value) -> str:
        if value is None:
            return ""
        t = str(value).strip()
        return "" if t.lower() in {"nan", "nat", "none"} else t

    @staticmethod
    def _parse_date_like(value) -> str:
        if value is None:
            return ""
        if hasattr(value, "strftime"):
            try:
                return value.strftime("%Y-%m-%d")
            except Exception:
                pass
        raw = str(value).strip()
        if not raw:
            return ""
        raw = raw.replace("/", "-")
        m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", raw)
        if m:
            y, mo, d = m.groups()
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
        digits = re.sub(r"\D", "", raw)
        if len(digits) >= 8:
            return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        return raw

    @staticmethod
    def _fmt_raise_cn(total_shares_10k, issue_price, net_raise_10k) -> str:
        shares = float(total_shares_10k or 0)
        price = float(issue_price or 0)
        net_raise = float(net_raise_10k or 0)
        parts = []
        if net_raise > 0:
            parts.append(f"募资净额{net_raise/10000:.2f}亿元")
        if shares > 0 and price > 0:
            gross = shares * 10000 * price / 100000000
            parts.append(f"预计募资{gross:.2f}亿元")
        elif shares > 0:
            parts.append(f"发行总量{shares:.2f}万股")
        return " / ".join(parts)

    @staticmethod
    def _fetch_cn(tomorrow: dt.date) -> List[IPOItem]:
        import akshare as ak

        target = tomorrow.strftime("%Y-%m-%d")
        out: List[IPOItem] = []

        try:
            df = ak.stock_new_ipo_cninfo()
        except Exception as e:
            logger.warning("CN IPO fetch failed: %s", e)
            return out

        if df is None or getattr(df, "empty", True):
            return out

        enable_enrich = os.getenv("IPO_CN_ENRICH", "0") == "1"

        for _, row in df.iterrows():
            listing_date = IPOCalendarService._parse_date_like(row.get("上市日期"))
            if listing_date != target:
                continue

            symbol = IPOCalendarService._fmt_cn_symbol(row.get("证劵代码"))
            name = IPOCalendarService._safe_text(row.get("证券简称"))
            issue_price = row.get("发行价")
            total_shares = row.get("总发行数量")

            main_business = ""
            raise_info = IPOCalendarService._fmt_raise_cn(total_shares, issue_price, 0)
            extra_bits = []
            sg_date = IPOCalendarService._parse_date_like(row.get("申购日期"))
            if sg_date:
                extra_bits.append(f"申购日:{sg_date}")
            if IPOCalendarService._safe_text(issue_price):
                extra_bits.append(f"发行价:{issue_price}")
            if IPOCalendarService._safe_text(row.get("发行市盈率")):
                extra_bits.append(f"发行PE:{row.get('发行市盈率')}")

            # 可选深度补充：默认关闭，避免慢接口拖垮定时任务
            if enable_enrich:
                try:
                    info_df = ak.stock_ipo_info(stock=symbol)
                    if info_df is not None and not info_df.empty:
                        m = {str(r["item"]).strip(): str(r["value"]).strip() for _, r in info_df.iterrows()}
                        main_business = m.get("主营业务", "") or m.get("经营范围", "")
                except Exception:
                    pass

                try:
                    sum_df = ak.stock_ipo_summary_cninfo(symbol=symbol)
                    if sum_df is not None and not sum_df.empty:
                        net_raise = sum_df.iloc[0].get("募集资金净额")
                        raise_info = IPOCalendarService._fmt_raise_cn(total_shares, issue_price, net_raise)
                except Exception:
                    pass

            out.append(
                IPOItem(
                    market="CN",
                    symbol=symbol,
                    name=name,
                    listing_date=listing_date,
                    main_business=main_business,
                    raise_info=raise_info,
                    extra=" | ".join(extra_bits),
                )
            )

        return out

    @staticmethod
    def _fetch_us(tomorrow: dt.date) -> List[IPOItem]:
        key = os.getenv("FINNHUB_API_KEY", "").strip()
        if not key:
            return []

        d = tomorrow.strftime("%Y-%m-%d")
        try:
            resp = requests.get(
                "https://finnhub.io/api/v1/calendar/ipo",
                params={"from": d, "to": d, "token": key},
                timeout=15,
            )
            data = resp.json() if resp is not None else {}
        except Exception as e:
            logger.warning("US IPO fetch failed: %s", e)
            return []

        rows = data.get("ipoCalendar", []) if isinstance(data, dict) else []
        out: List[IPOItem] = []

        for r in rows:
            symbol = IPOCalendarService._safe_text(r.get("symbol"))
            name = IPOCalendarService._safe_text(r.get("name"))
            listing_date = IPOCalendarService._parse_date_like(r.get("date"))

            industry = ""
            try:
                p = requests.get(
                    "https://finnhub.io/api/v1/stock/profile2",
                    params={"symbol": symbol, "token": key},
                    timeout=8,
                ).json()
                industry = IPOCalendarService._safe_text(p.get("finnhubIndustry"))
            except Exception:
                pass

            shares = float(r.get("numberOfShares") or 0)
            total = float(r.get("totalSharesValue") or 0)
            price = IPOCalendarService._safe_text(r.get("price"))

            raise_parts = []
            if total > 0:
                raise_parts.append(f"预计募资{total/100000000:.2f}亿美元")
            if shares > 0:
                raise_parts.append(f"发行{shares/1000000:.2f}百万股")
            if price:
                raise_parts.append(f"价格区间{price}美元")

            out.append(
                IPOItem(
                    market="US",
                    symbol=symbol,
                    name=name,
                    listing_date=listing_date,
                    industry=industry,
                    raise_info=" / ".join(raise_parts),
                    extra=f"交易所:{IPOCalendarService._safe_text(r.get('exchange'))}",
                )
            )

        return out

    @staticmethod
    def _fetch_hk(tomorrow: dt.date) -> List[IPOItem]:
        url = "https://www.aastocks.com/en/stocks/market/ipo/upcomingipo/company-summary?s=3&o=1&s3=1&o3=1"
        try:
            from lxml import html
            page = requests.get(url, timeout=20).text
            doc = html.fromstring(page)
        except Exception as e:
            logger.warning("HK IPO fetch failed: %s", e)
            return []

        target = tomorrow.strftime("%Y-%m-%d")
        out: List[IPOItem] = []

        # AASTOCKS 页面通常包含 tblGMUpcoming（含 Listing Date 列）
        tables = doc.xpath("//table[contains(@id, 'Upcoming')]")
        for t in tables:
            headers = [x.text_content().strip().replace("\n", " ") for x in t.xpath(".//thead//td")]
            if not headers or not any("Listing Date" in h for h in headers):
                continue

            rows = t.xpath(".//tbody/tr")
            for r in rows:
                vals = [c.text_content().strip().replace("\n", " ") for c in r.xpath("./td")]
                if not vals:
                    continue
                joined = " ".join(vals)
                if "No Upcoming IPO" in joined:
                    continue

                # 以 header 下标映射字段
                def idx(name: str) -> int:
                    for i, h in enumerate(headers):
                        if name in h:
                            return i
                    return -1

                i_name = idx("Name")
                i_ind = idx("Industry")
                i_offer = idx("Offer Price")
                i_lot = idx("Lot Size")
                i_fee = idx("Entry Fee")
                i_close = idx("Closing Date")
                i_gm = idx("Grey Market Date")
                i_list = idx("Listing Date")

                name_code = vals[i_name] if 0 <= i_name < len(vals) else ""
                m = re.search(r"\((\d{3,5})\)", name_code)
                symbol = m.group(1) if m else ""
                name = re.sub(r"\s*\(\d{3,5}\)\s*", "", name_code).strip()

                listing_date = IPOCalendarService._parse_date_like(vals[i_list] if 0 <= i_list < len(vals) else "")
                if listing_date != target:
                    continue

                industry = vals[i_ind] if 0 <= i_ind < len(vals) else ""
                offer = vals[i_offer] if 0 <= i_offer < len(vals) else ""
                lot = vals[i_lot] if 0 <= i_lot < len(vals) else ""
                fee = vals[i_fee] if 0 <= i_fee < len(vals) else ""
                close_date = vals[i_close] if 0 <= i_close < len(vals) else ""
                gm_date = vals[i_gm] if 0 <= i_gm < len(vals) else ""

                raise_info = " / ".join([x for x in [f"招股价{offer}" if offer else "", f"入场费{fee}" if fee else ""] if x])
                extra = " | ".join([x for x in [f"每手{lot}" if lot else "", f"截止{close_date}" if close_date else "", f"暗盘{gm_date}" if gm_date else ""] if x])

                out.append(
                    IPOItem(
                        market="HK",
                        symbol=symbol,
                        name=name,
                        listing_date=listing_date,
                        industry=industry,
                        main_business=industry,
                        raise_info=raise_info,
                        extra=extra,
                    )
                )

        return out

    @staticmethod
    def _extract_json_obj(text: str) -> Dict:
        raw = str(text or "").strip()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            pass
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return {}
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}

    @staticmethod
    def _ai_enrich_one(item: IPOItem) -> None:
        """联网搜索 + 大模型补全，失败静默降级。"""
        query = f"{item.market} 新股 {item.name} {item.symbol} {item.listing_date} 主营业务 募资"
        try:
            search_ctx = Tools.web_search(query)
        except Exception as e:
            logger.warning("IPO search failed for %s(%s): %s", item.name, item.symbol, e)
            return

        if not search_ctx or "未返回有效结果" in search_ctx:
            return

        prompt = f"""
你是 IPO 数据补全助手。请基于给定信息和搜索结果，只输出 JSON，不要额外文本：
{{
  "main_business": "不超过40字，可为空",
  "raise_info": "不超过40字，可为空",
  "risk_tip": "不超过30字，可为空"
}}

已知信息:
- 市场: {item.market}
- 公司: {item.name}
- 代码: {item.symbol}
- 上市日: {item.listing_date}
- 已有主营/行业: {item.main_business or item.industry}
- 已有募资信息: {item.raise_info}

搜索结果:
{search_ctx[:2500]}
"""

        try:
            resp = simple_prompt(prompt, temperature=0.1)
            obj = IPOCalendarService._extract_json_obj(resp)
        except Exception as e:
            logger.warning("IPO LLM enrich failed for %s(%s): %s", item.name, item.symbol, e)
            return

        if not isinstance(obj, dict):
            return

        mb = IPOCalendarService._safe_text(obj.get("main_business"))
        ri = IPOCalendarService._safe_text(obj.get("raise_info"))
        rk = IPOCalendarService._safe_text(obj.get("risk_tip"))

        if mb and not item.main_business:
            item.main_business = mb
        if ri and not item.raise_info:
            item.raise_info = ri
        if rk:
            tip = f"AI提示:{rk}"
            if item.extra:
                if tip not in item.extra:
                    item.extra = f"{item.extra} | {tip}"
            else:
                item.extra = tip

    @staticmethod
    def _enrich_with_search_llm(all_items: Dict[str, List[IPOItem]]) -> None:
        """增强链路总开关；任何失败均不影响主推送。"""
        if os.getenv("IPO_AI_ENRICH", "1") != "1":
            return

        flat: List[IPOItem] = []
        for m in ["CN", "HK", "US"]:
            flat.extend(all_items.get(m, []))
        if not flat:
            return

        max_items = int(os.getenv("IPO_AI_MAX_ITEMS", "6") or 6)
        targets = flat[:max_items]

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(targets))) as ex:
            futures = [ex.submit(IPOCalendarService._ai_enrich_one, it) for it in targets]
            for f in concurrent.futures.as_completed(futures):
                try:
                    f.result(timeout=45)
                except Exception:
                    continue

    @staticmethod
    def _collect_tomorrow_items() -> Dict:
        tomorrow = IPOCalendarService._tomorrow_shanghai()

        cn = IPOCalendarService._fetch_cn(tomorrow)
        hk = IPOCalendarService._fetch_hk(tomorrow)
        us = IPOCalendarService._fetch_us(tomorrow)

        all_items = {"CN": cn, "HK": hk, "US": us}

        # 搜索+大模型补全（可选增强，失败不影响主流程）
        try:
            IPOCalendarService._enrich_with_search_llm(all_items)
        except Exception as e:
            logger.warning("IPO AI enrich pipeline failed but ignored: %s", e)

        total = sum(len(v) for v in all_items.values())
        return {"tomorrow": tomorrow, "all_items": all_items, "total": total}

    @staticmethod
    def _render_tomorrow_report(tomorrow: dt.date, all_items: Dict[str, List[IPOItem]], total: int) -> str:
        lines = [f"🆕 明日新股预告 ({tomorrow.strftime('%Y-%m-%d')})", f"覆盖市场: A股 / 港股 / 美股 | 共 {total} 只", ""]

        for mkt in ["CN", "HK", "US"]:
            items = all_items[mkt]
            lines.append(f"【{IPOCalendarService.MARKET_NAMES[mkt]}】")
            if not items:
                lines.append("- 暂无明日上市新股")
                lines.append("")
                continue

            for i, it in enumerate(items, 1):
                title = f"{i}. {it.name}"
                if it.symbol:
                    title += f" ({it.symbol})"
                lines.append(title)
                lines.append(f"   上市日: {it.listing_date}")

                biz = it.main_business or it.industry
                if biz:
                    lines.append(f"   主营/行业: {biz}")
                if it.raise_info:
                    lines.append(f"   募资信息: {it.raise_info}")
                if it.extra:
                    lines.append(f"   其他: {it.extra}")
                lines.append("   提示: 关注招股进度、定价区间与流动性风险。")
                lines.append("")

        return "\n".join(lines).strip()

    @staticmethod
    def build_tomorrow_report() -> str:
        payload = IPOCalendarService._collect_tomorrow_items()
        return IPOCalendarService._render_tomorrow_report(
            payload["tomorrow"], payload["all_items"], payload["total"]
        )

    @staticmethod
    def generate_and_push_tomorrow() -> Dict:
        payload = IPOCalendarService._collect_tomorrow_items()
        total = int(payload.get("total", 0) or 0)
        if total <= 0:
            logger.info("No IPO listing for tomorrow, skip ipo_tomorrow push.")
            return {"ok": True, "skipped": True, "reason": "no_ipo_tomorrow"}

        msg = IPOCalendarService._render_tomorrow_report(
            payload["tomorrow"], payload["all_items"], total
        )
        Notifier.broadcast(msg)
        return {"ok": True, "length": len(msg), "total": total}


ipo_calendar_service = IPOCalendarService()
