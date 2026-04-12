import logging
import os
import datetime as dt
import gc
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Dict, List

from modules.ingestion.market_cap import (
    get_cn_market_metrics,
    get_cn_fund_flow,
    format_mv_cn,
    format_flow_cn,
)

from core.llm import simple_prompt
from modules.monitor.notifier import Notifier
from modules.monitor.trend_service import TrendCalculator

logger = logging.getLogger(__name__)


class TrendReportService:
    @staticmethod
    def _current_rss_mb() -> float:
        try:
            with open('/proc/self/status', 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        kb = float(line.split()[1])
                        return round(kb / 1024.0, 1)
        except Exception:
            pass
        return 0.0

    """Trend 报告组装与推送（手动 /trend 与定时推送共用）"""

    MARKET_NAMES = {"CN": "A股", "HK": "港股", "US": "美股", "CF": "期货"}

    @staticmethod
    def _format_stock_line(i: int, s: dict, market: str) -> str:
        price = s.get("current_price", 0)
        ret = s.get("return_pct", 0)
        symbol = s.get("symbol", "")
        news_cnt = int(s.get("news_count", 0) or 0)
        news_text = f" 📰{news_cnt}条/3d" if news_cnt > 0 else ""

        if market == "US":
            display_symbol = str(symbol).split(".")[-1].strip() if symbol else symbol
            cap_100m = float(s.get("market_cap_100m_usd", 0) or 0)
            cap_text = f" 市值:{cap_100m:.1f}亿美元" if cap_100m > 0 else ""
            return f"{i}. ({display_symbol}) 现价:{price} {ret:+.2f}%{cap_text}{news_text}"

        if market == "CN":
            total_mv = float(s.get("total_mv_100m", 0) or 0)
            circ_mv = float(s.get("circ_mv_100m", 0) or 0)
            main_inflow = float(s.get("main_net_inflow_100m", 0) or 0)
            mv_txt = format_mv_cn(total_mv, circ_mv)
            flow_txt = format_flow_cn(main_inflow)
            cap_text = f" {mv_txt}" if mv_txt else ""
            flow_text = f" {flow_txt}" if flow_txt else ""
            return f"{i}. {s.get('name','')}({symbol}) 现价:{price} {ret:+.2f}%{cap_text}{flow_text}{news_text}"

        return f"{i}. {s.get('name','')}({symbol}) 现价:{price} {ret:+.2f}%{news_text}"

    @staticmethod
    def _refresh_market_prices(market: str, stks: List[dict]) -> None:
        """
        用实时行情补齐/修正 trend 展示价：
        - US: 优先使用 Finnhub（通过 DataManager provider 顺序）
        - HK/CN: 若当前价缺失(<=0)再尝试补齐
        失败时静默回退到本地 TrendDailyBar 价格。
        """
        from modules.ingestion.data_factory import data_manager
        if not stks:
            return

        def _need_refresh(s: dict) -> bool:
            p = float(s.get("current_price", 0) or 0)
            if market == "US":
                return True
            return p <= 0

        targets = [s for s in stks if _need_refresh(s)]
        if not targets:
            return

        def _fetch_one(s: dict):
            symbol = str(s.get("symbol", "")).strip()
            if not symbol:
                return None
            quote = data_manager.get_quote(symbol, market)
            if not quote:
                return None
            if market == "US" and quote.get("provider") != "Finnhub":
                return None
            p = float(quote.get("price", 0) or 0)
            if p <= 0:
                return None
            return symbol, p

        with ThreadPoolExecutor(max_workers=min(8, len(targets))) as executor:
            future_map = {executor.submit(_fetch_one, s): s for s in targets}
            for future, stock in future_map.items():
                try:
                    out = future.result(timeout=6)
                    if out:
                        _, price = out
                        stock["current_price"] = round(price, 4)
                except Exception:
                    continue

    @staticmethod
    def _enrich_market_caps(market: str, stks: List[dict]) -> None:
        """补齐趋势推送的市值与资金流信息（当前重点支持 A 股）。"""
        if not stks:
            return

        if market == "CN":
            def _fetch_one(s: dict):
                symbol = str(s.get("symbol", "")).strip()
                if not symbol:
                    return None
                trade_date = str(s.get("price_date", "") or "")
                metrics = get_cn_market_metrics(symbol)
                flow = get_cn_fund_flow(symbol, trade_date=trade_date)
                return s, metrics, flow

            with ThreadPoolExecutor(max_workers=min(8, len(stks))) as executor:
                future_map = [executor.submit(_fetch_one, s) for s in stks]
                for future in future_map:
                    try:
                        out = future.result(timeout=6)
                        if not out:
                            continue
                        stock, metrics, flow = out
                        if metrics:
                            total_mv = float(metrics.get("total_mv_100m", 0) or 0)
                            circ_mv = float(metrics.get("circ_mv_100m", 0) or 0)
                            if total_mv > 0:
                                stock["total_mv_100m"] = total_mv
                            if circ_mv > 0:
                                stock["circ_mv_100m"] = circ_mv
                        if flow:
                            main_inflow = float(flow.get("main_net_inflow_100m", 0) or 0)
                            if main_inflow != 0:
                                stock["main_net_inflow_100m"] = main_inflow
                    except Exception:
                        continue

    @staticmethod
    def _pick_market_items(market: str, stks: List[dict]) -> List[dict]:
        # 港股只推前5
        if market == "HK":
            return stks[:5]
        # 其他市场做精简
        return stks[:8]

    @staticmethod
    def _pick_cf_items(stks: List[dict], min_n: int = 3, max_n: int = 5) -> List[dict]:
        """期货市场合并同类项后，保留 3-5 个代表标的"""
        from modules.monitor.commodity_scanner import CommodityScanner

        grouped: Dict[str, List[dict]] = defaultdict(list)
        for s in stks:
            cat = CommodityScanner._determine_category(str(s.get("name", "")))
            grouped[cat].append(s)

        # 每类取 trend_score 最高的一个
        reps: List[dict] = []
        for cat, items in grouped.items():
            items_sorted = sorted(items, key=lambda x: x.get("trend_score", x.get("return_pct", 0)), reverse=True)
            top = dict(items_sorted[0])
            top["category"] = cat
            reps.append(top)

        reps.sort(key=lambda x: x.get("trend_score", x.get("return_pct", 0)), reverse=True)

        # 保底3个，最多5个
        if len(reps) < min_n:
            extra = [s for s in stks if s not in reps]
            reps.extend(extra[: (min_n - len(reps))])
        return reps[:max_n]

    @staticmethod
    def _llm_summary(market: str, days: int, stks: List[dict]) -> str:
        market_name = TrendReportService.MARKET_NAMES.get(market, market)
        prompt = (
            f"你是一名交易复盘分析师。请基于以下{market_name}{days}日趋势标的，"
            "仅输出3行，每行不超过50字，不要括号解释：\n"
            "1) 主线逻辑：...\n"
            "2) 资金抱团：...\n"
            "3) 独立逻辑：列出1-2个独立走势标的；若无写“暂无明显独立逻辑”。\n"
            "禁止输出emoji、禁止Markdown符号。\n\n"
            "数据：\n"
        )
        for i, s in enumerate(stks, 1):
            prompt += (
                f"{i}. {s.get('name')}({s.get('symbol')}) "
                f"涨幅:{s.get('return_pct',0):+.2f}% "
                f"趋势分:{s.get('trend_score',0)} "
                f"信号:{s.get('signal_strength',0)}\n"
            )

        try:
            rss_mb = TrendReportService._current_rss_mb()
            if rss_mb > float(os.getenv("TREND_LLM_SKIP_RSS_MB", "1500") or 1500):
                return "主线逻辑：内存保护降级\n资金抱团：以内生价格信号为准\n独立逻辑：暂无明显独立逻辑"
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(simple_prompt, prompt, temperature=0.1)
                out = future.result(timeout=TrendReportService.LLM_TIMEOUT_SEC)
            return str(out).strip()
        except FutureTimeout:
            logger.warning(f"Trend LLM summary timeout for {market}, fallback used.")
            return "主线逻辑：暂无\n资金抱团：暂无\n独立逻辑：暂无明显独立逻辑"
        except Exception as e:
            logger.warning(f"Trend LLM summary failed for {market}: {e}")
            return "主线逻辑：暂无\n资金抱团：暂无\n独立逻辑：暂无明显独立逻辑"

    @staticmethod
    def build_report(days: int) -> str:
        tops = TrendCalculator.calculate_trend(days)
        if not tops:
            return f"📭 最近{days}日暂无有效趋势标的。"

        lines: List[str] = [f"Trend {days}日趋势推送"]
        market_order = ["CN", "HK", "US", "CF"]
        selected_by_market: Dict[str, List[dict]] = {}

        for market in market_order:
            stks = tops.get(market, [])
            if not stks:
                continue
            stks = [s for s in stks if TrendReportService._is_fresh_price(market, str(s.get("price_date", "")))]
            if not stks:
                continue

            if market == "CF":
                picked = TrendReportService._pick_cf_items(stks)
            else:
                picked = TrendReportService._pick_market_items(market, stks)
            # 补齐市值字段（主要用于推送展示）
            TrendReportService._enrich_market_caps(market, picked)

            # Trend 报告只使用本地收盘价序列，避免实时行情口径波动
            selected_by_market[market] = picked

        # 并行生成各市场 3 行总结，避免串行调用导致 /trend 长时间等待
        summaries: Dict[str, str] = {}
        if selected_by_market:
            with ThreadPoolExecutor(max_workers=min(4, len(selected_by_market))) as executor:
                future_map = {
                    market: executor.submit(TrendReportService._llm_summary, market, days, picked)
                    for market, picked in selected_by_market.items()
                }
                for market, future in future_map.items():
                    try:
                        summaries[market] = future.result(timeout=TrendReportService.LLM_TIMEOUT_SEC + 5)
                    except FutureTimeout:
                        summaries[market] = "主线逻辑：暂无\n资金抱团：暂无\n独立逻辑：暂无明显独立逻辑"
                    except Exception:
                        summaries[market] = "主线逻辑：暂无\n资金抱团：暂无\n独立逻辑：暂无明显独立逻辑"

        if not selected_by_market:
            return f"📭 最近{days}日暂无新鲜收盘价趋势标的。"

        for market in market_order:
            picked = selected_by_market.get(market)
            if not picked:
                continue
            market_name = TrendReportService.MARKET_NAMES.get(market, market)
            lines.append(f"\n【{market_name}】")
            for i, s in enumerate(picked, 1):
                prefix = ""
                if market == "CF" and s.get("category"):
                    prefix = f"{s.get('category')}-"
                lines.append(prefix + TrendReportService._format_stock_line(i, s, market))

            lines.append(summaries.get(market, "主线逻辑：暂无\n资金抱团：暂无\n独立逻辑：暂无明显独立逻辑"))

        out = "\n".join(lines).strip()
        gc.collect()
        return out

    @staticmethod
    def generate_and_push(days: int):
        report = TrendReportService.build_report(days)
        Notifier.broadcast(report)
        logger.info(f"Trend report pushed for {days} days.")
    LLM_TIMEOUT_SEC = int(os.getenv("TREND_LLM_TIMEOUT_SEC", "180") or 180)
    @staticmethod
    def _is_fresh_price(market: str, price_date: str) -> bool:
        if not price_date:
            return False
        try:
            d = dt.datetime.strptime(price_date, "%Y-%m-%d").date()
        except Exception:
            return False
        max_age = 4 if market in {"US", "CF"} else 3
        return (dt.date.today() - d).days <= max_age
