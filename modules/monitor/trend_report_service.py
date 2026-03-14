import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Dict, List

from core.llm import simple_prompt
from modules.monitor.notifier import Notifier
from modules.monitor.trend_service import TrendCalculator

logger = logging.getLogger(__name__)


class TrendReportService:
    """Trend 报告组装与推送（手动 /trend 与定时推送共用）"""

    MARKET_NAMES = {"CN": "A股", "HK": "港股", "US": "美股", "CF": "期货"}

    @staticmethod
    def _format_stock_line(i: int, s: dict) -> str:
        price = s.get("current_price", 0)
        ret = s.get("return_pct", 0)
        return f"{i}. {s.get('name','')}({s.get('symbol','')}) 现价:{price} {ret:+.2f}%"

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

            if market == "CF":
                picked = TrendReportService._pick_cf_items(stks)
            else:
                picked = TrendReportService._pick_market_items(market, stks)
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
                lines.append(prefix + TrendReportService._format_stock_line(i, s))

            lines.append(summaries.get(market, "主线逻辑：暂无\n资金抱团：暂无\n独立逻辑：暂无明显独立逻辑"))

        return "\n".join(lines).strip()

    @staticmethod
    def generate_and_push(days: int):
        report = TrendReportService.build_report(days)
        Notifier.broadcast(report)
        logger.info(f"Trend report pushed for {days} days.")
    LLM_TIMEOUT_SEC = 90
