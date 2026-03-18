"""
Daily Summary Service - 每日推送标的汇总服务
每工作日北京时间 20:00 把当日所有推送过的标的（热榜股票 + 大宗商品）
汇总写入 logs/daily_summary_YYYYMMDD.txt
"""
import logging
import datetime
import os
from typing import List, Dict

from sqlmodel import select, Session

from core.db import db_manager
from domain.ledger.analytics import DailyRank, TrendDailyBar

logger = logging.getLogger(__name__)

# 输出目录（与 system_run.log 同级）
SUMMARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
))), "logs")


class DailySummaryService:
    """
    汇总当日所有推送标的并写入 TXT 文件。

    数据来源：
    ─ DailyRank (rank_type='change_pct')   → A股 / 港股 / 美股热榜
    ─ TrendDailyBar (market='CF')           → 大宗商品战报
    """

    @staticmethod
    def generate_and_save(target_date: datetime.date = None) -> str:
        """
        生成并保存当日汇总。
        返回写入的文件路径（若无数据则返回空字符串）。
        """
        if target_date is None:
            from zoneinfo import ZoneInfo
            target_date = datetime.datetime.now(ZoneInfo("Asia/Shanghai")).date()

        logger.info("生成每日推送汇总: %s", target_date)

        engine = db_manager.ledger_engine
        with Session(engine) as session:
            # ── 1. 股票热榜（DailyRank，涨幅榜，去重保留最高涨幅的一条）
            ranks: List[DailyRank] = session.exec(
                select(DailyRank).where(
                    DailyRank.date == target_date,
                    DailyRank.rank_type == "change_pct",
                )
            ).all()

            # ── 2. 大宗商品（TrendDailyBar，market='CF'，数据源=commodity）
            bars: List[TrendDailyBar] = session.exec(
                select(TrendDailyBar).where(
                    TrendDailyBar.date == target_date,
                    TrendDailyBar.market == "CF",
                    TrendDailyBar.source == "commodity",
                )
            ).all()

        # 按市场分组（股票）
        market_map: Dict[str, List[DailyRank]] = {"CN": [], "HK": [], "US": []}
        seen = {}  # symbol → 已保留的最高涨幅记录
        for r in ranks:
            key = (r.market, r.symbol)
            if key not in seen or r.change_pct > seen[key].change_pct:
                seen[key] = r
        for r in seen.values():
            if r.market in market_map:
                market_map[r.market].append(r)

        # 各市场按涨幅降序
        for mkt in market_map:
            market_map[mkt].sort(key=lambda x: x.change_pct, reverse=True)

        # 大宗商品去重（同 symbol 保留一条）；涨幅从 open/close 反算
        def _bar_pct(b: TrendDailyBar) -> float:
            if b.open and b.open > 0 and b.close and b.close > 0:
                return round((b.close - b.open) / b.open * 100, 2)
            return 0.0

        cf_seen = {}
        for b in bars:
            pct = _bar_pct(b)
            prev = cf_seen.get(b.symbol)
            if prev is None or pct > _bar_pct(prev):
                cf_seen[b.symbol] = b
        cf_list = sorted(cf_seen.values(), key=_bar_pct, reverse=True)

        # 无任何数据时静默退出
        total = sum(len(v) for v in market_map.values()) + len(cf_list)
        if total == 0:
            logger.info("当日暂无推送标的，跳过汇总写入。")
            return ""

        # ── 组装文本
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            "=" * 42,
            "   TradeDB 每日推送标的汇总",
            "=" * 42,
            f"生成时间: {now_str}",
            f"统计日期: {target_date}",
            "=" * 42,
            "",
        ]

        market_labels = {
            "CN": "🇨🇳 A股 - 热榜标的",
            "HK": "🇭🇰 港股 - 热榜标的",
            "US": "🇺🇸 美股 - 热榜标的",
        }

        for mkt in ["CN", "HK", "US"]:
            items = market_map[mkt]
            if not items:
                continue
            lines.append(f"【{market_labels[mkt]}】")
            for i, r in enumerate(items, 1):
                price_str = f"{r.price:.2f}" if r.price else "N/A"
                lines.append(
                    f"  {i:>2}. {r.name} ({r.symbol})"
                    f" | 现价: {price_str}"
                    f" | 涨幅: +{r.change_pct:.2f}%"
                )
            lines.append("")

        if cf_list:
            lines.append("【🛢️ 大宗商品 - 战报标的】")
            for i, b in enumerate(cf_list, 1):
                price_str = f"{b.close:.2f}" if b.close else "N/A"
                pct = _bar_pct(b)
                pct_str = f"+{pct:.2f}%" if pct >= 0 else f"{pct:.2f}%"
                lines.append(
                    f"  {i:>2}. {b.name} ({b.symbol})"
                    f" | 现价: {price_str}"
                    f" | 涨幅: {pct_str}"
                )
            lines.append("")

        lines += [
            "=" * 42,
            f"合计推送标的数: {total}",
            "=" * 42,
        ]

        content = "\n".join(lines)

        # ── 写入文件
        os.makedirs(SUMMARY_DIR, exist_ok=True)
        filename = f"daily_summary_{target_date.strftime('%Y%m%d')}.txt"
        filepath = os.path.join(SUMMARY_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info("每日汇总已写入: %s  (共 %d 条标的)", filepath, total)
        return filepath


# 全局单例
daily_summary_service = DailySummaryService()
