"""
Daily Summary Service - 每日推送标的汇总服务
每工作日北京时间 20:00 把当日所有推送过的标的（热榜股票 + 大宗商品）
汇总写入 logs/daily_summary_YYYYMMDD.txt
"""
import logging
import datetime
import os
import requests
from typing import List, Dict

from sqlmodel import select, Session

from core.db import db_manager
from domain.ledger.analytics import TrendDailyBar
from modules.monitor.notifier import Notifier
from modules.ingestion.market_cap import (
    get_cn_market_metrics,
    get_hk_market_metrics,
    format_mv_cn,
    format_mv_hk,
)

logger = logging.getLogger(__name__)

# 输出目录（与 system_run.log 同级）
SUMMARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
))), "logs")


class DailySummaryService:
    """
    汇总当日所有推送标的并写入 TXT 文件。

    数据来源：
    ─ TrendDailyBar (source='heatmap')        → A股 / 港股 / 美股热榜
    ─ TrendDailyBar (source='commodity')      → 大宗商品战报
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

        cn_mv_cache = {}
        hk_mv_cache = {}
        us_mv_cache = {}
        finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()

        def _format_market_cap(market: str, symbol: str) -> str:
            symbol = str(symbol or "").strip()
            if not symbol:
                return "市值:N/A"

            if market == "CN":
                if symbol not in cn_mv_cache:
                    try:
                        metrics = get_cn_market_metrics(symbol)
                        total_mv = float((metrics or {}).get("total_mv_100m", 0) or 0)
                        circ_mv = float((metrics or {}).get("circ_mv_100m", 0) or 0)
                        mv_txt = format_mv_cn(total_mv, circ_mv)
                        cn_mv_cache[symbol] = f"市值:{mv_txt}" if mv_txt else "市值:N/A"
                    except Exception:
                        cn_mv_cache[symbol] = "市值:N/A"
                return cn_mv_cache[symbol]

            if market == "HK":
                if symbol not in hk_mv_cache:
                    hk_mv_cache[symbol] = "市值:N/A"
                    try:
                        metrics = get_hk_market_metrics(symbol)
                        cap_hkd = float((metrics or {}).get("market_cap_100m_hkd", 0) or 0)
                        cap_usd = float((metrics or {}).get("market_cap_100m_usd", 0) or 0)
                        mv_txt = format_mv_hk(cap_hkd, cap_usd)
                        if mv_txt:
                            hk_mv_cache[symbol] = mv_txt
                    except Exception:
                        pass
                return hk_mv_cache[symbol]

            if market == "US":
                if symbol not in us_mv_cache:
                    us_mv_cache[symbol] = "市值:N/A"
                    if finnhub_key:
                        try:
                            payload = requests.get(
                                "https://finnhub.io/api/v1/stock/profile2",
                                params={"symbol": symbol.split(".")[-1], "token": finnhub_key},
                                timeout=8,
                            ).json()
                            cap_musd = float((payload or {}).get("marketCapitalization", 0) or 0)
                            if cap_musd > 0:
                                us_mv_cache[symbol] = f"市值:{cap_musd/100.0:.2f}亿美元"
                        except Exception:
                            pass
                return us_mv_cache[symbol]

            return "市值:N/A"

        # 涨幅统一从 open/close 反算
        def _bar_pct(b: TrendDailyBar) -> float:
            if b.open and b.open > 0 and b.close and b.close > 0:
                return round((b.close - b.open) / b.open * 100, 2)
            return 0.0

        engine = db_manager.ledger_engine
        with Session(engine) as session:
            # ── 1. 股票热榜（TrendDailyBar，source='heatmap'）
            stock_bars: List[TrendDailyBar] = session.exec(
                select(TrendDailyBar).where(
                    TrendDailyBar.date == target_date,
                    TrendDailyBar.source == "heatmap",
                )
            ).all()

            # ── 2. 大宗商品（TrendDailyBar，source='commodity'）
            cf_bars: List[TrendDailyBar] = session.exec(
                select(TrendDailyBar).where(
                    TrendDailyBar.date == target_date,
                    TrendDailyBar.source == "commodity",
                )
            ).all()

        # 按市场分组（股票）
        market_map: Dict[str, List[TrendDailyBar]] = {"CN": [], "HK": [], "US": []}
        seen = {}  # symbol → 已保留的最高涨幅记录
        for r in stock_bars:
            key = (r.market, r.symbol)
            pct = _bar_pct(r)
            prev = seen.get(key)
            if prev is None or pct > _bar_pct(prev):
                seen[key] = r
                
        for r in seen.values():
            if r.market in market_map:
                market_map[r.market].append(r)

        # 各市场按涨幅降序
        for mkt in market_map:
            market_map[mkt].sort(key=_bar_pct, reverse=True)

        # 大宗商品去重（同 symbol 保留一条）
        cf_seen = {}
        for b in cf_bars:
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
                price_str = f"{r.close:.2f}" if r.close else "N/A"
                pct = _bar_pct(r)
                pct_str = f"+{pct:.2f}%" if pct >= 0 else f"{pct:.2f}%"
                mv_str = _format_market_cap(mkt, r.symbol)
                lines.append(
                    f"  {i:>2}. {r.name} ({r.symbol})"
                    f" | 现价: {price_str}"
                    f" | 涨幅: {pct_str}"
                    f" | {mv_str}"
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
        
        # 增加推送到 Telegram 的逻辑
        caption = f"📊 TradeDB 每日汇总 ({target_date})\n合计推送标的数: {total}"
        try:
            Notifier.broadcast_document(filepath, caption=caption)
            logger.info("每日汇总已成功通过 Telegram 广播。")
        except Exception as e:
            logger.error("每日汇总推送到 Telegram 失败: %s", e)

        return filepath


# 全局单例
daily_summary_service = DailySummaryService()
