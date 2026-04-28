"""
Pattern Tagger — 技术形态标签

纯量价计算, 不依赖外部数据源。
为热榜标的生成技术形态标签: 一字涨停 / 涨停板 / 放量跳空 / 缩量新高 / 底部突破 / 放量拉升 / 温和上涨
"""

import logging
import datetime as dt
from typing import Dict, List, Optional

from sqlmodel import select, col

logger = logging.getLogger(__name__)

# A股涨跌停比例
_CN_LIMIT_NORMAL = 10.0   # 主板/创业板
_CN_LIMIT_ST = 5.0
_CN_LIMIT_KC = 20.0       # 科创板 (688)
_CN_LIMIT_CY = 20.0       # 创业板 (300) 注册制后20%


def tag_patterns(stocks: List[Dict], market: str) -> None:
    """
    为热榜标的计算技术形态标签, 写入 stock['pattern_tag']。
    依赖 TrendDailyBar 历史数据 (最近20个交易日)。
    """
    market = str(market or "").upper()
    if not stocks:
        return

    # 批量获取历史数据
    symbols = [str(s.get("symbol", "")).strip() for s in stocks if s.get("symbol")]
    history = _batch_fetch_history(symbols, market, days=20)

    for s in stocks:
        sym = str(s.get("symbol", "")).strip()
        bars = history.get(sym, [])
        tag = _compute_tag(s, bars, market)
        s["pattern_tag"] = tag


def _batch_fetch_history(symbols: List[str], market: str, days: int = 20) -> Dict[str, List[Dict]]:
    """从 TrendDailyBar 批量获取最近 N 天历史数据"""
    result: Dict[str, List[Dict]] = {s: [] for s in symbols}
    if not symbols:
        return result

    try:
        from domain.ledger.analytics import TrendDailyBar
        from core.db import db_manager

        cutoff = dt.date.today() - dt.timedelta(days=days + 10)  # 留余量

        with db_manager.ledger_session() as session:
            stmt = (
                select(TrendDailyBar)
                .where(
                    TrendDailyBar.market == market,
                    TrendDailyBar.symbol.in_(symbols),
                    TrendDailyBar.date >= cutoff,
                    TrendDailyBar.date < dt.date.today(),  # 不含今天
                )
                .order_by(col(TrendDailyBar.date).desc())
            )
            rows = session.exec(stmt).all()

            for row in rows:
                sym = row.symbol
                if sym in result:
                    result[sym].append({
                        "date": row.date,
                        "open": float(row.open or 0),
                        "high": float(row.high or 0),
                        "low": float(row.low or 0),
                        "close": float(row.close or 0),
                        "amount": float(row.amount or 0),
                    })

        # 每个 symbol 的 bars 按日期降序 (最新在前)
        for sym in result:
            result[sym].sort(key=lambda b: b["date"], reverse=True)

    except Exception as e:
        logger.warning("Failed to fetch TrendDailyBar history: %s", e)

    return result


def _compute_tag(stock: Dict, bars: List[Dict], market: str) -> str:
    """根据当日数据 + 历史 bars 计算形态标签"""
    pct = float(stock.get("pct_chg", 0) or 0)
    price = float(stock.get("price", 0) or 0)
    amount = float(stock.get("amount", 0) or 0)
    open_price = float(stock.get("open", 0) or 0)
    high = float(stock.get("high", 0) or 0)

    # 历史平均成交额 (最近5天)
    recent_amounts = [b["amount"] for b in bars[:5] if b["amount"] > 0]
    avg_5d_amount = sum(recent_amounts) / len(recent_amounts) if recent_amounts else 0

    # 前一交易日收盘价
    prev_close = bars[0]["close"] if bars else 0

    # 20日最高/最低
    all_highs = [b["high"] for b in bars[:20] if b["high"] > 0]
    all_lows = [b["low"] for b in bars[:20] if b["low"] > 0]
    high_20d = max(all_highs) if all_highs else 0
    low_20d = min(all_lows) if all_lows else 0

    # ── A股涨停判断 ──
    if market == "CN":
        limit = _get_cn_limit(stock)
        is_limit_up = pct >= limit * 0.97

        if is_limit_up:
            # 一字涨停: open == close == high (允许极小误差)
            if open_price > 0 and high > 0:
                tolerance = price * 0.002
                if (abs(open_price - price) < tolerance and
                    abs(high - price) < tolerance):
                    return "一字涨停"
            return "涨停板"

    # ── 放量跳空 ──
    if prev_close > 0 and open_price > 0:
        gap_pct = (open_price - prev_close) / prev_close
        if gap_pct > 0.02 and avg_5d_amount > 0 and amount > avg_5d_amount * 2:
            return "放量跳空"

    # ── 缩量新高 ──
    if high_20d > 0 and price >= high_20d * 0.98:
        if avg_5d_amount > 0 and amount < avg_5d_amount * 0.8:
            return "缩量新高"

    # ── 底部突破 ──
    if low_20d > 0 and prev_close > 0 and pct > 5:
        # 前日收盘在20日低点附近 (±10%)
        if prev_close <= low_20d * 1.10:
            return "底部突破"

    # ── 放量拉升 ──
    if avg_5d_amount > 0 and amount > avg_5d_amount * 2:
        return "放量拉升"

    # ── 默认 ──
    return "温和上涨"


def _get_cn_limit(stock: Dict) -> float:
    """根据 A 股代码推断涨跌停幅度"""
    sym = str(stock.get("symbol", "")).strip()
    name = str(stock.get("name", "")).strip()

    # ST 股
    if "ST" in name.upper():
        return _CN_LIMIT_ST

    # 提取纯数字
    code = "".join(c for c in sym if c.isdigit())

    # 科创板 688
    if code.startswith("688"):
        return _CN_LIMIT_KC

    # 创业板 300
    if code.startswith("300") or code.startswith("301"):
        return _CN_LIMIT_CY

    return _CN_LIMIT_NORMAL
