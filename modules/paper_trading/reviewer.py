"""
Paper Trading Reviewer
模拟交易系统 - AI 复盘教练引擎
"""
import datetime
import logging
import os
import traceback
from typing import List, Optional

import pandas as pd

from core.llm import simple_prompt
from domain.ledger.paper_trade import PaperTrade
from modules.ingestion.akshare_client import akshare_client
from modules.monitor.news_intel import get_symbol_news_events, summarize_symbol_news

logger = logging.getLogger(__name__)


class PaperTradeReviewer:
    """提供紧凑的 AI 交易复盘短评"""

    REVIEW_MAX_CHARS = int(os.getenv("PAPER_REVIEW_MAX_CHARS", "100"))

    @staticmethod
    def _compact_review(raw: str) -> str:
        """仅做文本清洗，不做截断。"""
        text = str(raw or "").replace("\r", " ").replace("\n", " ").strip()
        text = text.replace("**", "").replace("`", "").replace("###", "").replace("##", "").replace("#", "")
        while "  " in text:
            text = text.replace("  ", " ")
        return text

    @staticmethod
    def _fetch_historical_kbars(
        symbol: str,
        market: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> pd.DataFrame:
        """从 akshare 抓取目标时间段的前复权日K线"""
        sd = start_date.strftime("%Y%m%d")
        ed = end_date.strftime("%Y%m%d")

        try:
            if market == "CN":
                df = akshare_client._safe_call(
                    ["stock_zh_a_hist"],
                    symbol=symbol,
                    period="daily",
                    start_date=sd,
                    end_date=ed,
                    adjust="qfq",
                )
                if df.empty:
                    return df
                df["date"] = pd.to_datetime(df["日期"])
                return df.rename(columns={"开盘": "open", "最高": "high", "最低": "low", "收盘": "close"})

            if market == "HK":
                df = akshare_client._safe_call(
                    ["stock_hk_hist"],
                    symbol=symbol,
                    start_date=sd,
                    end_date=ed,
                    adjust="qfq",
                )
                if df.empty:
                    return df
                df["date"] = pd.to_datetime(df["日期"])
                return df.rename(columns={"开盘": "open", "最高": "high", "最低": "low", "收盘": "close"})

            if market == "US":
                clean_sym = symbol.split(".")[-1]
                df = akshare_client._safe_call(
                    ["stock_us_hist"],
                    symbol=clean_sym,
                    start_date=sd,
                    end_date=ed,
                    adjust="qfq",
                )
                if df.empty:
                    return df
                df["date"] = pd.to_datetime(df["日期"])
                return df.rename(columns={"开盘": "open", "最高": "high", "最低": "low", "收盘": "close"})

        except Exception as e:
            logger.error(f"Failed to fetch kbars for {symbol} ({market}): {e}")

        return pd.DataFrame()

    @staticmethod
    def _calc_pct_from_df(df: pd.DataFrame) -> Optional[float]:
        if df is None or df.empty:
            return None

        cols = set(df.columns)
        if "收盘" in cols:
            start_p = float(df.iloc[0].get("收盘", 0) or 0)
            end_p = float(df.iloc[-1].get("收盘", 0) or 0)
        elif "close" in cols:
            start_p = float(df.iloc[0].get("close", 0) or 0)
            end_p = float(df.iloc[-1].get("close", 0) or 0)
        else:
            return None

        if start_p <= 0:
            return None
        return round(((end_p - start_p) / start_p) * 100, 2)

    @staticmethod
    def _fetch_market_index(
        market: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> Optional[float]:
        """获取同期大盘基准涨跌幅，失败返回 None（避免误导为 0）。"""
        sd = start_date.strftime("%Y%m%d")
        ed = end_date.strftime("%Y%m%d")

        try:
            if market == "CN":
                df = akshare_client._safe_call(
                    ["index_zh_a_hist"],
                    symbol="000001",
                    period="daily",
                    start_date=sd,
                    end_date=ed,
                )
                return PaperTradeReviewer._calc_pct_from_df(df)

            if market == "HK":
                for idx in ["HSI", "800000"]:
                    df = akshare_client._safe_call(["index_hk_hist"], symbol=idx, start_date=sd, end_date=ed)
                    pct = PaperTradeReviewer._calc_pct_from_df(df)
                    if pct is not None:
                        return pct
                return None

            if market == "US":
                for idx in [".INX", ".IXIC", ".DJI"]:
                    df = akshare_client._safe_call(["index_us_stock_sina"], symbol=idx)
                    pct = PaperTradeReviewer._calc_pct_from_df(df)
                    if pct is not None:
                        return pct
                return None

        except Exception as e:
            logger.warning(f"Failed to fetch market index history for {market}: {e}")

        return None

    @staticmethod
    def _fetch_related_events(symbol: str, start_date: datetime.date) -> str:
        """从 market_events 拉取建仓后的相关新闻，优先定向新闻。"""
        symbol = str(symbol or "").strip()
        if not symbol:
            return "暂无相关专有事件记录。"

        try:
            events = get_symbol_news_events(symbol=symbol, start_date=start_date, max_items=24)
            if not events:
                return "暂无相关专有事件记录。"

            def _source_tag(src: str) -> str:
                src = str(src or "")
                if src.startswith("targeted_news"):
                    return "定向"
                if src == "monitor_scan":
                    return "异动"
                if src == "news_stream":
                    return "新闻"
                return "事件"

            events.sort(key=lambda x: (0 if str(x.get("source", "")).startswith("targeted_news") else 1, x.get("date")))
            events = list(reversed(events))

            lines: List[str] = []
            for e in events[:12]:
                d = str(e.get("date", ""))
                tag = _source_tag(str(e.get("source", "")))
                headline = str(e.get("headline", "")).strip() or str(e.get("document", "")).strip()[:88]
                lines.append(f"[{d}][{tag}] {headline}")

            return "\n".join(lines) if lines else "暂无相关专有事件记录。"
        except Exception as e:
            logger.error(f"Failed to fetch events for {symbol}: {e}")
            return "事件检索失败。"

    @staticmethod
    def generate_review(trade: PaperTrade) -> str:
        """
        核心方法：生成交易复盘短评（默认 100 字内）。
        """
        end_date = trade.exit_date or datetime.date.today()
        hold_days = (end_date - trade.entry_date).days or 1

        current_price = trade.exit_price
        if not current_price:
            p = PaperTradeReviewer._fetch_historical_kbars(trade.symbol, trade.market, end_date, end_date)
            if not p.empty:
                current_price = float(p.iloc[-1]["close"])
            else:
                current_price = trade.entry_price

        pnl_pct = trade.pnl_pct
        if pnl_pct is None:
            pnl_pct = round(((current_price - trade.entry_price) / trade.entry_price) * 100, 2)

        df = PaperTradeReviewer._fetch_historical_kbars(trade.symbol, trade.market, trade.entry_date, end_date)
        max_profit, max_drawdown = 0.0, 0.0
        if not df.empty:
            highest = float(df["high"].max())
            lowest = float(df["low"].min())
            max_profit = round(((highest - trade.entry_price) / trade.entry_price) * 100, 2)
            max_drawdown = round(((lowest - trade.entry_price) / trade.entry_price) * 100, 2)

        index_pct = PaperTradeReviewer._fetch_market_index(trade.market, trade.entry_date, end_date)
        index_text = f"{index_pct}%" if index_pct is not None else "指数缺失"
        events = PaperTradeReviewer._fetch_related_events(trade.symbol, trade.entry_date)
        news_meta = summarize_symbol_news(trade.symbol, lookback_days=max(3, min(30, hold_days)))
        news_signal = f"新闻强度:{news_meta.get('intensity_score', 0)} 新闻数:{news_meta.get('total', 0)}"

        max_chars = max(30, PaperTradeReviewer.REVIEW_MAX_CHARS)
        prompt = f"""你是交易教练。请基于以下数据输出【单段中文短评】，严格要求：
1) 总长度不超过{max_chars}字；
2) 仅1句话，不换行，不要Markdown；
3) 必须包含：评级(S/A/B/C/F) + 核心结论 + 1条动作建议；
4) 禁止客套话与解释过程。

交易快照：
标的:{trade.name}({trade.symbol}-{trade.market})
持仓:{hold_days}天 成本:{trade.entry_price} 现价:{current_price} 盈亏:{pnl_pct}%
最大浮盈:{max_profit}% 最大浮亏:{max_drawdown}% 大盘:{index_text}
建仓逻辑:{trade.entry_reason if trade.entry_reason else '无'}
新闻信号:{news_signal}
事件:{events}
"""

        try:
            logger.info(f"Generating compact review for {trade.symbol}...")
            raw = simple_prompt(prompt)
            return PaperTradeReviewer._compact_review(raw)
        except Exception as e:
            logger.error(f"AI review generation failed: {traceback.format_exc()}")
            return f"❌ 复盘分析生成失败: {e}"
