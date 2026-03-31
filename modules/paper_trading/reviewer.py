"""
Paper Trading Reviewer
模拟交易系统 - AI 复盘教练引擎
"""
import datetime
import logging
import traceback
from typing import List, Optional

import pandas as pd

from core.db import get_collection
from core.llm import simple_prompt
from domain.ledger.paper_trade import PaperTrade
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class PaperTradeReviewer:
    """提供深度的 AI 多维度复盘分析"""

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
                # US 指数接口历史稳定性较差，按优先级多路回退。
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
    def _is_related_symbol(meta_symbols, symbol: str) -> bool:
        if meta_symbols is None:
            return False
        symbol = str(symbol or "").upper()
        if isinstance(meta_symbols, list):
            return symbol in {str(x).upper() for x in meta_symbols}
        raw = str(meta_symbols).upper()
        parts = {x.strip() for x in raw.replace(";", ",").split(",") if x.strip()}
        return symbol in parts or symbol in raw

    @staticmethod
    def _fetch_related_events(symbol: str, start_date: datetime.date) -> str:
        """从 ChromaDB 中拉取建仓后的异动事件，支持严格+宽松双通道回退。"""
        symbol = str(symbol or "").strip()
        if not symbol:
            return "暂无相关专有事件记录。"

        try:
            collection = get_collection("market_events")
            docs: List[str] = []

            # 严格通道：where 精确匹配
            strict = collection.query(
                query_texts=[symbol],
                n_results=10,
                where={"related_symbols": symbol},
            )

            if strict and strict.get("documents"):
                for doc, meta in zip(strict["documents"][0], strict.get("metadatas", [[]])[0]):
                    evt_date = str((meta or {}).get("event_date", ""))
                    if evt_date >= str(start_date):
                        docs.append(f"[{evt_date}] {doc}")

            # 宽松回退：无 where 检索后在 metadata 层做包含过滤
            if not docs:
                relaxed = collection.query(query_texts=[symbol], n_results=20)
                if relaxed and relaxed.get("documents"):
                    for doc, meta in zip(relaxed["documents"][0], relaxed.get("metadatas", [[]])[0]):
                        meta = meta or {}
                        if not PaperTradeReviewer._is_related_symbol(meta.get("related_symbols"), symbol):
                            continue
                        evt_date = str(meta.get("event_date", ""))
                        if evt_date >= str(start_date):
                            docs.append(f"[{evt_date}] {doc}")

            if not docs:
                return "暂无相关专有事件记录。"

            return "\n".join(docs[:10])
        except Exception as e:
            logger.error(f"Failed to fetch ChromaDB events for {symbol}: {e}")
            return "事件检索失败。"

    @staticmethod
    def generate_review(trade: PaperTrade) -> str:
        """
        核心方法：拼装上下文片段并向 LLM 提问，生成 6 大维度的复盘研报
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
        index_text = f"{index_pct}%" if index_pct is not None else "未获取到有效指数数据"

        events = PaperTradeReviewer._fetch_related_events(trade.symbol, trade.entry_date)

        prompt = f"""你是一位铁面无私、目光如炬的顶尖对冲基金风控官与交易教练。
你的任务是对基金经理（用户）的一笔模拟交易进行深度的【六维度回测复盘研报】。

【交易快照】
- 标的: {trade.name} ({trade.symbol} - {trade.market})
- 持仓期: {trade.entry_date} 至 {end_date} (共 {hold_days} 天)
- 成本价: {trade.entry_price}
- 平仓/现价: {current_price}
- 最终盈亏: {pnl_pct}%
- 建仓初始逻辑: {trade.entry_reason if trade.entry_reason else "无"}
- 期间标的最大浮盈: {max_profit}%, 最大浮亏: {max_drawdown}%
- 同期大盘基准表现: {index_text}
- 期间底层新闻/异动:
{events}

【要求】
请直接输出一篇排版精美、使用 Markdown 语法的干货复盘研报，必须且且仅包含以下标题框架：
📈 交易策略复盘：(初始逻辑是否得到印证？这笔交易进出场是否科学？)
🌪️ 持仓波动分析：(是否经历了巨大的浮亏过山车？盈亏比如何？)
📊 标的基本面：(这期间是否有影响估值的核心财报/基本面转变？)
📰 关联事件与题材：(拉动或重挫股价的实质内核到底是什么？)
🧭 市场主线与风格：(有没有跑赢大盘？同期资金是拥挤在这个板块还是流向了别处？)
💡 AI 回测结论：(冷醒毒舌的综合评价。例如：S级神操作、B级及格但靠运气、F级盲目赌博。加上最核心的一句建议。)

只返回这6个标题的内容，拒绝任何客套话。
"""

        try:
            logger.info(f"Generating review for {trade.symbol}...")
            return simple_prompt(prompt)
        except Exception as e:
            logger.error(f"AI review generation failed: {traceback.format_exc()}")
            return f"❌ 复盘分析生成失败: {e}"
