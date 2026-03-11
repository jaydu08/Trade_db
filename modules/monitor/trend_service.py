import logging
import datetime as dt
from sqlmodel import Session, select
from typing import List, Dict

from core.db import get_ledger_session
from domain.ledger.analytics import TrendSeedPool

logger = logging.getLogger(__name__)

class TrendService:
    @staticmethod
    def add_to_pool(market: str, items: List[Dict]):
        """
        向滚动种子池添加记录
        items: [{"symbol": "...", "name": "...", "reason": "..."}]
        """
        today = dt.date.today()
        
        try:
            with get_ledger_session() as session:
                for item in items:
                    sym = item.get("symbol")
                    if not sym:
                        continue
                        
                    # 检查今天是否已经入库，避免重复
                    stmt = select(TrendSeedPool).where(
                        TrendSeedPool.date == today,
                        TrendSeedPool.market == market,
                        TrendSeedPool.symbol == sym
                    )
                    existing = session.exec(stmt).first()
                    
                    if not existing:
                        seed = TrendSeedPool(
                            date=today,
                            market=market,
                            symbol=sym,
                            name=item.get("name", ""),
                            daily_reason=item.get("reason", "")
                        )
                        session.add(seed)
                # 自动清理 30 天前的数据
                cutoff = today - dt.timedelta(days=30)
                del_stmt = select(TrendSeedPool).where(TrendSeedPool.date < cutoff)
                old_records = session.exec(del_stmt).all()
                for old in old_records:
                    session.delete(old)
                
                # context manager 将在退出时自动 commit
            logger.info(f"TrendSeedPool: Saved {len(items)} items for {market}")
        except Exception as e:
            logger.error(f"Failed to add to TrendSeedPool: {e}")

class TrendCalculator:
    @staticmethod
    def _get_return(symbol: str, market: str, d_days: int) -> float:
        """获取标的 N 天前的真实累计涨幅 (基于日K线)"""
        import akshare as ak
        import pandas as pd
        import time
        from datetime import datetime, timedelta
        
        try:
            df = None
            if market == "CN":
                # CN 接口通常需要加 sh/sz 或直接用6位代码
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily")
            elif market == "US":
                # US 接口接受 AAPL
                df = ak.stock_us_daily(symbol=symbol)
            elif market == "HK":
                # HK 接口接受 00700
                df = ak.stock_hk_daily(symbol=symbol)
            elif market == "CF":
                # CF 期货历史数据
                df = ak.futures_zh_daily_sina(symbol=symbol)
                
            if df is None or df.empty:
                return 0.0

            # 字段归一化处理
            date_col = "date" if "date" in df.columns else "日期"
            close_col = "close" if "close" in df.columns else "收盘"
            
            if date_col not in df.columns or close_col not in df.columns:
                return 0.0
                
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.sort_values(by=date_col).reset_index(drop=True)
            
            # 最近的收盘价
            current_close = float(df.iloc[-1][close_col])
            current_date = df.iloc[-1][date_col]
            
            # N 天前的实际交易日 (往回找 d_days 个自然日左右的最近交易日)
            target_date = current_date - timedelta(days=d_days)
            # 找到小于等于 target_date 的最后一条记录
            past_df = df[df[date_col] <= target_date]
            if past_df.empty:
                # 获取不到足够长的数据，用第一条
                past_close = float(df.iloc[0][close_col])
            else:
                past_close = float(past_df.iloc[-1][close_col])
                
            if past_close <= 0:
                return 0.0
                
            return round((current_close - past_close) / past_close * 100, 2)
        except Exception as e:
            logger.debug(f"Calculate return failed for {symbol} ({market}): {e}")
            return 0.0

    @staticmethod
    def calculate_trend(days: int = 7) -> Dict[str, List[Dict]]:
        """
        计算各市场趋势榜单
        """
        from collections import defaultdict
        import concurrent.futures
        
        cutoff = dt.date.today() - dt.timedelta(days=days)
        
        # 1. 查询种子池
        with get_ledger_session() as session:
            stmt = select(TrendSeedPool).where(TrendSeedPool.date >= cutoff)
            records = session.exec(stmt).all()
            
        if not records:
            return {}
            
        # 2. 按市场和代码去重合并理由
        grouped = defaultdict(dict)
        for r in records:
            key = (r.market, r.symbol)
            if key not in grouped:
                grouped[key] = {
                    "market": r.market,
                    "symbol": r.symbol,
                    "name": r.name,
                    "reasons": set()
                }
            if r.daily_reason:
                grouped[key]["reasons"].add(r.daily_reason.strip())
                
        items = list(grouped.values())
        
        # 3. 并发查 N 日涨幅
        def _process(item: dict):
            ret = TrendCalculator._get_return(item["symbol"], item["market"], days)
            item["return_pct"] = ret
            # 将多日的理由合并成一条主线索，供下游 LLM 做宏大叙事归纳
            lines = [r for r in item["reasons"] if r and "分析原因失败" not in r]
            item["aggregated_reason"] = " | ".join(lines) if lines else "暂无新闻催化"
            return item
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(_process, items))
            
        # 4. 按市场分组排序并取 Top 10
        market_tops = defaultdict(list)
        for res in results:
            market_tops[res["market"]].append(res)
            
        final_tops = {}
        for mkt, stks in market_tops.items():
            stks.sort(key=lambda x: x["return_pct"], reverse=True)
            # 取出前10名
            final_tops[mkt] = stks[:10]
            
        return dict(final_tops)
