"""
Daily Rank Service - 每日榜单服务
"""
import logging
from datetime import date
from typing import List

from core.db import db_manager
from domain.ledger.analytics import DailyRank
from modules.ingestion.akshare_client import akshare_client
from sqlmodel import select

logger = logging.getLogger(__name__)

class DailyRankService:
    """
    负责每天拉取全市场的榜单并保存到数据库。
    """
    
    @staticmethod
    def sync_daily_ranks(markets: List[str] = None, rank_types: List[str] = None):
        """
        同步各个市场不同维度的榜单，并进行持久化。
        应配置为每天盘后执行一次。
        """
        if markets is None:
            markets = ["CN", "HK", "US"]
        if rank_types is None:
            rank_types = ["change_pct", "amount", "turnover"]
            
        today = date.today()
        
        with db_manager.ledger_session() as session:
            for market in markets:
                for rank_type in rank_types:
                    # 检查今天是否已经同步过
                    existing = session.exec(
                        select(DailyRank).where(
                            DailyRank.date == today,
                            DailyRank.market == market,
                            DailyRank.rank_type == rank_type
                        )
                    ).first()
                    
                    if existing:
                        logger.info(f"Daily rank already exists for {market} - {rank_type} on {today}, skipping.")
                        continue
                    
                    logger.info(f"Fetching daily rank for {market} - {rank_type}...")
                    
                    # 取 Top 10
                    df = akshare_client.get_daily_top_ranks(market=market, rank_type=rank_type, top_n=10)
                    if df.empty:
                        logger.warning(f"No rank data available for {market} - {rank_type}.")
                        continue
                        
                    ranks = []
                    for _, row in df.iterrows():
                        r = DailyRank(
                            date=today,
                            market=market,
                            rank_type=rank_type,
                            symbol=str(row["symbol"]),
                            name=str(row["name"]),
                            price=float(row["price"]),
                            change_pct=float(row["change_pct"]),
                            amount=float(row["amount"]),
                            turnover_rate=float(row["turnover_rate"]),
                        )
                        ranks.append(r)
                        
                    session.add_all(ranks)
                    logger.info(f"[OK] Synced {len(ranks)} records to DailyRank: {market} - {rank_type}")
                    
                    # 触发 AI 归因 (抽取 Top 3)
                    try:
                        from modules.monitor.scanner import MonitorService, analysis_executor
                        if ranks:
                            top_focus = sorted(ranks, key=lambda x: x.change_pct, reverse=True)[:3]
                            for focus_r in top_focus:
                                item_mock = {
                                    'symbol': focus_r.symbol,
                                    'name': focus_r.name,
                                    'chat_id': None,  # System level log
                                    'market': focus_r.market
                                }
                                quote_mock = {
                                    'pct_chg': focus_r.change_pct,
                                    'price': focus_r.price
                                }
                                direction = f"🏆 榜单登顶 ({focus_r.rank_type})"
                                # 放进分析器进行多线程深度挖掘
                                analysis_executor.submit(MonitorService._analyze_and_report, item_mock, quote_mock, direction)
                    except Exception as e:
                        logger.error(f"Failed to submit daily rank AI analysis: {e}")
                    
            # 提交事务
            session.commit()
            logger.info(f"All requested daily ranks synced successfully for {today}.")

# 全局实例
daily_rank_service = DailyRankService()
