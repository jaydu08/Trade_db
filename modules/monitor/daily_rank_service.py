"""
Daily Rank Service - 每日榜单服务
"""
import logging
import os
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
        inserted_by_market = {}
        expected_markets = []
        enable_top_alert = DailyRankService._env_flag("ENABLE_DAILY_RANK_TOP_ALERT", default=False)
        enable_zero_alert = DailyRankService._env_flag("ENABLE_DAILY_RANK_ZERO_ALERT", default=False)
        
        with db_manager.ledger_session() as session:
            bar_buffer = {}
            for market in markets:
                if not DailyRankService._should_sync_today(market):
                    logger.info(f"Skipping {market} daily rank sync: Not a valid reporting day (Weekend).")
                    continue
                expected_markets.append(market)
                inserted_by_market.setdefault(market, 0)
                    
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
                        # 仅保留正涨幅数据
                        if float(row["change_pct"]) <= 0:
                            continue
                        symbol = str(row["symbol"])
                        name = str(row["name"])
                        price = float(row["price"])
                        change_pct = float(row["change_pct"])
                        amount = float(row["amount"])
                        turnover_rate = float(row["turnover_rate"])
                        r = DailyRank(
                            date=today,
                            market=market,
                            rank_type=rank_type,
                            symbol=symbol,
                            name=name,
                            price=price,
                            change_pct=change_pct,
                            amount=amount,
                            turnover_rate=turnover_rate,
                        )
                        ranks.append(r)
                        inserted_by_market[market] = inserted_by_market.get(market, 0) + 1
                        key = (market, symbol)
                        if key not in bar_buffer or amount > bar_buffer[key]["amount"]:
                            bar_buffer[key] = {
                                "symbol": symbol,
                                "name": name,
                                "price": price,
                                "pct_chg": change_pct,
                                "amount": amount,
                                "turnover_rate": turnover_rate,
                            }
                        
                    session.add_all(ranks)
                    logger.info(f"[OK] 已写入 {len(ranks)} 条数据到每日榜单: {market} - {rank_type}")
                    
                    # 触发 AI 归因 (抽取 Top 3)
                    try:
                        from modules.monitor.scanner import MonitorService, analysis_executor
                        # 默认关闭：避免盘后榜单产生额外“Top标的预警”噪音
                        # 如需启用，设置 ENABLE_DAILY_RANK_TOP_ALERT=1
                        if enable_top_alert and rank_type == "change_pct" and ranks:
                            market_cn = {'CN': 'A股', 'HK': '港股', 'US': '美股'}.get(market, market)
                            top_focus = sorted(ranks, key=lambda x: x.change_pct, reverse=True)[:3]
                            for focus_r in top_focus:
                                item_mock = {
                                    'symbol': focus_r.symbol,
                                    'name': focus_r.name,
                                    'chat_id': None,
                                    'market': focus_r.market
                                }
                                quote_mock = {
                                    'pct_chg': focus_r.change_pct,
                                    'price': focus_r.price
                                }
                                direction = f"🏆 {market_cn}每日涨幅榜 Top标的"
                                analysis_executor.submit(MonitorService._analyze_and_report, item_mock, quote_mock, direction)
                    except Exception as e:
                        logger.error(f"每日榜单 AI 归因提交失败: {e}")
                    
            # 提交事务
            session.commit()
            logger.info(f"All requested daily ranks synced successfully for {today}.")

        # 写入趋势日线快照（与 DailyRank 解耦，避免单事务过重）
        if bar_buffer:
            try:
                from modules.monitor.trend_service import TrendService
                by_market = {}
                for (mkt, _), payload in bar_buffer.items():
                    by_market.setdefault(mkt, []).append(payload)
                for mkt, payloads in by_market.items():
                    TrendService.save_daily_bars(mkt, payloads, source="daily_rank")
            except Exception as e:
                logger.error(f"Failed to save TrendDailyBar from DailyRank: {e}")

        # 交易日应采集但全 0 时，主动告警
        zero_markets = [m for m in expected_markets if inserted_by_market.get(m, 0) == 0]
        if zero_markets:
            logger.error("DailyRank sync wrote zero rows on valid day: markets=%s", zero_markets)
            # 默认不向 UI/Telegram 广播内部采集异常，避免污染用户侧体验
            # 如需启用，设置 ENABLE_DAILY_RANK_ZERO_ALERT=1
            if enable_zero_alert:
                try:
                    from modules.monitor.notifier import Notifier
                    Notifier.broadcast("⚠️ DailyRank采集异常：交易日写入为0 -> " + ",".join(zero_markets))
                except Exception as e:
                    logger.warning("DailyRank zero-row alert failed: %s", e)
            else:
                logger.info("DailyRank zero-row broadcast disabled, markets=%s", zero_markets)

    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _should_sync_today(market: str) -> bool:
        """
        判断北京时间今天是否应该拉取该市场的榜单。
        CN/HK: 交易日为周一至周五，通常在北京时间傍晚拉取，所以北京时间的周六、周日跳过。
        US: 交易日为美东周一至周五，对应北京时间的周二至周六凌晨/早上拉取，所以北京时间的周日、周一跳过。
        """
        import datetime
        from zoneinfo import ZoneInfo
        
        now = datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
        weekday = now.weekday() # 0=Mon, ..., 5=Sat, 6=Sun
        
        if market in ['CN', 'HK']:
            if weekday >= 5: # Sat, Sun
                return False
        elif market == 'US':
            if weekday in [6, 0]: # Sun, Mon
                return False
                
        return True

# 全局实例
daily_rank_service = DailyRankService()
