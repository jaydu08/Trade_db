"""
Performance Report Service
负责计算和定期推送7日/30日标的表现战报。
通过查询本地数据库中曾经被系统推送（WatchlistAlert / DailyRank）的标的，获取其最新现价，并计算区间累计涨幅。
仅推送正收益标的。
"""
import logging
import datetime
import asyncio
from typing import Dict, List, Tuple
from sqlmodel import select

from core.db import db_manager
from domain.ledger.analytics import WatchlistAlert, DailyRank
from modules.ingestion.akshare_client import akshare_client, AkShareClient
from modules.monitor.notifier import Notifier

logger = logging.getLogger(__name__)

class PerformanceReportService:
    @staticmethod
    def _get_pushed_stocks(days: int) -> Dict[str, Dict]:
        """
        获取过去 days 天内系统推送过的标的。
        返回字典: {symbol: {'name': name, 'market': market, 'push_price': price, 'push_date': date}}
        如果同一标的被推送多次，保留最早一次的推送价格和时间作为基准。
        """
        target_datetime = datetime.datetime.now() - datetime.timedelta(days=days)
        target_date = target_datetime.date()
        
        stocks = {}
        
        with db_manager.ledger_session() as session:
            # 1. 查询 WatchlistAlert
            alerts = session.exec(
                select(WatchlistAlert).where(WatchlistAlert.timestamp >= target_datetime)
            ).all()
            for a in alerts:
                sym = a.symbol
                if sym not in stocks or a.timestamp < stocks[sym]['push_date']:
                    stocks[sym] = {
                        'name': a.name,
                        'market': a.market,
                        'push_price': a.price,
                        'push_date': a.timestamp
                    }
            
            # 2. 查询 DailyRank (只计入涨跌幅榜的推送标的)
            ranks = session.exec(
                select(DailyRank).where(
                    DailyRank.date >= target_date,
                    DailyRank.rank_type == 'change_pct'
                )
            ).all()
            for r in ranks:
                sym = r.symbol
                # 转换 date 为 datetime 方便比较
                dt_val = datetime.datetime.combine(r.date, datetime.time())
                if sym not in stocks or dt_val < stocks[sym]['push_date']:
                    stocks[sym] = {
                        'name': r.name,
                        'market': r.market,
                        'push_price': r.price,
                        'push_date': dt_val
                    }
                    
        return stocks

    @staticmethod
    def _fetch_current_prices(symbols_by_market: Dict[str, List[str]]) -> Dict[str, float]:
        """按市场批量获取最新价格"""
        prices = {}
        
        # CN 市场
        if symbols_by_market.get('CN'):
            try:
                df_cn = AkShareClient._fetch_bulk_sina('CN')
                for _, row in df_cn.iterrows():
                    sym = str(row['代码'])
                    if sym in symbols_by_market['CN']:
                        prices[sym] = float(row.get('最新价', 0))
            except Exception as e:
                logger.error(f"Failed to fetch CN prices for report: {e}")
                
        # HK 市场
        if symbols_by_market.get('HK'):
            try:
                df_hk = akshare_client.get_stock_info_hk()
                for _, row in df_hk.iterrows():
                    sym = str(row['代码'])
                    if sym in symbols_by_market['HK']:
                        prices[sym] = float(row.get('最新价', 0))
            except Exception as e:
                logger.error(f"Failed to fetch HK prices for report: {e}")
                
        # US 市场
        if symbols_by_market.get('US'):
            try:
                df_us = akshare_client.get_stock_info_us()
                for _, row in df_us.iterrows():
                    sym = str(row['代码'])
                    if sym in symbols_by_market['US']:
                        prices[sym] = float(row.get('最新价', 0))
            except Exception as e:
                logger.error(f"Failed to fetch US prices for report: {e}")
                
        return prices

    @staticmethod
    def generate_and_push_report(days: int):
        """生成并推送战报"""
        logger.info(f"Generating {days}-day performance report...")
        
        try:
            stocks = PerformanceReportService._get_pushed_stocks(days)
            if not stocks:
                logger.info(f"No pushed stocks found in the last {days} days. Skip report.")
                return
                
            # 按市场分类
            symbols_by_market = {'CN': [], 'HK': [], 'US': []}
            for sym, info in stocks.items():
                mkt = info['market']
                if mkt in symbols_by_market:
                    symbols_by_market[mkt].append(sym)
                    
            # 获取最新价
            current_prices = PerformanceReportService._fetch_current_prices(symbols_by_market)
            
            # 计算收益率
            results_by_market = {'CN': [], 'HK': [], 'US': []}
            for sym, info in stocks.items():
                curr_price = current_prices.get(sym, 0)
                push_price = info.get('push_price', 0)
                
                if curr_price > 0 and push_price > 0:
                    pct_gain = (curr_price - push_price) / push_price * 100
                    # 仅保留正收益且剔除疑似数据的超离谱翻倍(如合股/拆股导致的价格突变)
                    if 0 < pct_gain < 500:
                        mkt = info['market']
                        results_by_market[mkt].append({
                            'symbol': sym,
                            'name': info['name'],
                            'push_price': push_price,
                            'curr_price': curr_price,
                            'pct_gain': pct_gain
                        })
                        
            # 排序
            for mkt in results_by_market:
                results_by_market[mkt].sort(key=lambda x: x['pct_gain'], reverse=True)
                
            # 构建消息
            # 如果全市场都没有正收益，静默
            if all(len(v) == 0 for v in results_by_market.values()):
                logger.info(f"No positive gains in the last {days} days. Skip report.")
                return
                
            end_date = datetime.date.today()
            start_date = end_date - datetime.timedelta(days=days)
            
            lines = [
                f"📊 **TradeDB {days}日 标的监控跟踪战报**",
                f"🗓 周期: {start_date} 至 {end_date}\n",
                f"以下为本系统过去 {days} 天挖掘并推送过的强表现标的："
            ]
            
            market_names = {'CN': '🇨🇳 A股', 'HK': '🇭🇰 港股', 'US': '🇺🇸 美股'}
            
            for mkt in ['CN', 'HK', 'US']:
                top_gainers = results_by_market[mkt][:10] # 仅列出Top 10防刷屏
                if top_gainers:
                    lines.append(f"\n{market_names[mkt]}")
                    for i, g in enumerate(top_gainers, 1):
                        lines.append(
                            f"{i}. {g['name']} ({g['symbol']}) | "
                            f"推荐价: {g['push_price']:.2f} -> 现价: {g['curr_price']:.2f} | "
                            f"累计涨幅: `+{g['pct_gain']:.2f}%`"
                        )
                        
            lines.append("\n*注：仅展示录得正向收益的个股，多次推送取首推价格。*")
            
            report = "\n".join(lines)
            Notifier.broadcast(report)
            logger.info(f"Successfully broadcasted {days}-day performance report.")
            
        except Exception as e:
            logger.error(f"Failed to generate performance report: {e}", exc_info=True)
