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
        获取过去 days 天内热榜推送过的标的。
        仅使用 DailyRank 表（盘后计算的正确收盘价）作为基准价。
        WatchlistAlert 不纳入（它在非市场时间也会触发，价格不可靠）。
        """
        target_date = (datetime.datetime.now() - datetime.timedelta(days=days)).date()
        stocks = {}

        from sqlmodel import Session
        engine = db_manager.ledger_engine
        with Session(engine) as session:
            ranks = session.exec(
                select(DailyRank).where(
                    DailyRank.date >= target_date,
                    DailyRank.rank_type == 'change_pct'
                )
            ).all()
            rank_data = [(r.symbol, r.name, r.market, r.price, r.date) for r in ranks]

        for sym, name, market, price, date in rank_data:
            if not sym or not price or price <= 0:
                continue
            dt_val = datetime.datetime.combine(date, datetime.time())
            if sym not in stocks or dt_val < stocks[sym]['push_date']:
                stocks[sym] = {
                    'name': name,
                    'market': market,
                    'push_price': price,
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
                
        # HK 市场（统一代码格式：'700'/'00700' → 5位含前导零匹配）
        if symbols_by_market.get('HK'):
            try:
                df_hk = akshare_client.get_stock_info_hk()
                hk_lookup = {}
                for _, row in df_hk.iterrows():
                    raw = str(row['代码'])
                    normalized = raw.lstrip('0').zfill(5)
                    try:
                        hk_lookup[normalized] = float(row.get('最新价', 0))
                    except (ValueError, TypeError):
                        pass
                for sym in symbols_by_market['HK']:
                    key = sym.lstrip('0').zfill(5)
                    if key in hk_lookup and hk_lookup[key] > 0:
                        prices[sym] = hk_lookup[key]
            except Exception as e:
                logger.error(f"Failed to fetch HK prices for report: {e}")

        # US 市场（Sina格式 '105.NVDA' → 取 ticker 部分匹配 DB 中的 'NVDA'）
        if symbols_by_market.get('US'):
            try:
                df_us = AkShareClient._fetch_bulk_sina('US')
                for _, row in df_us.iterrows():
                    raw_sym = str(row.get('代码', ''))
                    ticker = raw_sym.split('.')[-1]
                    if ticker in symbols_by_market['US']:
                        try:
                            prices[ticker] = float(row.get('最新价', 0))
                        except (ValueError, TypeError):
                            pass
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
