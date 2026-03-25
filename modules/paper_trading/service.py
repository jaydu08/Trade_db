"""
Paper Trading Service
模拟交易核心业务逻辑：建仓、平仓、查询、定期到期检查
"""
import logging
import datetime
from typing import List, Optional, Tuple

from sqlmodel import select

from core.db import db_manager
from domain.ledger.paper_trade import PaperTrade
from modules.monitor.resolver import SymbolResolver
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class PaperTradingService:
    """模拟交易服务类"""

    @staticmethod
    def _fetch_current_price(symbol: str, market: str) -> Optional[float]:
        """拉取最新价格"""
        try:
            quote = akshare_client.get_realtime_quote_eastmoney(symbol, market)
            if quote and quote.get("price"):
                return float(quote["price"])
        except Exception as e:
            logger.error(f"Failed to fetch current price for {symbol}: {e}")
        return None

    @staticmethod
    def open_position(query: str, chat_id: int, target_days: Optional[int] = None, reason: str = "") -> Tuple[bool, str, Optional[PaperTrade]]:
        """
        发起一笔模拟交易 (建仓)
        :param query: 用户输入的标的名称或代码
        :param chat_id: Telegram 会话 ID (区分用户)
        :param target_days: 可选的目标持仓天数
        :param reason: 建仓逻辑
        :return: (is_success, message, trade)
        """
        # 解析标的
        resolved = SymbolResolver.resolve(query)
        if not resolved:
            return False, f"未识别到标的: {query}，请使用规范的代码或全称。", None
            
        symbol, market, name = resolved

        # 获取现价
        price = PaperTradingService._fetch_current_price(symbol, market)
        if not price or price <= 0:
            return False, f"获取 {name}({symbol}) 最新价格失败，可能未开盘或接口异常。", None

        # 检查是否已有一笔正在进行的模拟交易
        with db_manager.ledger_session() as session:
            stmt = select(PaperTrade).where(
                PaperTrade.symbol == symbol,
                PaperTrade.chat_id == chat_id,
                PaperTrade.status == "ACTIVE"
            )
            existing = session.exec(stmt).first()
            if existing:
                return False, f"您已经持仓 {name}({symbol}) 了，请先 /sell 或 /review 结束后再开新仓。", existing

            today = datetime.date.today()
            trade = PaperTrade(
                symbol=symbol,
                name=name,
                market=market,
                entry_date=today,
                entry_price=price,
                entry_reason=reason,
                target_days=target_days,
                status="ACTIVE",
                chat_id=chat_id
            )
            session.add(trade)
            session.commit()
            session.refresh(trade)
            
            return True, f"✅ 成功建仓 {name}({symbol})，成本：{price}。", trade

    @staticmethod
    def close_position(query: str, chat_id: int) -> Tuple[bool, str, Optional[PaperTrade]]:
        """
        平仓/结束一笔模拟交易
        :return: (is_success, message, trade)
        """
        resolved = SymbolResolver.resolve(query)
        if not resolved:
            return False, f"未识别到标的: {query}", None
            
        symbol, market, name = resolved

        with db_manager.ledger_session() as session:
            stmt = select(PaperTrade).where(
                PaperTrade.symbol == symbol,
                PaperTrade.chat_id == chat_id,
                PaperTrade.status == "ACTIVE"
            )
            trade = session.exec(stmt).first()
            if not trade:
                return False, f"暂未找到 {name}({symbol}) 的进行中模拟持仓记录。", None

            # 获取现价用于平仓
            price = PaperTradingService._fetch_current_price(symbol, trade.market)
            if not price or price <= 0:
                # 若无法获取，用上次收盘价或拒绝平仓
                return False, f"无法获取 {name}({symbol}) 的当前价格，平仓失败。", trade

            trade.exit_date = datetime.date.today()
            trade.exit_price = price
            trade.pnl_pct = round(((price - trade.entry_price) / trade.entry_price) * 100, 2)
            trade.status = "CLOSED"
            trade.updated_at = datetime.datetime.utcnow()

            session.add(trade)
            session.commit()
            session.refresh(trade)

            return True, f"✅ 成功平仓 {name}({symbol})，最终盈亏：{trade.pnl_pct}%", trade

    @staticmethod
    def get_active_trades(chat_id: int) -> List[PaperTrade]:
        """获取用户当前所有的有效持仓"""
        with db_manager.ledger_session() as session:
            stmt = select(PaperTrade).where(
                PaperTrade.chat_id == chat_id,
                PaperTrade.status == "ACTIVE"
            ).order_by(PaperTrade.entry_date.desc())
            return list(session.exec(stmt).all())
    
    @staticmethod
    def check_expired_trades() -> List[PaperTrade]:
        """
        后台轮询：检查是否有到了 target_days 自动平仓期的模拟交易。
        该方法由 APScheduler 每天盘后调用。
        """
        expired_trades = []
        today = datetime.date.today()
        
        with db_manager.ledger_session() as session:
            stmt = select(PaperTrade).where(
                PaperTrade.status == "ACTIVE",
                PaperTrade.target_days.is_not(None)
            )
            active_trades = session.exec(stmt).all()
            
            for trade in active_trades:
                expected_exit_date = trade.entry_date + datetime.timedelta(days=trade.target_days)
                if today >= expected_exit_date:
                    # 获取现价并平仓
                    price = PaperTradingService._fetch_current_price(trade.symbol, trade.market)
                    if price and price > 0:
                        trade.exit_date = today
                        trade.exit_price = price
                        trade.pnl_pct = round(((price - trade.entry_price) / trade.entry_price) * 100, 2)
                        trade.status = "CLOSED"
                        trade.updated_at = datetime.datetime.utcnow()
                        session.add(trade)
                        expired_trades.append(trade)
            
            if expired_trades:
                session.commit()
                # 刷新避免 detached
                for t in expired_trades:
                    session.refresh(t)
                    
        return expired_trades
