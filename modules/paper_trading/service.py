"""
Paper Trading Service
模拟交易核心业务逻辑：建仓、平仓、查询、定期到期检查
"""
import logging
import datetime
from typing import Dict, List, Optional, Tuple

from sqlmodel import select

from core.db import db_manager
from domain.ledger.paper_trade import PaperTrade
from modules.monitor.resolver import SymbolResolver
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class PaperTradingService:
    """模拟交易服务类"""

    REVIEW_PENDING = "PENDING"
    REVIEW_DONE = "DONE"
    REVIEW_FAILED = "FAILED"

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
    def mark_review_pending(trade_id: int, source: str = "manual") -> None:
        """将复盘状态置为待处理，并记录触发来源。"""
        with db_manager.ledger_session() as session:
            trade = session.get(PaperTrade, trade_id)
            if not trade:
                return
            trade.review_status = PaperTradingService.REVIEW_PENDING
            trade.review_error = None
            trade.review_source = source
            trade.updated_at = datetime.datetime.utcnow()
            session.add(trade)

    @staticmethod
    def save_review_success(trade_id: int, review_text: str, source: str = "manual") -> Optional[PaperTrade]:
        """保存复盘成功结果。"""
        now = datetime.datetime.utcnow()
        with db_manager.ledger_session() as session:
            trade = session.get(PaperTrade, trade_id)
            if not trade:
                return None

            trade.review_text = review_text
            trade.review_status = PaperTradingService.REVIEW_DONE
            trade.review_attempts = int(trade.review_attempts or 0) + 1
            trade.review_error = None
            trade.review_source = source
            trade.last_reviewed_at = now
            trade.updated_at = now

            session.add(trade)
            session.commit()
            session.refresh(trade)
            return trade

    @staticmethod
    def save_review_failure(trade_id: int, error: str, source: str = "manual") -> Optional[PaperTrade]:
        """保存复盘失败状态，不抛出异常，保证主流程不中断。"""
        now = datetime.datetime.utcnow()
        with db_manager.ledger_session() as session:
            trade = session.get(PaperTrade, trade_id)
            if not trade:
                return None

            trade.review_status = PaperTradingService.REVIEW_FAILED
            trade.review_attempts = int(trade.review_attempts or 0) + 1
            trade.review_error = (error or "")[:1000]
            trade.review_source = source
            trade.last_reviewed_at = now
            trade.updated_at = now

            session.add(trade)
            session.commit()
            session.refresh(trade)
            return trade

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
        resolved = SymbolResolver.resolve(query)
        if not resolved:
            return False, f"未识别到标的: {query}，请使用规范的代码或全称。", None

        symbol, market, name = resolved

        price = PaperTradingService._fetch_current_price(symbol, market)
        if not price or price <= 0:
            return False, f"获取 {name}({symbol}) 最新价格失败，可能未开盘或接口异常。", None

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
                review_status=PaperTradingService.REVIEW_PENDING,
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

        symbol, _, name = resolved

        with db_manager.ledger_session() as session:
            stmt = select(PaperTrade).where(
                PaperTrade.symbol == symbol,
                PaperTrade.chat_id == chat_id,
                PaperTrade.status == "ACTIVE"
            )
            trade = session.exec(stmt).first()
            if not trade:
                return False, f"暂未找到 {name}({symbol}) 的进行中模拟持仓记录。", None

            price = PaperTradingService._fetch_current_price(symbol, trade.market)
            if not price or price <= 0:
                return False, f"无法获取 {name}({symbol}) 的当前价格，平仓失败。", trade

            trade.exit_date = datetime.date.today()
            trade.exit_price = price
            trade.pnl_pct = round(((price - trade.entry_price) / trade.entry_price) * 100, 2)
            trade.status = "CLOSED"
            trade.review_status = PaperTradingService.REVIEW_PENDING
            trade.review_error = None
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
    def _trade_payload(trade: PaperTrade, include_review: bool = True) -> Dict:
        """Serialize a PaperTrade row for web/API responses."""
        end_date = trade.exit_date or datetime.date.today()
        hold_days = (end_date - trade.entry_date).days if trade.entry_date else 0
        payload = {
            "id": trade.id,
            "symbol": trade.symbol,
            "name": trade.name,
            "market": trade.market,
            "entry_price": trade.entry_price,
            "entry_date": str(trade.entry_date) if trade.entry_date else "",
            "target_days": trade.target_days,
            "entry_reason": trade.entry_reason or "",
            "status": trade.status,
            "exit_price": trade.exit_price,
            "exit_date": str(trade.exit_date) if trade.exit_date else "",
            "hold_days": hold_days,
            "days_held": hold_days,
            "pnl_pct": trade.pnl_pct,
            "review_status": trade.review_status or PaperTradingService.REVIEW_PENDING,
            "review_attempts": int(trade.review_attempts or 0),
            "review_source": trade.review_source or "",
            "last_reviewed_at": str(trade.last_reviewed_at) if trade.last_reviewed_at else "",
        }
        if include_review:
            payload.update({
                "review_text": trade.review_text or "",
                "review_error": trade.review_error or "",
            })
        return payload

    @staticmethod
    def get_trade_history(
        chat_id: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
        market: str = "",
        symbol: str = "",
        review_status: str = "",
        sort: str = "exit_date_desc",
    ) -> Dict:
        """获取 CLOSED 模拟交易历史，复用 papertrade 表，避免重复历史表。"""
        page = max(1, int(page or 1))
        page_size = max(1, min(100, int(page_size or 20)))
        market = str(market or "").strip().upper()
        symbol_q = str(symbol or "").strip().upper()
        review_status = str(review_status or "").strip().upper()

        with db_manager.ledger_session() as session:
            stmt = select(PaperTrade).where(PaperTrade.status == "CLOSED")
            if chat_id is not None:
                stmt = stmt.where(PaperTrade.chat_id == chat_id)
            rows = list(session.exec(stmt).all())

        if market:
            rows = [t for t in rows if str(t.market or "").upper() == market]
        if symbol_q:
            rows = [
                t for t in rows
                if symbol_q in str(t.symbol or "").upper()
                or symbol_q in str(t.name or "").upper()
            ]
        if review_status:
            rows = [t for t in rows if str(t.review_status or "").upper() == review_status]

        def _hold_days(t: PaperTrade) -> int:
            end_date = t.exit_date or datetime.date.today()
            return (end_date - t.entry_date).days if t.entry_date else 0

        total = len(rows)
        pnl_values = [float(t.pnl_pct) for t in rows if t.pnl_pct is not None]
        hold_values = [_hold_days(t) for t in rows]
        wins = len([v for v in pnl_values if v > 0])
        losses = len([v for v in pnl_values if v < 0])
        win_rate = round(wins / len(pnl_values) * 100, 2) if pnl_values else 0.0
        avg_pnl = round(sum(pnl_values) / len(pnl_values), 2) if pnl_values else 0.0
        avg_hold_days = round(sum(hold_values) / len(hold_values), 1) if hold_values else 0.0
        best = max(rows, key=lambda t: float(t.pnl_pct if t.pnl_pct is not None else -10**9), default=None)
        worst = min(rows, key=lambda t: float(t.pnl_pct if t.pnl_pct is not None else 10**9), default=None)

        def _sort_key(t: PaperTrade):
            if sort.startswith("pnl"):
                return float(t.pnl_pct if t.pnl_pct is not None else -10**9)
            if sort.startswith("hold_days"):
                return _hold_days(t)
            if sort.startswith("entry_date"):
                return t.entry_date or datetime.date.min
            return t.exit_date or t.updated_at.date() if getattr(t.updated_at, "date", None) else datetime.date.min

        reverse = not sort.endswith("_asc")
        rows.sort(key=_sort_key, reverse=reverse)
        start = (page - 1) * page_size
        page_rows = rows[start:start + page_size]

        return {
            "items": [PaperTradingService._trade_payload(t, include_review=True) for t in page_rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "summary": {
                "total": total,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "avg_pnl": avg_pnl,
                "avg_hold_days": avg_hold_days,
                "best_trade": PaperTradingService._trade_payload(best, include_review=False) if best else None,
                "worst_trade": PaperTradingService._trade_payload(worst, include_review=False) if worst else None,
            },
        }

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
                    price = PaperTradingService._fetch_current_price(trade.symbol, trade.market)
                    if price and price > 0:
                        trade.exit_date = today
                        trade.exit_price = price
                        trade.pnl_pct = round(((price - trade.entry_price) / trade.entry_price) * 100, 2)
                        trade.status = "CLOSED"
                        trade.review_status = PaperTradingService.REVIEW_PENDING
                        trade.review_error = None
                        trade.updated_at = datetime.datetime.utcnow()
                        session.add(trade)
                        expired_trades.append(trade)

            if expired_trades:
                session.commit()
                for t in expired_trades:
                    session.refresh(t)

        return expired_trades
