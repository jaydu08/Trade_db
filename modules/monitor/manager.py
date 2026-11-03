import logging
from datetime import datetime
from modules.monitor.resolver import SymbolResolver
from modules.monitor.repository import WatchlistRepository

logger = logging.getLogger(__name__)

class MonitorManager:
    
    @staticmethod
    def add_stock(query: str, chat_id: int = None) -> str:
        """添加监控"""
        # 1. Resolve Symbol
        result = SymbolResolver.resolve(query)
        if not result:
            return f"❌ 无法识别股票: {query}。请尝试输入标准代码 (如 00700) 或全称。"
            
        symbol, market, name = result
        
        # 2. Save to Repository (atomic de-dup by symbol+market)
        repo = WatchlistRepository()
            
        item = {
            "symbol": symbol,
            "market": market,
            "name": name,
            "added_at": str(datetime.now()),
            "last_alert_at": None,
            "alert_threshold_pct": 5.0, # Default to 5.0% move
            "is_active": True,
            "chat_id": chat_id # Save chat_id
        }
        
        added, existing_key, _existing_item = repo.add_unique_by_symbol_market(item)
        if not added:
            return f"⚠️ {name} ({symbol}.{market}) 已经在监控列表中了。"

        logger.info(f"Added watchlist item key={existing_key} symbol={symbol} market={market}")
        return f"✅ 已添加监控: {name} ({symbol}.{market})"

    @staticmethod
    def list_stocks() -> str:
        """列出所有监控"""
        repo = WatchlistRepository()
        data = repo.load_all()
        
        if not data:
            return "📭 监控列表为空。"
            
        msg = "📋 **当前监控列表**:\n"
        # 稳定排序，便于比对与排障
        ordered_items = sorted(
            data.items(),
            key=lambda kv: (str(kv[1].get("market", "")), str(kv[1].get("symbol", "")))
        )
        for symbol, item in ordered_items:
            msg += f"- {item['name']} ({item['symbol']}.{item['market']})\n"
        return msg

    @staticmethod
    def remove_stock(symbol: str) -> str:
        """移除监控"""
        repo = WatchlistRepository()
        query = symbol.strip()

        # 歧义保护：同代码存在多市场时，要求用户显式 market:symbol
        matches = repo.find_matches(query)
        if len(matches) > 1:
            q_upper = query.upper()
            same_symbol = [
                (k, v) for k, v in matches if str(v.get("symbol", "")).upper() == q_upper
            ]
            if len(same_symbol) > 1:
                hints = ", ".join([f"{v.get('market', '')}:{v.get('symbol', '')}" for _, v in same_symbol])
                return f"⚠️ 命中多个市场标的，请使用更精确格式删除：{hints}"

        removed, key, item = repo.remove_first_match(query)
        if removed and item:
            name = item.get("name", "未知标的")
            sym = item.get("symbol", symbol)
            mkt = item.get("market", "")
            suffix = f".{mkt}" if mkt else ""
            logger.info(f"Removed watchlist item key={key}")
            return f"🗑️ 已移除: {name} ({sym}{suffix})"

        return f"❌ 未找到代码: {symbol}"
