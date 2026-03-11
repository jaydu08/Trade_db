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
        
        # 2. Save to Repository
        repo = WatchlistRepository()
        data = repo.load_all()
        
        # Key = symbol
        if symbol in data:
            return f"⚠️ {name} ({symbol}) 已经在监控列表中了。"
            
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
        
        repo.add_item(symbol, item)
        return f"✅ 已添加监控: {name} ({symbol}.{market})"

    @staticmethod
    def list_stocks() -> str:
        """列出所有监控"""
        repo = WatchlistRepository()
        data = repo.load_all()
        
        if not data:
            return "📭 监控列表为空。"
            
        msg = "📋 **当前监控列表**:\n"
        for symbol, item in data.items():
            msg += f"- {item['name']} ({item['symbol']}.{item['market']})\n"
        return msg

    @staticmethod
    def remove_stock(symbol: str) -> str:
        """移除监控"""
        repo = WatchlistRepository()
        data = repo.load_all()
        
        # Try exact match first
        if symbol in data:
            name = data[symbol]['name']
            repo.remove_item(symbol)
            return f"🗑️ 已移除: {name} ({symbol})"
            
        # Try searching by name if symbol not found
        for k, v in list(data.items()):
            if symbol in v['name'] or symbol.upper() == k.upper():
                name = v['name']
                repo.remove_item(k)
                return f"🗑️ 已移除: {name} ({k})"
                
        return f"❌ 未找到代码: {symbol}"
