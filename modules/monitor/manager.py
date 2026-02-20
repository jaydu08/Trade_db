import logging
import json
import os
from datetime import datetime
from pathlib import Path
from modules.monitor.resolver import SymbolResolver

logger = logging.getLogger(__name__)

# 数据文件路径
DATA_DIR = Path("/root/Trade_db/data")
WATCHLIST_FILE = DATA_DIR / "watchlist.json"

class MonitorManager:
    
    @staticmethod
    def _load_data() -> dict:
        """加载监控列表"""
        if not WATCHLIST_FILE.exists():
            return {}
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load watchlist: {e}")
            return {}

    @staticmethod
    def _save_data(data: dict):
        """保存监控列表"""
        try:
            with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save watchlist: {e}")

    @staticmethod
    def add_stock(query: str, chat_id: int = None) -> str:
        """添加监控"""
        # 1. Resolve Symbol
        result = SymbolResolver.resolve(query)
        if not result:
            return f"❌ 无法识别股票: {query}。请尝试输入标准代码 (如 00700) 或全称。"
            
        symbol, market, name = result
        
        # 2. Save to JSON
        data = MonitorManager._load_data()
        
        # Key = symbol
        if symbol in data:
            return f"⚠️ {name} ({symbol}) 已经在监控列表中了。"
            
        data[symbol] = {
            "symbol": symbol,
            "market": market,
            "name": name,
            "added_at": str(datetime.now()),
            "last_alert_at": None,
            "alert_threshold_pct": 0.1, # DEBUG: Low threshold for testing
            "is_active": True,
            "chat_id": chat_id # Save chat_id
        }
        
        MonitorManager._save_data(data)
        return f"✅ 已添加监控: {name} ({symbol}.{market})"

    @staticmethod
    def list_stocks() -> str:
        """列出所有监控"""
        data = MonitorManager._load_data()
        if not data:
            return "📭 监控列表为空。"
            
        msg = "📋 **当前监控列表**:\n"
        for symbol, item in data.items():
            msg += f"- {item['name']} ({item['symbol']}.{item['market']})\n"
        return msg

    @staticmethod
    def remove_stock(symbol: str) -> str:
        """移除监控"""
        data = MonitorManager._load_data()
        
        # Try exact match first
        if symbol in data:
            name = data[symbol]['name']
            del data[symbol]
            MonitorManager._save_data(data)
            return f"🗑️ 已移除: {name} ({symbol})"
            
        # Try searching by name if symbol not found
        for k, v in list(data.items()):
            if symbol in v['name'] or symbol.upper() == k.upper():
                del data[k]
                MonitorManager._save_data(data)
                return f"🗑️ 已移除: {v['name']} ({k})"
                
        return f"❌ 未找到代码: {symbol}"