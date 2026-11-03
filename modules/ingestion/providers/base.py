"""
Base Provider Interfaces for Unified Data Ingestion
"""
import abc
from typing import List, Dict, Any, Optional

class BaseProvider(abc.ABC):
    """
    基础 Provider 接口
    """
    
    @property
    @abc.abstractmethod
    def provider_name(self) -> str:
        """返回 Provider 名称"""
        pass
        
    @abc.abstractmethod
    def health_check(self) -> bool:
        """检查 Provider 是否可用 (例如 API Key 是否有效，网络是否通畅)"""
        pass


class BaseSearchProvider(BaseProvider):
    """
    搜素 Provider 基础接口
    """
    
    @abc.abstractmethod
    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        """
        执行搜索
        返回格式: [{"title": "...", "snippet": "...", "url": "...", "date": "..."}, ...]
        """
        pass


class BaseMarketProvider(BaseProvider):
    """
    行情/基本面 Provider 基础接口
    """
    
    @abc.abstractmethod
    def get_quote(self, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        """
        获取实时行情
        返回格式: {"price": 10.0, "change_pct": 2.5, "volume": 1000}
        """
        pass
        
    def get_historical_data(self, symbol: str, market: str, days: int = 30) -> Optional[Any]:
        """可选实现: 获取历史数据"""
        raise NotImplementedError("This provider does not support historical data retrieval.")


class BaseNewsProvider(BaseProvider):
    """
    新闻/资讯 Provider 基础接口
    """
    
    @abc.abstractmethod
    def get_latest_news(self, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        获取最新资讯
        """
        pass
