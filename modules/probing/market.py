"""
Market Prober - 实时行情探测器

封装 AkShare 行情接口，提供：
1. 实时行情获取（带缓存）
2. 涨停/跌停/停牌检测
3. 流动性过滤
"""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

import pandas as pd

from core.cache import cached
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class MarketProber:
    """
    市场行情探测器
    
    提供实时行情查询和过滤功能
    """
    
    def __init__(self):
        self._quote_cache: Dict[str, Dict] = {}
    
    def get_realtime_quote(self, symbol: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        获取单个股票实时行情
        
        Args:
            symbol: 股票代码
            use_cache: 是否使用缓存（60秒）
        
        Returns:
            行情字典，包含价格、涨跌幅、成交量等
        """
        try:
            if use_cache and symbol in self._quote_cache:
                cached_data = self._quote_cache[symbol]
                # 检查缓存是否过期（60秒）
                if (datetime.now() - cached_data["timestamp"]).seconds < 60:
                    return cached_data["data"]
            
            # 获取实时行情
            quote = akshare_client.get_stock_quote(symbol)
            
            if not quote:
                logger.warning(f"No quote data for {symbol}")
                return None
            
            # 标准化字段
            result = {
                "symbol": symbol,
                "name": quote.get("名称", ""),
                "price": float(quote.get("最新价", 0)),
                "change_pct": float(quote.get("涨跌幅", 0)),
                "change_amount": float(quote.get("涨跌额", 0)),
                "volume": float(quote.get("成交量", 0)),
                "amount": float(quote.get("成交额", 0)),
                "turnover_rate": float(quote.get("换手率", 0)),
                "high": float(quote.get("最高", 0)),
                "low": float(quote.get("最低", 0)),
                "open": float(quote.get("今开", 0)),
                "prev_close": float(quote.get("昨收", 0)),
                "timestamp": datetime.now(),
            }
            
            # 缓存
            if use_cache:
                self._quote_cache[symbol] = {
                    "data": result,
                    "timestamp": datetime.now(),
                }
            
            return result
        
        except Exception as e:
            logger.error(f"Error getting quote for {symbol}: {e}")
            return None
    
    def get_batch_quotes(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        批量获取实时行情
        
        Args:
            symbols: 股票代码列表
        
        Returns:
            {symbol: quote_dict} 字典
        """
        try:
            result = {}
            for symbol in symbols:
                # Fallback to individual quote fetching to avoid full-market polling bans
                quote = self.get_realtime_quote(symbol, use_cache=True)
                if quote:
                    result[symbol] = quote
            
            return result
        
        except Exception as e:
            logger.error(f"Error getting batch quotes: {e}")
            return {}
    
    def check_trading_status(self, quote: Dict[str, Any]) -> Dict[str, bool]:
        """
        检查交易状态
        
        Args:
            quote: 行情字典
        
        Returns:
            状态字典：{
                "is_limit_up": 是否涨停,
                "is_limit_down": 是否跌停,
                "is_suspended": 是否停牌,
                "is_tradable": 是否可交易
            }
        """
        price = quote.get("price", 0)
        change_pct = quote.get("change_pct", 0)
        volume = quote.get("volume", 0)
        
        # 涨停判断（涨幅 >= 9.9%，考虑浮点误差）
        is_limit_up = change_pct >= 9.9
        
        # 跌停判断（跌幅 <= -9.9%）
        is_limit_down = change_pct <= -9.9
        
        # 停牌判断（价格为0或成交量为0）
        is_suspended = price == 0 or volume == 0
        
        # 可交易判断
        is_tradable = not (is_limit_up or is_limit_down or is_suspended)
        
        return {
            "is_limit_up": is_limit_up,
            "is_limit_down": is_limit_down,
            "is_suspended": is_suspended,
            "is_tradable": is_tradable,
        }
    
    def filter_by_liquidity(
        self,
        quotes: Dict[str, Dict[str, Any]],
        min_amount: float = 10_000_000,  # 最小成交额 1000万
        min_turnover: float = 0.5,  # 最小换手率 0.5%
    ) -> Dict[str, Dict[str, Any]]:
        """
        按流动性过滤
        
        Args:
            quotes: 行情字典
            min_amount: 最小成交额（元）
            min_turnover: 最小换手率（%）
        
        Returns:
            过滤后的行情字典
        """
        filtered = {}
        
        for symbol, quote in quotes.items():
            amount = quote.get("amount", 0)
            turnover = quote.get("turnover_rate", 0)
            
            # 流动性检查
            if amount >= min_amount and turnover >= min_turnover:
                filtered[symbol] = quote
            else:
                logger.debug(
                    f"Filtered out {symbol}: amount={amount:.0f}, turnover={turnover:.2f}%"
                )
        
        return filtered
    
    def filter_tradable(
        self,
        quotes: Dict[str, Dict[str, Any]],
        exclude_limit_up: bool = True,
        exclude_limit_down: bool = True,
        exclude_suspended: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        过滤可交易股票
        
        Args:
            quotes: 行情字典
            exclude_limit_up: 排除涨停
            exclude_limit_down: 排除跌停
            exclude_suspended: 排除停牌
        
        Returns:
            过滤后的行情字典
        """
        filtered = {}
        
        for symbol, quote in quotes.items():
            status = self.check_trading_status(quote)
            
            # 检查是否应该排除
            should_exclude = (
                (exclude_limit_up and status["is_limit_up"]) or
                (exclude_limit_down and status["is_limit_down"]) or
                (exclude_suspended and status["is_suspended"])
            )
            
            if not should_exclude:
                filtered[symbol] = quote
            else:
                logger.debug(f"Filtered out {symbol}: {status}")
        
        return filtered
    
    def get_filtered_quotes(
        self,
        symbols: List[str],
        min_amount: float = 10_000_000,
        min_turnover: float = 0.5,
        exclude_limit: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        获取过滤后的行情（一站式）
        
        Args:
            symbols: 股票代码列表
            min_amount: 最小成交额
            min_turnover: 最小换手率
            exclude_limit: 排除涨跌停
        
        Returns:
            过滤后的行情字典
        """
        # 获取行情
        quotes = self.get_batch_quotes(symbols)
        
        # 流动性过滤
        quotes = self.filter_by_liquidity(quotes, min_amount, min_turnover)
        
        # 交易状态过滤
        if exclude_limit:
            quotes = self.filter_tradable(quotes)
        
        logger.info(f"Filtered quotes: {len(quotes)}/{len(symbols)}")
        return quotes


# 全局实例
market_prober = MarketProber()


# 便捷函数
def get_realtime_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """获取实时行情"""
    return market_prober.get_realtime_quote(symbol)


def get_batch_quotes(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量获取行情"""
    return market_prober.get_batch_quotes(symbols)


def filter_by_liquidity(
    quotes: Dict[str, Dict[str, Any]],
    min_amount: float = 10_000_000,
    min_turnover: float = 0.5,
) -> Dict[str, Dict[str, Any]]:
    """流动性过滤"""
    return market_prober.filter_by_liquidity(quotes, min_amount, min_turnover)


def check_trading_status(quote: Dict[str, Any]) -> Dict[str, bool]:
    """检查交易状态"""
    return market_prober.check_trading_status(quote)
