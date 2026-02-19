"""
AkShare API 封装 - 带重试和错误处理
"""
import logging
import time
from typing import Optional, Callable, TypeVar
from functools import wraps

import pandas as pd
import akshare as ak

from config.settings import (
    AKSHARE_RETRY_TIMES,
    AKSHARE_RETRY_DELAY,
    AKSHARE_REQUEST_TIMEOUT,
)
from core.cache import cached

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_on_error(
    max_retries: int = AKSHARE_RETRY_TIMES,
    delay: float = AKSHARE_RETRY_DELAY,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    重试装饰器
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_error: Optional[Exception] = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"AkShare call failed (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))  # 递增延迟
            
            logger.error(f"AkShare call failed after {max_retries} retries: {last_error}")
            raise last_error
        
        return wrapper
    return decorator


class AkShareClient:
    """
    AkShare 客户端封装
    
    提供常用的 A 股数据获取方法
    """
    
    # ================================================================
    # 股票列表
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_info", ttl=3600)  # 缓存1小时
    def get_stock_info_a() -> pd.DataFrame:
        """
        获取 A 股股票列表
        
        Returns:
            DataFrame with columns: 代码, 名称, 上市日期, ...
        """
        logger.info("Fetching A-share stock list...")
        df = ak.stock_info_a_code_name()
        logger.info(f"Fetched {len(df)} A-share stocks")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_sh", ttl=3600)
    def get_stock_info_sh() -> pd.DataFrame:
        """获取上海证券交易所股票列表"""
        logger.info("Fetching Shanghai stock list...")
        df = ak.stock_info_sh_name_code()
        logger.info(f"Fetched {len(df)} SH stocks")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_sz", ttl=3600)
    def get_stock_info_sz() -> pd.DataFrame:
        """获取深圳证券交易所股票列表"""
        logger.info("Fetching Shenzhen stock list...")
        df = ak.stock_info_sz_name_code()
        logger.info(f"Fetched {len(df)} SZ stocks")
        return df
    
    # ================================================================
    # 概念板块
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_concept_board", ttl=3600)
    def get_concept_board_list() -> pd.DataFrame:
        """
        获取东方财富概念板块列表
        
        Returns:
            DataFrame with columns: 排名, 板块名称, 板块代码, ...
        """
        logger.info("Fetching concept board list...")
        df = ak.stock_board_concept_name_em()
        logger.info(f"Fetched {len(df)} concept boards")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_concept_cons", ttl=3600)
    def get_concept_constituents(board_name: str) -> pd.DataFrame:
        """
        获取概念板块成分股
        
        Args:
            board_name: 板块名称
        
        Returns:
            DataFrame with constituents
        """
        logger.info(f"Fetching constituents for concept: {board_name}")
        df = ak.stock_board_concept_cons_em(symbol=board_name)
        logger.info(f"Fetched {len(df)} constituents for {board_name}")
        return df
    
    # ================================================================
    # 行业板块
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_industry_board", ttl=3600)
    def get_industry_board_list() -> pd.DataFrame:
        """
        获取东方财富行业板块列表
        
        Returns:
            DataFrame with columns: 排名, 板块名称, 板块代码, ...
        """
        logger.info("Fetching industry board list...")
        df = ak.stock_board_industry_name_em()
        logger.info(f"Fetched {len(df)} industry boards")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_industry_cons", ttl=3600)
    def get_industry_constituents(board_name: str) -> pd.DataFrame:
        """
        获取行业板块成分股
        
        Args:
            board_name: 板块名称
        
        Returns:
            DataFrame with constituents
        """
        logger.info(f"Fetching constituents for industry: {board_name}")
        df = ak.stock_board_industry_cons_em(symbol=board_name)
        logger.info(f"Fetched {len(df)} constituents for {board_name}")
        return df
    
    # ================================================================
    # 公司信息
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_profile", ttl=86400)  # 缓存24小时
    def get_stock_profile(symbol: str) -> pd.DataFrame:
        """
        获取个股公司简介
        
        Args:
            symbol: 股票代码 (如 "000001")
        
        Returns:
            DataFrame with company profile
        """
        logger.info(f"Fetching profile for: {symbol}")
        df = ak.stock_individual_info_em(symbol=symbol)
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_business", ttl=86400)
    def get_stock_business(symbol: str) -> pd.DataFrame:
        """
        获取个股主营业务
        
        Args:
            symbol: 股票代码
        
        Returns:
            DataFrame with business info
        """
        logger.info(f"Fetching business for: {symbol}")
        try:
            df = ak.stock_zyjs_ths(symbol=symbol)
            return df
        except Exception as e:
            logger.warning(f"Failed to get business for {symbol}: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # 实时行情
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_spot", ttl=60)  # 缓存60秒
    def get_realtime_quotes() -> pd.DataFrame:
        """
        获取 A 股实时行情
        
        Returns:
            DataFrame with realtime quotes
        """
        logger.info("Fetching A-share realtime quotes...")
        df = ak.stock_zh_a_spot_em()
        logger.info(f"Fetched {len(df)} realtime quotes")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_spot_single", ttl=60)
    def get_stock_quote(symbol: str) -> dict:
        """
        获取单个股票实时行情
        
        Args:
            symbol: 股票代码
        
        Returns:
            dict with quote info
        """
        df = AkShareClient.get_realtime_quotes()
        row = df[df["代码"] == symbol]
        if row.empty:
            return {}
        return row.iloc[0].to_dict()


# 全局实例
akshare_client = AkShareClient()
