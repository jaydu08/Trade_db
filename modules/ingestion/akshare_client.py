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
    
    提供常用的市场数据获取方法
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
    @cached("ak_stock_hk", ttl=3600)
    def get_stock_info_hk() -> pd.DataFrame:
        logger.info("Fetching HK stock list...")
        df = AkShareClient._safe_call(
            ["stock_hk_spot_em", "stock_hk_spot"]
        )
        logger.info(f"Fetched {len(df)} HK stocks")
        return df

    @staticmethod
    @retry_on_error()
    @cached("ak_stock_us", ttl=3600)
    def get_stock_info_us() -> pd.DataFrame:
        logger.info("Fetching US stock list...")
        df = AkShareClient._safe_call(
            ["stock_us_spot_em", "stock_us_spot"]
        )
        logger.info(f"Fetched {len(df)} US stocks")
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
    @cached("ak_stock_profile_cninfo", ttl=86400)
    def get_stock_profile_cninfo(symbol: str) -> pd.DataFrame:
        """
        获取个股公司简介（巨潮资讯）
        
        Args:
            symbol: 股票代码
        
        Returns:
            DataFrame with company profile
        """
        logger.info(f"Fetching cninfo profile for: {symbol}")
        try:
            df = ak.stock_profile_cninfo(symbol=symbol)
            return df
        except Exception as e:
            logger.warning(f"Failed to get cninfo profile for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    @retry_on_error()
    @cached("ak_stock_profile_hk", ttl=86400)
    def get_stock_profile_hk(symbol: str) -> pd.DataFrame:
        logger.info(f"Fetching HK profile for: {symbol}")
        # Try new interface for HK profile
        try:
            return ak.stock_hk_company_profile_em(symbol=symbol)
        except Exception as e:
            logger.warning(f"Failed to get HK profile for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    @retry_on_error()
    @cached("ak_stock_profile_us", ttl=86400)
    def get_stock_profile_us(symbol: str) -> pd.DataFrame:
        logger.info(f"Fetching US profile for: {symbol}")
        # Clean symbol for XQ interface (e.g. 105.RXT -> RXT)
        clean_symbol = symbol.split(".")[-1]
        try:
            return ak.stock_individual_basic_info_us_xq(symbol=clean_symbol)
        except Exception as e:
            logger.warning(f"Failed to get US profile for {symbol}: {e}")
            return pd.DataFrame()
    
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
    # 实时行情 (东方财富源 - 极速版)
    # ================================================================
    @staticmethod
    def get_realtime_quote_eastmoney(symbol: str, market: str = "CN") -> dict:
        """
        从东方财富获取实时行情 (更准确，无延时)
        """
        import requests
        import time
        
        # 东方财富接口
        # secid: 1.600519 (SH), 0.300251 (SZ), 116.00700 (HK), 105.NVDA (US)
        # 这里的 secid 需要根据 symbol 动态生成
        
        secid = ""
        if market == "CN":
            if symbol.startswith("6"): secid = f"1.{symbol}"
            else: secid = f"0.{symbol}"
        elif market == "HK":
            secid = f"116.{symbol}"
        elif market == "US":
            # US 需要先查一下代码映射，或者盲猜
            # 东财美股代码通常是 105.xxx, 106.xxx, 107.xxx
            # 简单起见，我们先尝试 105 (纳斯达克) 和 106 (纽交所)
            secid = f"105.{symbol.upper()}"
            
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": secid,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields": "f43,f57,f58,f59,f107,f46,f60,f44,f45,f47,f48,f19,f17,f531,f15,f16,f113",
            "invt": "2",
            "_": int(time.time() * 1000)
        }
        
        try:
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            
            if data and data.get("data"):
                d = data["data"]
                # f43: price, f58: name, f170: change_pct (need check fields)
                # Correct fields mapping:
                # f43: 最新价
                # f58: 名称
                # f169: 涨跌额 (f169 not in fields list, let's use f170 for pct?)
                # Actually eastmoney fields are tricky. Let's use a standard list.
                # f43: price, f58: name, f170: pct_chg, f169: change
                
                # Let's re-request with standard fields
                params["fields"] = "f43,f58,f169,f170,f46,f60,f19,f17,f59,f86" 
                # f86: update_time
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()
                if not data or not data.get("data"):
                    # Retry for US market if first fail
                    if market == "US":
                        params["secid"] = f"106.{symbol.upper()}"
                        resp = requests.get(url, params=params, timeout=10)
                        data = resp.json()
                
                if data and data.get("data"):
                    d = data["data"]
                    price = d.get("f43")
                    if price == "-": return {} # Invalid
                    
                    # 价格修正逻辑
                    final_price = float(price)
                    pct_chg = float(d.get("f170") or 0)
                    change = float(d.get("f169") or 0)
                    
                    if market == "CN":
                        # A股通常是放大100倍
                        if final_price > 1000: final_price = final_price / 100
                        change = change / 100
                        pct_chg = pct_chg / 100
                    elif market == "HK":
                        # 港股通常是放大1000倍
                        if final_price > 1000: final_price = final_price / 1000
                        # 港股涨跌幅通常不需要除，但如果是 414 这种整数，那就要除
                        if abs(pct_chg) > 100: pct_chg = pct_chg / 100
                        # change 也不确定，保守起见不乱动
                    elif market == "US":
                        # 美股情况复杂，东财可能返回放大100倍或1000倍的数值
                        # 启发式：如果价格 > 5000 (BRK.A除外)，尝试缩小
                        if final_price > 10000: 
                            final_price = final_price / 1000 # 316140 -> 316.14
                        elif final_price > 5000:
                            final_price = final_price / 100
                            
                        # 涨跌幅：如果 > 50% (对于 GOOG 这种票)，肯定是放大了
                        if abs(pct_chg) > 50: pct_chg = pct_chg / 100
                        
                        # 涨跌额同理
                        if abs(change) > 50: change = change / 100
                    
                    return {
                        "symbol": symbol,
                        "name": d.get("f58"),
                        "price": final_price,
                        "change": change,
                        "pct_chg": pct_chg,
                        "timestamp": d.get("f86") 
                    }
                    
        except Exception as e:
            logger.warning(f"Eastmoney quote failed for {symbol}: {e}")
            
        return {}

    # ================================================================
    # 实时行情 (AkShare)
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
    def _safe_call(func_names: list[str], **kwargs) -> pd.DataFrame:
        for name in func_names:
            func = getattr(ak, name, None)
            if func is None:
                continue
            try:
                return func(**kwargs)
            except Exception as e:
                logger.warning(f"AkShare call {name} failed: {e}")
        return pd.DataFrame()
    
    @staticmethod
    @retry_on_error()
    def get_stock_info_global_cls() -> pd.DataFrame:
        """获取财联社全球电报"""
        logger.info("Fetching CLS Global Telegraph...")
        # Note: akshare API might change, ensure we use the correct one
        try:
            return ak.stock_info_global_cls()
        except Exception as e:
            logger.warning(f"Failed to fetch global news: {e}")
            return pd.DataFrame()



# 全局实例
akshare_client = AkShareClient()
