
"""
YFinance Client - Yahoo Finance API 封装
用于获取美股/港股历史数据和财务数据
"""
import logging
import yfinance as yf
import pandas as pd
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class YFinanceClient:
    """
    Yahoo Finance 客户端
    """
    
    @staticmethod
    def get_historical_data(symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """
        获取历史行情数据
        """
        # Symbol 映射
        yf_symbol = symbol
        if "." in symbol and symbol.startswith("6"): yf_symbol = f"{symbol[:6]}.SS"
        elif "." in symbol and (symbol.startswith("0") or symbol.startswith("3")): yf_symbol = f"{symbol[:6]}.SZ"
        elif len(symbol) == 5 and symbol.isdigit(): yf_symbol = f"{symbol}.HK" # HK stocks usually 4 or 5 digits
        
        try:
            # Revert to default session handling as curl_cffi conflicts with requests_cache
            # YFinance handles sessions internally if not provided
            ticker = yf.Ticker(yf_symbol)
            hist = ticker.history(period=period, interval=interval)
            return hist
        except Exception as e:
            logger.warning(f"YFinance history failed for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_financials(symbol: str) -> Dict:
        """
        获取财务数据 (Balance Sheet, Income Statement, Cash Flow)
        """
        # Symbol 映射 (同上)
        yf_symbol = symbol
        # Simple mapping for US stocks: usually just ticker (e.g. AAPL)
        # HK stocks: 0700.HK
        
        if symbol.isdigit() and len(symbol) == 5:
             yf_symbol = f"{symbol}.HK"
        elif "." not in symbol: # US stock
             yf_symbol = symbol
             
        try:
            ticker = yf.Ticker(yf_symbol)
            info = ticker.info
            
            # Extract key metrics
            data = {
                "market_cap": info.get("marketCap"),
                "pe_ttm": info.get("trailingPE"),
                "pb": info.get("priceToBook"),
                "revenue": info.get("totalRevenue"),
                "net_income": info.get("netIncomeToCommon"),
                "gross_margins": info.get("grossMargins"),
                "profit_margins": info.get("profitMargins"),
                "roe": info.get("returnOnEquity"),
                "revenue_growth": info.get("revenueGrowth"),
            }
            return data
        except Exception as e:
            logger.warning(f"YFinance financials failed for {symbol}: {e}")
            return {}

yfinance_client = YFinanceClient()
