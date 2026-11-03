"""
Market Data Providers Implementation
"""
import os
import logging
from typing import Dict, Any, Optional

from .base import BaseMarketProvider

# Import existing clients
from modules.ingestion.akshare_client import akshare_client
from core.cache import cached

logger = logging.getLogger(__name__)

class AkShareProvider(BaseMarketProvider):
    """
    AkShare Provider (Free, existing core)
    """
    @property
    def provider_name(self) -> str:
        return "AkShare"
        
    def health_check(self) -> bool:
        return True # Always assume true, handles failure internally
        
    def get_quote(self, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        try:
            return akshare_client.get_realtime_quote_eastmoney(symbol, market)
        except Exception as e:
            logger.error(f"AkShare get_quote failed for {symbol}: {e}")
            return None


class TushareProvider(BaseMarketProvider):
    """
    Tushare Pro Provider
    """
    def __init__(self):
        self.token = os.getenv("TUSHARE_TOKEN")
        self.api = None
        if self.token:
            try:
                import tushare as ts
                ts.set_token(self.token)
                self.api = ts.pro_api()
            except ImportError:
                logger.warning("tushare library not installed.")
                pass
                
    @property
    def provider_name(self) -> str:
        return "Tushare"
        
    def health_check(self) -> bool:
        return self.api is not None
        
    def get_quote(self, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        # Tushare mapping can be complex, primarily focused on A-share end-of-day.
        # This is a stub for future deep integration. Realtime is better via AkShare for now.
        return None

class FinnhubProvider(BaseMarketProvider):
    """
    Finnhub Provider for robust US stock quotes.
    """
    def __init__(self):
        self.api_key = os.getenv("FINNHUB_API_KEY")
        
    @property
    def provider_name(self) -> str:
        return "Finnhub"
        
    def health_check(self) -> bool:
        return bool(self.api_key)
        
    def get_quote(self, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        if not self.health_check() or market != "US":
            return None
        norm_symbol = str(symbol or "").split(".")[-1].strip().upper()
        if not norm_symbol:
            return None
        return self._cached_quote(norm_symbol)

    @staticmethod
    @cached("finnhub_quote", ttl=120)
    def _cached_quote(norm_symbol: str) -> Optional[Dict[str, Any]]:
        api_key = os.getenv("FINNHUB_API_KEY", "").strip()
        if not api_key:
            return None
        try:
            import requests
            url = f"https://finnhub.io/api/v1/quote?symbol={norm_symbol}&token={api_key}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('c') and data.get('c') > 0:
                    return {
                        "symbol": norm_symbol,
                        "price": data['c'],
                        "change": data['d'],
                        "pct_chg": data['dp'],
                        "timestamp": "now"
                    }
        except Exception as e:
            logger.error(f"Finnhub quote failed: {e}")
        return None
