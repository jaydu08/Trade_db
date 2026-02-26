import logging
import re
from typing import Optional, Tuple
from duckduckgo_search import DDGS
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)

class SymbolResolver:
    """
    股票代码识别器
    将用户输入的自然语言（如"腾讯"、"茅台"）转换为标准代码
    """
    
    @staticmethod
    def _fetch_name_by_quote(symbol: str, market: str, fallback_name: str) -> str:
        """尝试通过拉取行情获取真实的股票名称"""
        try:
            quote = akshare_client.get_realtime_quote_eastmoney(symbol, market)
            if quote and quote.get("name"):
                return quote["name"]
        except Exception as e:
            logger.warning(f"Failed to fetch name for {symbol}: {e}")
        return fallback_name

    @staticmethod
    def resolve(query: str) -> Optional[Tuple[str, str, str]]:
        """
        解析股票代码
        Returns: (symbol, market, name) or None
        """
        query = query.strip().upper()
        
        # 1. 规则匹配 (优先)
        # A股 (6位数字) - 包含主板、中小板、创业板、科创板 (688), 北交所等
        if re.match(r"^(60\d{4}|00\d{4}|30\d{4}|688\d{3}|8\d{5}|4\d{5}|87\d{4})$", query):
            name = SymbolResolver._fetch_name_by_quote(query, "CN", "未知A股")
            return (query, "CN", name) 
            
        # 港股 (5位数字)
        if re.match(r"^\d{5}$", query):
            name = SymbolResolver._fetch_name_by_quote(query, "HK", "未知港股")
            return (query, "HK", name)
            
        # 美股 (纯字母，2-5位) -> 直接信任用户输入
        # 避免去搜 "CRCL" 结果搜出一堆无关信息
        if re.match(r"^[A-Z]{2,5}$", query):
            name = SymbolResolver._fetch_name_by_quote(query, "US", query)
            return (query, "US", name)
            
        # 2. 联网搜索 (Bocha/DDG / Agent)
        # 搜索词: "谷歌 股票代码"
        from core.agent import Tools # Lazy import
        search_query = f"{query} 股票代码"
        
        try:
            logger.info(f"Resolving symbol for '{query}' via Agent Tools...")
            # Use the robust web_search tool from Agent
            content = Tools.web_search(search_query)
            
            # DEBUG: Log content to see what we got
            logger.info(f"Web Search Result for '{query}': {content[:500]}...")
            
            # A股正则: 包含主板、创业板、科创板、北交所等
            cn_match = re.search(r"\b(60\d{4}|00\d{4}|30\d{4}|688\d{3}|8\d{5}|4\d{5}|87\d{4})\b", content)
            if cn_match:
                symbol = cn_match.group(1)
                name = SymbolResolver._fetch_name_by_quote(symbol, "CN", query)
                return (symbol, "CN", name)
                
            # 港股正则: 0xxxx (通常显示为 00700)
            hk_match = re.search(r"\b(0\d{4})\b", content)
            if hk_match:
                symbol = hk_match.group(1)
                name = SymbolResolver._fetch_name_by_quote(symbol, "HK", query)
                return (symbol, "HK", name)
                
            # 美股正则: 匹配 (AAPL) 格式优先，或者明显的代码
            # 排除常见单词
            exclude_words = ["THE", "INC", "CORP", "LTD", "PLC", "USA", "HKG", "NYSE", "NASDAQ"]
            
            # 1. 括号优先 (GOOG)
            us_match_1 = re.search(r"\(([A-Z]{2,5})\)", content)
            if us_match_1:
                code = us_match_1.group(1)
                if code not in exclude_words and code not in ["HK", "SH", "SZ", "CN"]:
                    name = SymbolResolver._fetch_name_by_quote(code, "US", query)
                    return (code, "US", name)
            
        except Exception as e:
            logger.error(f"Symbol resolve failed: {e}")
            
        return None