import logging
import re
from typing import Optional, Tuple
from duckduckgo_search import DDGS

logger = logging.getLogger(__name__)

class SymbolResolver:
    """
    股票代码识别器
    将用户输入的自然语言（如"腾讯"、"茅台"）转换为标准代码
    """
    
    @staticmethod
    def resolve(query: str) -> Optional[Tuple[str, str, str]]:
        """
        解析股票代码
        Returns: (symbol, market, name) or None
        """
        query = query.strip().upper()
        
        # 1. 规则匹配 (优先)
        # A股 (6位数字)
        if re.match(r"^\d{6}$", query):
            return (query, "CN", "未知A股") 
            
        # 港股 (5位数字)
        if re.match(r"^\d{5}$", query):
            return (query, "HK", "未知港股")
            
        # 美股 (纯字母，2-5位) -> 直接信任用户输入
        # 避免去搜 "CRCL" 结果搜出一堆无关信息
        if re.match(r"^[A-Z]{2,5}$", query):
            return (query, "US", query)
            
        # 2. 联网搜索 (Bocha/DDG)
        # 搜索词: "谷歌 股票代码"
        from core.agent import Tools # Lazy import
        search_query = f"{query} 股票代码"
        
        try:
            logger.info(f"Resolving symbol for '{query}' via Agent Tools...")
            # Use the robust web_search tool from Agent
            content = Tools.web_search(search_query)
            
            # DEBUG: Log content to see what we got
            logger.info(f"Web Search Result for '{query}': {content[:500]}...")
            
            # A股正则: 60xxxx, 00xxxx, 30xxxx
            cn_match = re.search(r"\b(60\d{4}|00\d{4}|30\d{4})\b", content)
            if cn_match:
                return (cn_match.group(1), "CN", query)
                
            # 港股正则: 0xxxx (通常显示为 00700)
            hk_match = re.search(r"\b(0\d{4})\b", content)
            if hk_match:
                return (hk_match.group(1), "HK", query)
                
            # 美股正则: 匹配 (AAPL) 格式优先，或者明显的代码
            # 排除常见单词
            exclude_words = ["THE", "INC", "CORP", "LTD", "PLC", "USA", "HKG", "NYSE", "NASDAQ"]
            
            # 1. 括号优先 (GOOG)
            us_match_1 = re.search(r"\(([A-Z]{2,5})\)", content)
            if us_match_1:
                code = us_match_1.group(1)
                if code not in exclude_words and code not in ["HK", "SH", "SZ", "CN"]:
                    return (code, "US", query)
            
            # 2. 单词边界匹配 (GOOG) - 风险较大，需谨慎
            # 只有当 query 是中文时才尝试这个，防止把英文单词当代码
            # 假设 DuckDuckGo snippet: "Alphabet Inc. Class C (GOOG)"
            # ...
            
            # Fallback: 如果都没匹配到，尝试用 LLM 解析 (Slow but smart)
            # 但这里为了响应速度，暂不引入 LLM 解析，除非用户接受等待。
            
        except Exception as e:
            logger.error(f"Symbol resolve failed: {e}")
                
        except Exception as e:
            logger.error(f"Symbol resolve failed: {e}")
            
        return None