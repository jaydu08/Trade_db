"""
Data Factory (Router)
统一管理各种数据源，负责并发调用与失败降级。
"""
import logging
import concurrent.futures
from typing import List, Dict, Any, Optional

from modules.ingestion.providers.search_providers import (
    SearXNGProvider, TavilyProvider, BochaProvider
)
from modules.ingestion.providers.market_providers import (
    AkShareProvider, TushareProvider, FinnhubProvider
)

logger = logging.getLogger(__name__)

class DataManager:
    """
    统一数据源管理器
    """
    def __init__(self):
        # 注册 Search Providers
        self.search_providers = [
            SearXNGProvider(),
            TavilyProvider(),
            BochaProvider(),
            # 其他可以在此添加
        ]
        
        # 过滤出可用的
        self.active_search_providers = [p for p in self.search_providers if p.health_check()]
        logger.info(f"Active search providers: {[p.provider_name for p in self.active_search_providers]}")
        
        # 注册 Market Providers
        self.market_providers = [
            FinnhubProvider(), # 优先海外优质源
            AkShareProvider(), # 底层兜底全量源
            TushareProvider()
        ]
        self.active_market_providers = [p for p in self.market_providers if p.health_check()]
        
    def search(self, query: str, limit_per_source: int = 5, timeout: int = 20) -> str:
        """
        并发调用所有可用的搜索源，并将结果格式化为 Markdown 字符串供 Agent 使用。
        """
        logger.info(f"DataManager executing concurrent search for: '{query}'")
        
        if not self.active_search_providers:
            return "本地未配置任何有效的搜索引擎 (如 SearXNG, Tavily 等)。"
            
        all_results_str = []
        
        def _fetch(provider):
            try:
                res = provider.search(query, limit=limit_per_source)
                return provider.provider_name, res
            except Exception as e:
                logger.error(f"Provider {provider.provider_name} search failed: {e}")
                return provider.provider_name, []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.active_search_providers)) as executor:
            futures = [executor.submit(_fetch, p) for p in self.active_search_providers]
            
            for future in concurrent.futures.as_completed(futures, timeout=timeout):
                try:
                    p_name, results = future.result()
                    if results:
                        formatted = f"【{p_name} 搜索结果】:\n"
                        for i, r in enumerate(results, 1):
                            date_str = f"[{r['date']}] " if r.get('date') else ""
                            formatted += f"{i}. {date_str}{r['title']}: {r['snippet'][:200]}...\n"
                        all_results_str.append(formatted)
                except concurrent.futures.TimeoutError:
                    logger.warning("Search provider timed out.")
                except Exception as e:
                    logger.error(f"Error processing search future: {e}")
                    
        if all_results_str:
            return "\n".join(all_results_str)
            
        return "所有搜索引擎均未返回有效结果。"

    def get_quote(self, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        """
        按优先级尝试获取实时行情
        """
        for provider in self.active_market_providers:
            try:
                res = provider.get_quote(symbol, market)
                if res is not None:
                    # 标准化返回
                    return {
                        "provider": provider.provider_name,
                        **res
                    }
            except Exception as e:
                logger.debug(f"{provider.provider_name} failed to get quote for {symbol}: {e}")
                continue
                
        return None

# 全局单例
data_manager = DataManager()
