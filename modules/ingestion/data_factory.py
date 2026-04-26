"""
Data Factory (Router)
统一管理各种数据源，负责并发调用与失败降级。
"""
import logging
import concurrent.futures
import hashlib
import os
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv

from core.cache import get_cache, set_cache
from modules.ingestion.providers.search_providers import (
    SearXNGProvider, DuckDuckGoProvider, GoogleNewsRSSProvider
)
from modules.ingestion.providers.market_providers import (
    AkShareProvider, TushareProvider, FinnhubProvider
)

logger = logging.getLogger(__name__)


class DataManager:
    """统一数据源管理器"""

    def __init__(self):
        load_dotenv(dotenv_path=".env")

        self.search_cache_ttl = int(os.getenv("SEARCH_CACHE_TTL", "300") or 300)

        self.search_providers = [
            DuckDuckGoProvider(),
            SearXNGProvider(),
            GoogleNewsRSSProvider(),
        ]
        self.active_search_providers = [p for p in self.search_providers if p.health_check()]
        logger.info(f"Active search providers: {[p.provider_name for p in self.active_search_providers]}")

        self.market_providers = [
            FinnhubProvider(),
            AkShareProvider(),
            TushareProvider(),
        ]
        self.active_market_providers = [p for p in self.market_providers if p.health_check()]
        logger.info(f"Active market providers: {[p.provider_name for p in self.active_market_providers]}")

    @staticmethod
    def _dedupe_results(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen = set()
        for r in items:
            title = str(r.get("title", "")).strip()
            url = str(r.get("url", "")).strip()
            key = (title.lower(), url.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _format_results(provider_name: str, results: List[Dict[str, Any]]) -> str:
        lines = [f"【{provider_name} 搜索结果】:"]
        for i, r in enumerate(results, 1):
            date_str = f"[{r.get('date')}] " if r.get("date") else ""
            title = str(r.get("title", "")).strip()
            snippet = str(r.get("snippet", "")).strip()[:220]
            lines.append(f"{i}. {date_str}{title}: {snippet}...")
        return "\n".join(lines)

    def _search_cache_key(self, query: str, limit_per_source: int) -> str:
        raw = f"{query}|{limit_per_source}".encode("utf-8", errors="ignore")
        return f"search_agg_{hashlib.md5(raw).hexdigest()}"

    def search(self, query: str, limit_per_source: int = 5, timeout: int = 20) -> str:
        """
        并发调用可用搜索源，失败自动降级并缓存。
        """
        logger.info(f"DataManager executing concurrent search for: '{query}'")

        if not self.active_search_providers:
            return "本地未配置任何有效的搜索引擎 (如 SearXNG, Tavily 等)。"

        cache_key = self._search_cache_key(query, limit_per_source)
        cached = get_cache(cache_key)
        if isinstance(cached, str) and cached.strip():
            return cached

        all_blocks: List[str] = []

        def _fetch(provider):
            try:
                res = provider.search(query, limit=limit_per_source)
                return provider.provider_name, res
            except Exception as e:
                logger.warning("Provider %s search failed: %s", provider.provider_name, e)
                return provider.provider_name, []

        workers = max(1, len(self.active_search_providers))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_fetch, p) for p in self.active_search_providers]
            try:
                for future in concurrent.futures.as_completed(futures, timeout=timeout):
                    try:
                        p_name, results = future.result()
                        if not results:
                            continue
                        deduped = self._dedupe_results(results, limit_per_source)
                        if deduped:
                            all_blocks.append(self._format_results(p_name, deduped))
                    except Exception as e:
                        logger.warning("Error processing search future: %s", e)
            except concurrent.futures.TimeoutError:
                logger.warning("Search aggregation timed out, using partial results.")
                for f in futures:
                    if not f.done():
                        f.cancel()

        # 强制兜底：若上游都空，单独走 GoogleNewsRSS 一次轻量查询
        if not all_blocks:
            try:
                rss = GoogleNewsRSSProvider()
                results = rss.search(query, limit=limit_per_source)
                results = self._dedupe_results(results, limit_per_source)
                if results:
                    all_blocks.append(self._format_results(rss.provider_name, results))
            except Exception:
                pass

        if all_blocks:
            merged = "\n\n".join(all_blocks)
            set_cache(cache_key, merged, ttl=self.search_cache_ttl)
            return merged

        return "所有搜索引擎均未返回有效结果。"

    def get_quote(self, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        """按优先级尝试获取实时行情"""
        for provider in self.active_market_providers:
            try:
                res = provider.get_quote(symbol, market)
                if res is not None and float(res.get("price", 0) or 0) > 0:
                    return {
                        "provider": provider.provider_name,
                        **res,
                    }
            except Exception as e:
                logger.debug(f"{provider.provider_name} failed to get quote for {symbol}: {e}")
                continue

        return None


# 全局单例
data_manager = DataManager()
