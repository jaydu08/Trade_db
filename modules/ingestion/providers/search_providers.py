"""
Search Providers Implementation
"""
import os
import requests
import logging
from typing import List, Dict, Any
from .base import BaseSearchProvider

logger = logging.getLogger(__name__)

class SearXNGProvider(BaseSearchProvider):
    """
    SearXNG 多 URLs 自动降级 Provider
    """
    def __init__(self):
        # 允许多个URL配置，逗号分隔
        urls_str = os.getenv("SEARXNG_URLS", "http://localhost:8080,https://searx.be")
        self.urls = [url.strip() for url in urls_str.split(",") if url.strip()]
        self.timeout = 10
        
    @property
    def provider_name(self) -> str:
        return "SearXNG"
        
    def health_check(self) -> bool:
        return len(self.urls) > 0
        
    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if not self.health_check():
            return []
            
        params = {
            "q": query,
            "format": "json",
            "language": "zh",
        }
        
        for url in self.urls:
            endpoint = f"{url.rstrip('/')}/search"
            try:
                resp = requests.get(endpoint, params=params, timeout=self.timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    results = []
                    for item in data.get('results', [])[:limit]:
                        results.append({
                            "title": item.get("title", ""),
                            "snippet": item.get("content", ""),
                            "url": item.get("url", ""),
                            "date": item.get("publishedDate", "")
                        })
                    return results
            except Exception as e:
                logger.warning(f"SearXNG instance {url} failed: {e}. Trying next...")
                continue
                
        logger.error("All SearXNG instances failed.")
        return []


class TavilyProvider(BaseSearchProvider):
    """
    Tavily 搜索 API
    """
    def __init__(self):
        self.api_key = os.getenv("TAVILY_API_KEY")
        
    @property
    def provider_name(self) -> str:
        return "Tavily"
        
    def health_check(self) -> bool:
        return bool(self.api_key)
        
    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if not self.health_check():
            return []
            
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": self.api_key,
            "query": query,
            "search_depth": "basic",
            "max_results": limit
        }
        try:
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                results = []
                for item in resp.json().get('results', []):
                    results.append({
                        "title": item.get("title", ""),
                        "snippet": item.get("content", ""),
                        "url": item.get("url", "")
                    })
                return results
            else:
                logger.warning(f"Tavily returned {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Tavily search failed: {e}")
        return []

class BochaProvider(BaseSearchProvider):
    """
    Bocha AI Search
    """
    def __init__(self):
        # Never ship with a hardcoded secret fallback.
        self.api_key = os.getenv("BOCHA_API_KEY")
        
    @property
    def provider_name(self) -> str:
        return "BochaAI"
        
    def health_check(self) -> bool:
        return bool(self.api_key)
        
    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if not self.health_check():
            return []
            
        url = "https://api.bochaai.com/v1/web-search"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        freshness = kwargs.get("freshness", "oneMonth")
        payload = {
            "query": query,
            "count": limit,
            "freshness": freshness
        }
        
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == 200 and data.get('data'):
                    web_pages = data['data'].get('webPages', {}).get('value', [])
                    results = []
                    for item in web_pages:
                        results.append({
                            "title": item.get("name", ""),
                            "snippet": item.get("snippet", "") or item.get("summary", ""),
                            "url": item.get("url", ""),
                            "date": item.get("datePublished", "")[:10]
                        })
                    return results
        except Exception as e:
            logger.error(f"Bocha search failed: {e}")
            
        return []
