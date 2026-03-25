"""
Search Providers Implementation
"""
import os
import requests
import logging
import html
import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from .base import BaseSearchProvider

logger = logging.getLogger(__name__)

class SearXNGProvider(BaseSearchProvider):
    """
    SearXNG 多 URLs 自动降级 Provider
    """
    def __init__(self):
        # 允许多个URL配置，逗号分隔。若未配置，使用公网可用实例兜底。
        urls_str = os.getenv("SEARXNG_URLS", "")
        urls = [url.strip() for url in urls_str.split(",") if url.strip()]
        fallback_urls = [
            "https://searx.be",
            "https://searx.tiekoetter.com",
        ]
        if not urls:
            urls = fallback_urls.copy()
        else:
            for u in fallback_urls:
                if u not in urls:
                    urls.append(u)
        self.urls = urls
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
                logger.warning(
                    "SearXNG instance %s returned status=%s, body=%s",
                    url,
                    resp.status_code,
                    str(resp.text)[:120].replace("\n", " "),
                )
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
        self.api_key = os.getenv("TAVILY_API_KEY") or os.getenv("TAVILY_KEY")
        
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
        self.api_key = (
            os.getenv("BOCHA_API_KEY")
            or os.getenv("BOCHA_KEY")
            or os.getenv("BOCHA_API_TOKEN")
        )
        
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


class GoogleNewsRSSProvider(BaseSearchProvider):
    """
    Google News RSS 兜底搜索（免 API Key）
    """
    def __init__(self):
        self.url = "https://news.google.com/rss/search"
        self.timeout = 12

    @property
    def provider_name(self) -> str:
        return "GoogleNewsRSS"

    def health_check(self) -> bool:
        return True

    @staticmethod
    def _clean_text(text: str) -> str:
        if not text:
            return ""
        unescaped = html.unescape(str(text))
        cleaned = re.sub(r"<[^>]+>", " ", unescaped)
        return re.sub(r"\s+", " ", cleaned).strip()

    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        params = {
            "q": query,
            "hl": kwargs.get("hl", "zh-CN"),
            "gl": kwargs.get("gl", "CN"),
            "ceid": kwargs.get("ceid", "CN:zh-Hans"),
        }
        try:
            resp = requests.get(self.url, params=params, timeout=self.timeout)
            if resp.status_code != 200:
                logger.warning("GoogleNewsRSS returned %s: %s", resp.status_code, str(resp.text)[:120])
                return []

            root = ET.fromstring(resp.content)
            items = root.findall(".//item")
            results: List[Dict[str, Any]] = []
            for item in items[:limit]:
                title = self._clean_text(item.findtext("title", default=""))
                link = self._clean_text(item.findtext("link", default=""))
                pub_date = self._clean_text(item.findtext("pubDate", default=""))
                snippet = self._clean_text(item.findtext("description", default=""))
                results.append(
                    {
                        "title": title,
                        "snippet": snippet,
                        "url": link,
                        "date": pub_date,
                    }
                )
            return results
        except Exception as e:
            logger.error(f"GoogleNewsRSS search failed: {e}")
            return []
