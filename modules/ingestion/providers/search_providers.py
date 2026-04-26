"""
Search Providers Implementation
"""
import os
import requests
import logging
import html
import re
import time
import xml.etree.ElementTree as ET
from typing import List, Dict, Any

from .base import BaseSearchProvider

logger = logging.getLogger(__name__)


class SearXNGProvider(BaseSearchProvider):
    """SearXNG 多实例自动降级 Provider（含实例冷却）"""

    def __init__(self):
        urls_str = os.getenv("SEARXNG_URLS", "")
        urls = [url.strip() for url in urls_str.split(",") if url.strip()]
        fallback_urls = [
            "https://search.sapti.me",
            "https://searx.be",
            "https://search.bus-hit.me",
        ]
        if not urls:
            urls = fallback_urls.copy()
        else:
            for u in fallback_urls:
                if u not in urls:
                    urls.append(u)

        self.urls = urls
        self.timeout = int(os.getenv("SEARXNG_TIMEOUT", "8") or 8)
        self.url_cooldown: Dict[str, float] = {}

    @property
    def provider_name(self) -> str:
        return "SearXNG"

    def health_check(self) -> bool:
        return len(self.urls) > 0

    def _is_url_available(self, url: str) -> bool:
        return time.time() >= float(self.url_cooldown.get(url, 0))

    def _mark_cooldown(self, url: str, seconds: int) -> None:
        self.url_cooldown[url] = time.time() + max(1, seconds)

    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if not self.health_check():
            return []

        params = {
            "q": query,
            "format": "json",
            "language": kwargs.get("language", "zh"),
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        }

        for url in self.urls:
            if not self._is_url_available(url):
                continue

            endpoint = f"{url.rstrip('/')}/search"
            try:
                resp = requests.get(endpoint, params=params, headers=headers, timeout=self.timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    results = []
                    for item in data.get("results", [])[:limit]:
                        results.append(
                            {
                                "title": item.get("title", ""),
                                "snippet": item.get("content", ""),
                                "url": item.get("url", ""),
                                "date": item.get("publishedDate", ""),
                            }
                        )

                    if results:
                        self._mark_cooldown(url, 0)
                    return results

                # 反爬/限流直接长冷却
                if resp.status_code in {403, 429}:
                    self._mark_cooldown(url, 600)
                else:
                    self._mark_cooldown(url, 180)

                logger.warning(
                    "SearXNG instance %s returned status=%s, body=%s",
                    url,
                    resp.status_code,
                    str(resp.text)[:120].replace("\n", " "),
                )
            except Exception as e:
                self._mark_cooldown(url, 180)
                logger.warning("SearXNG instance %s failed: %s", url, e)
                continue

        logger.warning("All SearXNG instances unavailable or failed.")
        return []


class TavilyProvider(BaseSearchProvider):
    """Tavily 搜索 API（含额度/鉴权冷却）"""

    def __init__(self):
        self.api_key = os.getenv("TAVILY_API_KEY") or os.getenv("TAVILY_KEY")
        self.cooldown_until = 0.0

    @property
    def provider_name(self) -> str:
        return "Tavily"

    def health_check(self) -> bool:
        return bool(self.api_key)

    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if not self.health_check():
            return []
        if time.time() < self.cooldown_until:
            return []

        url = "https://api.tavily.com/search"
        payload = {
            "api_key": self.api_key,
            "query": query,
            "search_depth": kwargs.get("search_depth", "basic"),
            "max_results": limit,
        }

        try:
            resp = requests.post(url, json=payload, timeout=12)
            if resp.status_code == 200:
                results = []
                for item in resp.json().get("results", []):
                    results.append(
                        {
                            "title": item.get("title", ""),
                            "snippet": item.get("content", ""),
                            "url": item.get("url", ""),
                            "date": item.get("published_date", ""),
                        }
                    )
                return results

            # 超配额/鉴权问题，长冷却
            if resp.status_code in {401, 403, 429, 432}:
                self.cooldown_until = time.time() + 21600
            else:
                self.cooldown_until = time.time() + 300

            logger.warning("Tavily returned %s: %s", resp.status_code, str(resp.text)[:160])
        except Exception as e:
            self.cooldown_until = time.time() + 120
            logger.warning("Tavily search failed: %s", e)

        return []


class DuckDuckGoProvider(BaseSearchProvider):
    """DuckDuckGo 免费搜索（无需 API Key）"""

    def __init__(self):
        self.cooldown_until = 0.0

    @property
    def provider_name(self) -> str:
        return "DuckDuckGo"

    def health_check(self) -> bool:
        return True

    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if time.time() < self.cooldown_until:
            return []
        try:
            from ddgs import DDGS
            raw = DDGS().text(query, max_results=limit)
            results = []
            for item in raw:
                results.append({
                    "title": item.get("title", ""),
                    "snippet": item.get("body", ""),
                    "url": item.get("href", ""),
                    "date": "",
                })
            return results
        except Exception as e:
            self.cooldown_until = time.time() + 120
            logger.warning("DuckDuckGo search failed: %s", e)
            return []


class BochaProvider(BaseSearchProvider):
    """Bocha AI Search（含鉴权冷却）"""

    def __init__(self):
        self.api_key = (
            os.getenv("BOCHA_API_KEY")
            or os.getenv("BOCHA_KEY")
            or os.getenv("BOCHA_API_TOKEN")
        )
        self.cooldown_until = 0.0

    @property
    def provider_name(self) -> str:
        return "BochaAI"

    def health_check(self) -> bool:
        return bool(self.api_key)

    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        if not self.health_check():
            return []
        if time.time() < self.cooldown_until:
            return []

        url = "https://api.bochaai.com/v1/web-search"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        payload = {
            "query": query,
            "count": limit,
            "freshness": kwargs.get("freshness", "oneMonth"),
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 200 and data.get("data"):
                    web_pages = data["data"].get("webPages", {}).get("value", [])
                    results = []
                    for item in web_pages:
                        results.append(
                            {
                                "title": item.get("name", ""),
                                "snippet": item.get("snippet", "") or item.get("summary", ""),
                                "url": item.get("url", ""),
                                "date": item.get("datePublished", "")[:10],
                            }
                        )
                    return results

            if resp.status_code in {401, 403, 429}:
                self.cooldown_until = time.time() + 21600
            else:
                self.cooldown_until = time.time() + 300
        except Exception as e:
            self.cooldown_until = time.time() + 120
            logger.warning("Bocha search failed: %s", e)

        return []


class GoogleNewsRSSProvider(BaseSearchProvider):
    """Google News RSS 兜底搜索（免 API Key）"""

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

    @staticmethod
    def _simplify_query(query: str) -> str:
        q = re.sub(r"[\|,:;()（）\[\]{}]", " ", str(query or ""))
        parts = [p for p in q.split() if p]
        return " ".join(parts[:6]) if parts else query

    def _search_once(self, query: str, limit: int, **kwargs) -> List[Dict[str, Any]]:
        params = {
            "q": query,
            "hl": kwargs.get("hl", "zh-CN"),
            "gl": kwargs.get("gl", "CN"),
            "ceid": kwargs.get("ceid", "CN:zh-Hans"),
        }

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

    def search(self, query: str, limit: int = 5, **kwargs) -> List[Dict[str, Any]]:
        try:
            # 1) 默认中文区域
            res = self._search_once(query, limit, **kwargs)
            if res:
                return res

            # 2) 查询过长时做轻量回退
            q2 = self._simplify_query(query)
            if q2 and q2 != query:
                res2 = self._search_once(q2, limit, **kwargs)
                if res2:
                    return res2

            # 3) 英文/美股查询场景回退到 US 英文区域
            q_lower = str(query or "").lower()
            if (" us " in f" {q_lower} " or "ipo" in q_lower or "nasdaq" in q_lower or "nyse" in q_lower):
                kw = dict(kwargs)
                kw["hl"] = "en-US"
                kw["gl"] = "US"
                kw["ceid"] = "US:en"
                res3 = self._search_once(query, limit, **kw)
                if res3:
                    return res3
                if q2 and q2 != query:
                    return self._search_once(q2, limit, **kw)

            return []
        except Exception as e:
            logger.warning("GoogleNewsRSS search failed: %s", e)
            return []
