import asyncio
import aiohttp
import logging
import random
from typing import List, Dict, Any, Optional
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log

logger = logging.getLogger(__name__)

# Constants
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 3
SINA_HQ_URL = "http://hq.sinajs.cn/list="
REFERER_HEADER = {"Referer": "http://finance.sina.com.cn"}


class AsyncMarketProber:
    """
    异步市场探针
    专用于自选股等小规模池子的精准、稳健行情轮询。
    具备并发控制与智能回退重试机制。
    """
    
    def __init__(self, max_concurrent: int = MAX_CONCURRENT_REQUESTS):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    @staticmethod
    def _format_symbol_for_sina(symbol: str, market: str) -> str:
        """根据市场将标的代码转换为 Sina API 所需格式"""
        if market == "CN":
            return f"sh{symbol}" if symbol.startswith("6") else f"sz{symbol}"
        elif market == "HK":
            return f"hk{symbol}"
        elif market == "US":
            return f"gb_{symbol.lower().replace('.', '$')}"
        return symbol

    @staticmethod
    def _parse_sina_response(text: str, market: str, original_symbol: str) -> Optional[Dict[str, Any]]:
        """解析新浪行情单条响应内容 (与原有逻辑兼容)"""
        text = text.strip()
        if not text or '=""' in text or '="",' in text:
            return None
        
        try:
            parts = text.split('="')[1].split('";')[0].split(',')
            if len(parts) < 5:
                return None
                
            name, price, change, pct_chg = "", 0.0, 0.0, 0.0
            
            if market == "CN":
                if len(parts) < 32: return None
                name = parts[0]
                prev_close = float(parts[2])
                price = float(parts[3])
                if prev_close > 0:
                    change = price - prev_close
                    pct_chg = round((change / prev_close) * 100, 2)
            elif market == "HK":
                if len(parts) < 19: return None
                name = parts[1]
                price = float(parts[6])
                change = float(parts[7])
                pct_chg = float(parts[8])
            elif market == "US":
                if len(parts) < 6: return None
                name = parts[0]
                price = float(parts[1])
                pct_chg = float(parts[2])
                change = float(parts[4])
                
            return {
                "symbol": original_symbol,
                "name": name,
                "price": price,
                "change": change,
                "pct_chg": pct_chg,
            }
        except Exception as e:
            logger.debug(f"Parsing failed for {original_symbol}: {e}")
            return None

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(MAX_RETRIES),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _fetch_quote_with_retry(self, session: aiohttp.ClientSession, symbol: str, market: str) -> Optional[Dict[str, Any]]:
        """带重试机制的行情查询 (针对单一标的或小批量)"""
        sina_symbol = self._format_symbol_for_sina(symbol, market)
        url = f"{SINA_HQ_URL}{sina_symbol}"
        
        async with self.semaphore:
            # 增加少量的随机抖动，防止批量并发请求触发封禁
            await asyncio.sleep(random.uniform(0.1, 0.5))
            
            async with session.get(url, headers=REFERER_HEADER, timeout=10) as response:
                response.raise_for_status()
                # Sina 返回的是 GBK 编码数据
                text = await response.text(encoding="gbk", errors="ignore")
                return self._parse_sina_response(text, market, symbol)

    async def get_quotes_async(self, items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        批量异步拉取实时行情
        
        Args:
            items: 包含 {"symbol": "...", "market": "..."} 的字典列表
            
        Returns:
            Dict mapping symbol -> quote data
        """
        if not items:
            return {}
            
        results = {}
        
        # 现代版 aiohttp 会话管理，开启 TCP 连接复用
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                self._fetch_quote_with_retry(session, item["symbol"], item["market"])
                for item in items
            ]
            
            # 收集结果
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            for item, response in zip(items, responses):
                symbol = item["symbol"]
                if isinstance(response, Exception):
                    logger.error(f"Async probe failed for {symbol}: {response}")
                elif response is not None:
                    results[symbol] = response
                    
        return results

# 全局单例
async_prober = AsyncMarketProber()
