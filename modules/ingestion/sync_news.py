
"""
Sync News - 实时新闻监听
"""
import logging
from datetime import datetime
import pandas as pd
from typing import List

from core.db import db_manager, get_collection
from core.llm import get_llm_client
from domain.vector import MarketEventMetadata
import akshare as ak

logger = logging.getLogger(__name__)

class NewsSyncer:
    """
    实时新闻监听器
    """
    def __init__(self):
        self.llm = get_llm_client()
        self.collection = get_collection("market_events")

    def fetch_latest_news(self) -> pd.DataFrame:
        """获取最新的 7x24 小时快讯"""
        logger.info("Fetching latest news...")
        try:
            # 财联社 7x24
            # ak.stock_telegraph_cls(symbol="全部") or similar
            # ak.stock_info_global_cls()?
            
            # 备选: 新浪 7x24
            # ak.stock_news_live_sina() is not available?
            # ak.js_news(timestamp=...)
            
            # Let's try `stock_zh_a_new_em` (个股新闻) or `stock_info_global_cls`
            # For "events", we want global macro or industry news.
            
            # Using `stock_telegraph_cls` if available
            if hasattr(ak, "stock_telegraph_cls"):
                df = ak.stock_telegraph_cls()
                return df
            
            # Fallback to Sina
            # ak.stock_js_news_sina()?
            
            # Let's try to find a working news function
            news_funcs = [f for f in dir(ak) if "news" in f and ("sina" in f or "cls" in f or "cctv" in f)]
            if news_funcs:
                # Prioritize CCTV for major events, or Sina for speed
                # Let's pick `stock_news_live_sina` if exists, else first one
                target = "stock_news_live_sina"
                if target in news_funcs:
                    return getattr(ak, target)()
                else:
                    # Try to call the first one that looks promising
                    for func_name in news_funcs:
                        try:
                            df = getattr(ak, func_name)()
                            if not df.empty:
                                return df
                        except:
                            continue
                            
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to fetch news: {e}")
            return pd.DataFrame()

    def analyze_event(self, content: str) -> dict:
        """
        利用 LLM 分析新闻事件的影响
        """
        prompt = f"""
        请分析以下新闻快讯，判断其对资本市场的影响。
        
        新闻内容: {content}
        
        请提取:
        1. 事件类型 (policy/earnings/product/market/news)
        2. 相关行业 (如: 半导体, 新能源)
        3. 影响方向 (positive/negative/neutral)
        4. 影响评分 (0.0 - 1.0, 1.0为重磅)
        
        以 JSON 格式输出: {{ "event_type": "", "industries": [], "impact": "", "score": 0.5 }}
        """
        try:
            res = self.llm.simple_prompt(prompt, temperature=0.1)
            import json
            if "```json" in res:
                res = res.split("```json")[1].split("```")[0]
            elif "```" in res:
                res = res.split("```")[1].split("```")[0]
            return json.loads(res.strip())
        except Exception:
            return {}

    def sync_news_stream(self, limit: int = 20):
        """
        同步新闻流并分析
        """
        df = self.fetch_latest_news()
        if df.empty:
            logger.warning("No news fetched.")
            return

        logger.info(f"Fetched {len(df)} news items. Analyzing top {limit}...")
        
        # Process latest N
        # Sina columns usually: content, time...
        # Need to check columns
        content_col = None
        for col in df.columns:
            if "内容" in col or "content" in col or "title" in col:
                content_col = col
                break
        
        if not content_col:
            return

        count = 0
        for _, row in df.iterrows():
            if count >= limit: break
            
            content = row[content_col]
            if not content or len(str(content)) < 10: continue
            
            # Analysis
            analysis = self.analyze_event(str(content))
            
            if analysis:
                self._store_event(content, analysis)
                count += 1

    def _store_event(self, content: str, analysis: dict):
        """存入向量库"""
        now = datetime.utcnow().isoformat()
        # Use content hash as ID
        import hashlib
        doc_id = f"evt_{hashlib.md5(content.encode()).hexdigest()[:10]}"
        
        text = f"【市场事件】{content} (影响: {analysis.get('impact')} {analysis.get('score')})"
        
        meta = {
            "event_type": analysis.get("event_type", "news"),
            "event_date": datetime.now().strftime("%Y-%m-%d"),
            "impact": analysis.get("impact", "neutral"),
            "impact_score": float(analysis.get("score", 0.5)),
            "source": "news_stream",
            # Chroma metadata list support is tricky, join them
            "industries": ",".join(analysis.get("industries", [])),
            "created_at": now
        }
        
        try:
            self.collection.add(
                ids=[doc_id],
                documents=[text],
                metadatas=[meta]
            )
            logger.info(f"Stored event: {content[:20]}... ({analysis.get('impact')})")
        except Exception as e:
            logger.warning(f"Vector store failed: {e}")

news_syncer = NewsSyncer()
