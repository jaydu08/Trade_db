import logging
import datetime
import concurrent.futures
from typing import Optional
from pydantic import BaseModel, Field

from modules.monitor.repository import WatchlistRepository
from modules.monitor.notifier import Notifier
from modules.ingestion.akshare_client import akshare_client
from core.llm import structured_output, simple_prompt
from core.db import get_collection
import hashlib
# Import Tools to use the enhanced web_search
from core.agent import Tools 

logger = logging.getLogger(__name__)

# Thread pool for async analysis to avoid blocking the scanner loop
analysis_executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)

class AnalysisResult(BaseModel):
    reason: str = Field(description="The primary reason for the stock movement based on news. If unknown, state 'Unknown'.")
    confidence: str = Field(description="Confidence level: High, Medium, or Low.")
    summary: str = Field(description="A concise summary of the key events (max 100 words).")
    sources: list[str] = Field(description="List of news sources or titles used.")

class MonitorService:
    """
    Anomaly Monitor Service (Refactored)
    """
    
    @staticmethod
    def scan_and_alert():
        """
        Scan all watchlist items for anomalies.
        """
        logger.info("Scanning watchlist...")
        
        repo = WatchlistRepository()
        data = repo.load_all()
        
        if not data:
            return

        for symbol, item in data.items():
            if not item.get('is_active', True):
                continue
            
            try:
                MonitorService._check_item(symbol, item, repo)
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")

    @staticmethod
    def _check_item(symbol: str, item: dict, repo: WatchlistRepository):
        market = item['market']
        
        # 1. Get Quote
        quote = akshare_client.get_realtime_quote_eastmoney(symbol, market)
        if not quote:
            return
            
        pct_chg = quote.get('pct_chg')
        price = quote.get('price')
        
        if pct_chg is None:
            return
            
        # 2. Check Threshold
        threshold = item.get('alert_threshold_pct', 5.0)
        
        if abs(pct_chg) >= threshold:
            # 3. Check Cooldown
            today = str(datetime.date.today())
            last_alert_str = item.get('last_alert_at')
            last_alert = last_alert_str.split()[0] if last_alert_str else None
            
            if last_alert != today:
                # 4. Trigger Alert
                MonitorService.trigger_alert(item, quote)
                
                # 5. Update State
                item['last_alert_at'] = str(datetime.datetime.now())
                repo.add_item(symbol, item)

    @staticmethod
    def trigger_alert(item: dict, quote: dict):
        """
        Two-phase alert system:
        1. Immediate price alert.
        2. Async deep analysis.
        """
        symbol = item['symbol']
        name = item['name']
        pct = quote['pct_chg']
        price = quote['price']
        chat_id = item.get('chat_id')
        
        direction = "📈 暴涨" if pct > 0 else "📉 暴跌"
        
        # Phase 1: Immediate Notification
        short_msg = (
            f"🚨 **{direction}预警**\n"
            f"**标的**: {name} ({symbol})\n"
            f"**幅度**: {pct}%\n"
            f"**现价**: {price}\n"
            f"⏳ 正在进行智能归因分析..."
        )
        Notifier.send_telegram(chat_id, short_msg)
        
        # Phase 2: Async Analysis
        analysis_executor.submit(MonitorService._analyze_and_report, item, quote, direction)

    @staticmethod
    def _analyze_and_report(item: dict, quote: dict, direction: str):
        """
        Deep analysis using LLM + Search
        """
        symbol = item['symbol']
        name = item['name']
        chat_id = item.get('chat_id')
        
        try:
            # 1. Gather Info (Multi-source)
            # Strategy: Search for specific stock news AND general market news if needed
            
            # A. Specific News
            # Construct a better query: "{Symbol} stock news" + "{Name} latest"
            # Split keywords are handled by Tools.web_search now.
            # We combine them to ensure at least one hits.
            specific_query = f"{symbol} {name} stock news latest" 
            news_context = Tools.web_search(specific_query)
            
            # B. Check if we got enough info. If "not found", try market context
            if "暂无相关新闻" in news_context or len(news_context) < 50:
                logger.info(f"Specific news empty for {symbol}, trying market context...")
                market_query = "US stock market news today tech sector" if item['market'] == 'US' else "A股 市场 科技板块 今日"
                market_news = Tools.web_search(market_query)
                news_context += f"\n\n【市场背景】:\n{market_news}"

            # 2. LLM Analysis (Structured)
            system_prompt = (
                "你是专业的金融分析师。请根据提供的新闻分析股价异动原因。\n"
                "**核心原则**：\n"
                "1. 优先寻找个股特定的驱动因素（财报、产品、并购）。\n"
                "2. 如果没有个股新闻，但有市场大盘新闻（如纳指大涨），则归因为'跟随大盘波动'。\n"
                "3. 如果完全找不到原因，请在 reason 字段中如实填写 '暂未找到明确驱动因素'，严禁编造。\n"
                "4. 输出必须是严格的JSON格式。"
            )
            
            user_prompt = f"""
            标的：{name} ({symbol})
            状态：今日{direction} {quote['pct_chg']}%
            
            【新闻情报】
            {news_context}
            """
            
            try:
                # Use structured output for stability
                result: AnalysisResult = structured_output(
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_model=AnalysisResult
                )
                
                analysis_text = (
                    f"🧐 **智能归因报告**: {name}\n\n"
                    f"💡 **核心原因**: {result.reason}\n"
                    f"📊 **置信度**: {result.confidence}\n"
                    f"📝 **摘要**: {result.summary}\n\n"
                    f"🔗 **参考源**: {', '.join(result.sources[:3])}"
                )
                
            except Exception as llm_e:
                logger.error(f"LLM Structured output failed, falling back to simple text: {llm_e}")
                # Fallback to simple prompt
                fallback_prompt = f"{user_prompt}\n请简要分析原因，不要废话。"
                analysis_text = simple_prompt(fallback_prompt)
                analysis_text = f"🧐 **智能归因**: {analysis_text}"

            # 3. Send Report
            Notifier.send_telegram(chat_id, analysis_text)
            
            # 4. Store Event in Vector DB
            try:
                collection = get_collection("market_events")
                
                # Use content hash as ID
                doc_id = f"evt_{datetime.date.today()}_{hashlib.md5(analysis_text.encode()).hexdigest()[:8]}"
                
                # Determine impact based on direction
                impact = "positive" if "涨" in direction else "negative"
                
                # Try to map confidence to a float score
                score = 0.5
                if "High" in result.confidence or "高" in result.confidence:
                    score = 0.9
                elif "Medium" in result.confidence or "中" in result.confidence:
                    score = 0.6
                elif "Low" in result.confidence or "低" in result.confidence:
                    score = 0.3
                    
                meta = {
                    "event_type": "market",
                    "event_date": str(datetime.date.today()),
                    "impact": impact,
                    "impact_score": score,
                    "source": "monitor_scan",
                    "related_symbols": symbol, # Chroma meta doesn't support list directly, use comma separated string if multiple, here just one
                    "doc_version": 1,
                    "created_at": str(datetime.datetime.utcnow())
                }
                
                # Create a comprehensive document text
                doc_text = f"【异动归因】{name}({symbol}) {direction} {quote['pct_chg']}%。\n原因: {result.reason}\n摘要: {result.summary}"
                
                collection.add(
                    ids=[doc_id],
                    documents=[doc_text],
                    metadatas=[meta]
                )
                logger.info(f"Successfully stored market event for {symbol} to vector DB.")
            except Exception as e:
                logger.error(f"Failed to store event to vector DB for {symbol}: {e}")
            
        except Exception as e:
            logger.error(f"Analysis failed for {symbol}: {e}")
            Notifier.send_telegram(chat_id, f"❌ 分析失败: {e}")

    @staticmethod
    def _gather_news(symbol: str, name: str) -> str:
        # Deprecated: Logic moved to Tools.web_search and _analyze_and_report
        return ""
