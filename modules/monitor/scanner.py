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
    def _is_market_open(market: str) -> bool:
        """检查是否在盘中交易时间或盘后半小时内"""
        now = datetime.datetime.now()
        # 简单过滤周末
        if now.weekday() >= 5:
            return False
            
        time_str = now.strftime("%H:%M")
        
        if market == 'CN':
            # A股: 09:30-11:30, 13:00-15:00. 放宽到 15:30 允许尾盘数据延迟
            if ("09:30" <= time_str <= "11:35") or ("13:00" <= time_str <= "15:30"):
                return True
        elif market == 'HK':
            # 港股: 09:30-12:00, 13:00-16:00. 放宽到 16:30
            if ("09:30" <= time_str <= "12:05") or ("13:00" <= time_str <= "16:30"):
                return True
        elif market == 'US':
            # 美股 (北京时间): 夏令时 21:30-04:00, 冬令时 22:30-05:00. 粗略放宽
            # 晚上21:30 到次日凌晨 05:30
            if time_str >= "21:30" or time_str <= "05:30":
                return True
                
        return False

    @staticmethod
    def _check_item(symbol: str, item: dict, repo: WatchlistRepository):
        market = item['market']
        
        # 0. 检查是否在交易时段（防止晚上重启拉取收盘价报警）
        if not MonitorService._is_market_open(market):
            return
            
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
        Deep analysis using LLM + Multi-Channel Search
        """
        symbol = item['symbol']
        name = item['name']
        chat_id = item.get('chat_id')
        market = item['market']
        
        try:
            # 1. Gather Info (Multi-source parallel search)
            news_context = ""
            
            # Construct comprehensive search queries based on market
            if market == 'US':
                q_specific = f"{symbol} {name} stock jump drop reason news latest"
                q_market = f"US stock market tech sector news today"
            else:
                q_specific = f"{symbol} {name} 股价 异动原因 涨停 跌停 最新消息 最新公告"
                q_market = f"A股 今日大盘 异动 板块 领涨 领跌"
                
            logger.info(f"Gathering multi-channel info for {symbol}...")
            
            # Parallel search for both specific and market context
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as search_pool:
                fut_specific = search_pool.submit(Tools.web_search, q_specific)
                fut_market = search_pool.submit(Tools.web_search, q_market)
                
                specific_news = fut_specific.result()
                market_news = fut_market.result()
                
            news_context = f"【个股专有资讯】:\n{specific_news}"
            if len(specific_news) < 100 or "暂无相关新闻" in specific_news:
                logger.info(f"Specific news weak for {symbol}, appending deep market context...")
                news_context += f"\n\n【市场/大盘/板块背景】:\n{market_news}"

            # 2. LLM Analysis (Structured & Strict)
            system_prompt = (
                "你是一名顶尖的机构投研分析师。你的任务是根据提供的情报，为基金经理提供最精准、冷酷的股价异动归因。\n"
                "**绝对禁令**：\n"
                "1. 绝不允许使用任何客套话、总结性废话（如'综上所述'、'值得注意的是'、'股市有风险，投资需谨慎'）。\n"
                "2. 绝不允许编造。如果情报中没有个股的原因，必须直言'缺乏个股独立催化，倾向于板块跟风或资金博弈'。\n"
                "**归因逻辑**：\n"
                "- 优先寻找量化财务、重大合同、大行评级、政策突发等一级催化剂。\n"
                "- 其次寻找同板块龙头带动的跟随效应。\n"
                "**输出风格**：\n"
                "字数极简，一针见血，必须以专业的财经金融黑话（如：业绩超预期、情绪退潮、资金抱团、高切低）进行表述。\n"
                "输出必须是严格的JSON格式。"
            )
            
            user_prompt = f"""
            标的：{name} ({symbol} - {market})
            异动：今日{direction} {quote['pct_chg']}%
            
            【检索到的高价值情报库】
            {news_context}
            
            请直接输出JSON（reason字段请用最精炼的1-2句话直击要害）。
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
                    f"🎯 **极简复盘**: {name} ({symbol})\n\n"
                    f"⚡ **异动核心**: {result.reason}\n"
                    f"📈 **置信度**: {result.confidence}\n"
                    f"📝 **快照**: {result.summary}\n\n"
                    f"🔗 **溯源**: {', '.join(result.sources[:2])}"
                )
                
            except Exception as llm_e:
                logger.error(f"LLM Structured output failed: {llm_e}")
                fallback_prompt = f"{user_prompt}\n直接给出1句话原因，不要任何其他字。"
                analysis_text = simple_prompt(fallback_prompt)
                analysis_text = f"🎯 **直接归因**: {analysis_text}"

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
