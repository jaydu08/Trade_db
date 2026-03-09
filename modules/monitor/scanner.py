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
    
    # 异动判定阈值 (%)
    THRESHOLD_PCT = 3.0
    
    @staticmethod
    def scan_and_alert():
        """
        Scan all watchlist items for anomalies using AsyncMarketProber.
        """
        logger.info("Scanning watchlist...")
        
        repo = WatchlistRepository()
        data = repo.load_all()
        
        if not data:
            return

        active_items = []
        for symbol, item in data.items():
            if item.get('is_active', True) and MonitorService._is_market_open(item['market']):
                active_items.append(item)
                
        if not active_items:
            logger.debug("No active items or markets closed.")
            return

        # Fetch quotes asynchronously
        import asyncio
        from modules.probing.async_prober import async_prober
        
        logger.info(f"Fetching quotes for {len(active_items)} watchlist items via async prober...")
        try:
            quotes_results = asyncio.run(async_prober.get_quotes_async(active_items))
        except Exception as e:
            logger.error(f"Failed to fetch quotes async: {e}")
            return

        for item in active_items:
            symbol = item['symbol']
            quote = quotes_results.get(symbol)
            if not quote:
                continue
                
            pct_chg = quote.get('pct_chg')
            
            # 仅推送正向涨幅（只监控上涨异动）
            if pct_chg is None or pct_chg <= 0:
                continue
                
            # Check Threshold
            threshold = item.get('alert_threshold_pct', 5.0)
            
            if pct_chg >= threshold:
                # Check Cooldown
                today = str(datetime.date.today())
                last_alert_str = item.get('last_alert_at')
                last_alert = last_alert_str.split()[0] if last_alert_str else None
                
                if last_alert != today:
                    # Persist Alert to DB
                    MonitorService._persist_alert(item, quote)
                    
                    # Trigger Alert
                    MonitorService.trigger_alert(item, quote)
                    
                    # Update State
                    item['last_alert_at'] = str(datetime.datetime.now())
                    repo.add_item(symbol, item)

    @staticmethod
    def _is_market_open(market: str) -> bool:
        """检查是否在盘中交易时间或盘后半小时内 (带时区感知)"""
        import datetime
        from zoneinfo import ZoneInfo
        
        if market == 'CN':
            tz = ZoneInfo('Asia/Shanghai')
            now = datetime.datetime.now(tz)
            if now.weekday() >= 5: return False
            time_str = now.strftime("%H:%M")
            return ("09:30" <= time_str <= "11:35") or ("13:00" <= time_str <= "15:30")
            
        elif market == 'HK':
            tz = ZoneInfo('Asia/Hong_Kong')
            now = datetime.datetime.now(tz)
            if now.weekday() >= 5: return False
            time_str = now.strftime("%H:%M")
            return ("09:30" <= time_str <= "12:05") or ("13:00" <= time_str <= "16:30")
            
        elif market == 'US':
            tz = ZoneInfo('America/New_York')
            now = datetime.datetime.now(tz)
            if now.weekday() >= 5: return False
            time_str = now.strftime("%H:%M")
            # 美东常规交易时间: 09:30 - 16:00, 算上盘后放宽到 16:30
            return ("09:30" <= time_str <= "16:30")
                
        return False

    @staticmethod
    def _persist_alert(item: dict, quote: dict):
        """记录预警信息到数据库"""
        from core.db import db_manager
        from domain.ledger.analytics import WatchlistAlert
        
        try:
            alert = WatchlistAlert(
                symbol=item['symbol'],
                name=item['name'],
                market=item['market'],
                alert_reason=f"异动预警: 涨幅 {quote['pct_chg']}%，触发阈值 {item.get('alert_threshold_pct', 5.0)}%",
                price=quote['price'],
                change_pct=quote['pct_chg'],
                status="已触发"
            )
            with db_manager.ledger_session() as session:
                session.add(alert)
                logger.info(f"Persisted WatchlistAlert for {item['symbol']} to ledger DB.")
        except Exception as e:
            logger.error(f"Failed to persist WatchlistAlert: {e}")

    @staticmethod
    def trigger_alert(item: dict, quote: dict):
        """
        Single-message alert: wait for LLM analysis in thread pool, then send one
        complete consolidated message with price info + attribution.
        """
        direction = "📈 暴涨" if quote['pct_chg'] > 0 else "📉 暴跌"
        # Submit to thread pool and send consolidated message when ready
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
        pct = quote['pct_chg']
        price = quote['price']
        
        try:
            # 1. Gather Info (Multi-source parallel search)
            news_context = ""
            
            # Construct comprehensive search queries based on market
            # Avoid too many keywords which confuse search engines like Bocha or Google
            if market == 'US':
                q_specific = f"{symbol} stock news why down up today"
                q_market = f"US stock market today main drivers tech news"
            elif market == 'HK':
                q_specific = f"{symbol} {name} 港股 股价 异动原因 暴涨 暴跌 财报"
                q_market = f"港股 恒生科技 恒指 今日异动 大盘分析"
            else:
                q_specific = f"{symbol} {name} 股票 为什么 涨停 跌停 异动 最新公告"
                q_market = f"A股 沪指 创业板 今日异动 板块 领涨"
                
            logger.info(f"Gathering multi-channel info for {symbol}...")
            
            # Parallel search for both specific and market context
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as search_pool:
                fut_specific = search_pool.submit(Tools.web_search, q_specific)
                fut_market = search_pool.submit(Tools.web_search, q_market)
                
                try:
                    specific_news = fut_specific.result(timeout=25)
                    market_news = fut_market.result(timeout=25)
                except concurrent.futures.TimeoutError:
                    logger.error(f"Search timed out for {symbol}")
                    specific_news = "搜索超时"
                    market_news = "搜索超时"
                
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
                
                # 组装完整单条消息（价格信息 + 归因）
                analysis_text = (
                    f"🚨 **{direction}预警** | {name} ({symbol})\n"
                    f"💰 **现价**: {price}  **幅度**: {pct}%\n"
                    f"─────────────────\n"
                    f"⚡ **核心归因**: {result.reason}\n"
                    f"📈 **置信度**: {result.confidence}\n"
                    f"📝 **摘要**: {result.summary}"
                )
                
            except Exception as llm_e:
                logger.error(f"LLM Structured output failed: {llm_e}")
                fallback_prompt = f"{user_prompt}\n直接给出1句话原因，不要任何其他字。"
                reason_text = simple_prompt(fallback_prompt)
                analysis_text = (
                    f"🚨 **{direction}预警** | {name} ({symbol})\n"
                    f"💰 **现价**: {price}  **幅度**: {pct}%\n"
                    f"─────────────────\n"
                    f"⚡ **核心归因**: {reason_text.strip()}"
                )

            # 3. Send single consolidated report
            Notifier.send_telegram(chat_id, analysis_text)
            
            # 4. Store Event in Vector DB
            try:
                collection = get_collection("market_events")
                
                # Use content hash as ID
                doc_id = f"evt_{datetime.date.today()}_{hashlib.md5(analysis_text.encode()).hexdigest()[:8]}"
                
                # Determine impact based on direction
                impact = "positive" if "涨" in direction else "negative"
                
                # Confidence score: only available when structured LLM succeeded
                score = 0.5
                try:
                    conf = result.confidence
                    if "High" in conf or "高" in conf:
                        score = 0.9
                    elif "Medium" in conf or "中" in conf:
                        score = 0.6
                    elif "Low" in conf or "低" in conf:
                        score = 0.3
                except Exception:
                    pass
                    
                meta = {
                    "event_type": "market",
                    "event_date": str(datetime.date.today()),
                    "impact": impact,
                    "impact_score": score,
                    "source": "monitor_scan",
                    "related_symbols": symbol,
                    "doc_version": 1,
                    "created_at": str(datetime.datetime.utcnow())
                }
                
                # Use full analysis_text as the vector document
                doc_text = f"【异动归因】{name}({symbol}) {direction} {pct}%。\n{analysis_text}"
                
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
