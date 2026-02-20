import logging
import datetime
import json
from pathlib import Path
from modules.ingestion.akshare_client import akshare_client
from interface.telegram_bot import bot_instance

logger = logging.getLogger(__name__)

# 数据文件路径 (Same as Manager)
DATA_DIR = Path("/root/Trade_db/data")
WATCHLIST_FILE = DATA_DIR / "watchlist.json"

class MonitorService:
    """
    异动监控服务 (JSON Version)
    """
    
    @staticmethod
    def _load_data() -> dict:
        if not WATCHLIST_FILE.exists(): return {}
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}

    @staticmethod
    def _save_data(data: dict):
        try:
            with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except:
            pass

    @staticmethod
    def scan_and_alert():
        """
        扫描所有监控股票，触发报警
        """
        logger.info("Scanning watchlist for anomalies (JSON mode)...")
        
        data = MonitorService._load_data()
        if not data: return

        # Loop items
        for symbol, item in data.items():
            if not item.get('is_active', True): continue
            
            try:
                market = item['market']
                
                # 2. Get Quote
                quote = akshare_client.get_realtime_quote_eastmoney(symbol, market)
                if not quote: 
                    # logger.warning(f"No quote for {symbol}")
                    continue
                
                price = quote.get('price')
                pct_chg = quote.get('pct_chg') # Float, e.g. 5.23
                
                if not pct_chg: continue
                
                # 3. Check Threshold
                threshold = item.get('alert_threshold_pct', 5.0)
                if abs(pct_chg) >= threshold:
                    # 4. Check Cooldown (Daily)
                    today = str(datetime.date.today())
                    last_alert_str = item.get('last_alert_at')
                    last_alert = last_alert_str.split()[0] if last_alert_str else None
                    
                    if last_alert != today:
                        # TRIGGER ALERT!
                        MonitorService.trigger_alert(item, quote)
                        
                        # Update JSON
                        item['last_alert_at'] = str(datetime.datetime.now())
                        data[symbol] = item
                        MonitorService._save_data(data)
                        
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")

    @staticmethod
    def trigger_alert(item: dict, quote: dict):
        """
        触发报警 + 归因分析
        """
        symbol = item['symbol']
        name = item['name']
        pct = quote['pct_chg']
        price = quote['price']
        
        direction = "📈 暴涨" if pct > 0 else "📉 暴跌"
        
        # 1. Gather News (Dual Source)
        news_context = ""
        try:
            # Source A: Akshare News (Eastmoney) - Primary Source
            from modules.ingestion.akshare_client import akshare_client
            # market needs to be converted? CN/HK/US.
            # stock_news_em takes symbol.
            # US stocks might not work well with stock_news_em, let's try
            # For US, maybe 'GOOG' works.
            
            # Note: akshare_client.get_latest_news is for general news.
            # We need specific stock news.
            # Let's try to fetch it directly if akshare_client doesn't have it.
            import akshare as ak
            stock_news = ak.stock_news_em(symbol=symbol)
            if not stock_news.empty:
                # Take top 5 news
                top_news = stock_news.head(5)
                news_str = ""
                for _, row in top_news.iterrows():
                    news_str += f"- {row['发布时间']} {row['新闻标题']}: {row['新闻内容'][:50]}...\n"
                news_context += f"【个股公告/新闻】:\n{news_str}\n\n"
        except Exception as e:
            logger.warning(f"Akshare news failed: {e}")

        try:
            # Source B: Web Search (via Agent Tools) - Secondary Source
            from core.agent import Tools
            search_query = f"{name} {symbol} 今日 重大新闻" # Simplify query
            web_news = Tools.web_search(search_query)
            news_context += f"【全网搜索】:\n{web_news}\n\n"
        except Exception as e:
            news_context += f"搜索失败: {e}\n"

        # 2. LLM Analysis
        from core.agent import agent_executor
        
        prompt = f"""
        【紧急任务】
        标的：{name} ({symbol})
        状态：今日{direction} {pct}%，现价 {price}。
        
        【已知情报】
        {news_context}
        
        【要求】
        1. 根据上述情报，分析股价异动的**真实原因**。
        2. **严禁编造**财报、分红等未在情报中出现的信息。如果情报中没有相关信息，请直接说“暂未搜到明确驱动因素”。
        3. 重点关注：新产品发布（如Gemini 3.1）、政策变化、大额订单等。
        4. 简明扼要，列出核心驱动力。
        """
        
        analysis = agent_executor.run(prompt)
        
        final_msg = f"🚨 **{direction}预警**\n" \
                    f"**标的**: {name} ({symbol})\n" \
                    f"**幅度**: {pct}%\n" \
                    f"**现价**: {price}\n\n" \
                    f"🧐 **智能归因**:\n{analysis}"
                    
        # Send to Telegram (Use bot_instance directly)
        logger.info(f"Final Alert:\n{final_msg}")
        
        target_chat_id = item.get('chat_id')
        if not target_chat_id:
            from config.settings import TELEGRAM_ADMIN_ID
            target_chat_id = TELEGRAM_ADMIN_ID
             
        if not target_chat_id:
            from interface.telegram_bot import LAST_CHAT_ID
            target_chat_id = LAST_CHAT_ID
        
        if target_chat_id and bot_instance and bot_instance.app:
            import asyncio
            try:
                # Create a new event loop for this thread if needed
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                logger.info(f"Sending alert via bot_instance to {target_chat_id}...")
                
                # Use bot.send_message directly
                # No parse_mode to be safe
                loop.run_until_complete(
                    bot_instance.app.bot.send_message(
                        chat_id=target_chat_id, 
                        text=final_msg
                    )
                )
                logger.info("Alert sent successfully via bot_instance.")
                
            except Exception as e:
                logger.error(f"Failed to send telegram via bot_instance: {e}")