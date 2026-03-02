"""
Commodity Scanner
负责监控大宗商品异动，映射关联股票，并生成交易思路。
"""
import logging
import datetime
import concurrent.futures
from typing import List, Dict, Any
from pydantic import BaseModel, Field

from modules.ingestion.data_factory import data_manager
from core.llm import structured_output, simple_prompt
from core.db import get_collection
from modules.monitor.notifier import Notifier
import akshare as ak

logger = logging.getLogger(__name__)

# LLM 解析结构模型
class CommodityAttribution(BaseModel):
    catalyst: str = Field(description="The core reason for the commodity price surge (e.g., Supply chain disruption, Macro policy). Max 2 sentences.")
    confidence: str = Field(description="Confidence level: High, Medium, Low.")

class TradingIdea(BaseModel):
    logic: str = Field(description="The trading logic connecting the commodity surge to equities. Max 3 sentences.")
    target_tickers: List[str] = Field(description="List of specific stock symbols/tickers (A-share, HK, US) most likely to benefit/suffer.")
    action: str = Field(description="Specific actionable idea (e.g., Go long upstream miners, short downstream manufacturers).")

class CommodityScanner:
    """
    大宗商品异动监控器
    """
    # 缓存动态获取的主力合约池
    _cached_pool: Dict[str, str] = {}
    _pool_last_update: datetime.date = None
    
    # 触发阈值 (%)
    THRESHOLD_PCT = 2.0
    
    @classmethod
    def _get_active_commodities(cls) -> Dict[str, str]:
        """动态获取当前全市场主力合约列表，按天缓存"""
        today = datetime.date.today()
        if cls._pool_last_update == today and cls._cached_pool:
            return cls._cached_pool
            
        try:
            # 获取新浪全市场主力连续合约
            df_main = ak.futures_display_main_sina()
            if not df_main.empty:
                pool = {}
                for _, row in df_main.iterrows():
                    sym = str(row['symbol'])
                    name = str(row['name']).replace('连续', '') # 清洗后缀
                    # 过滤掉一些非主流或不活跃的品种，为了演示我们尽量保留大部分有交易量的
                    if len(sym) >= 2:
                        pool[sym] = name
                
                cls._cached_pool = pool
                cls._pool_last_update = today
                logger.info(f"Dynamically loaded {len(pool)} active main commodity contracts.")
                return pool
        except Exception as e:
            logger.error(f"Failed to fetch active commodities dynamically: {e}")
            
        return cls._cached_pool # default to whatever is cached if failed
    
    @staticmethod
    def scan_and_alert():
        logger.info("Starting Commodity Anomaly Scan...")
        
        try:
            pool = CommodityScanner._get_active_commodities()
            if not pool:
                logger.warning("No commodities to scan (pool is empty).")
                return
                
            # 分批查询，因为新浪接口一次性查询太多可能报错
            symbols = list(pool.keys())
            batch_size = 30
            
            all_dfs = []
            for i in range(0, len(symbols), batch_size):
                batch_syms = symbols[i:i+batch_size]
                try:
                    df = ak.futures_zh_spot(
                        symbol=",".join(batch_syms), 
                        market="CF"
                    )
                    if not df.empty:
                        all_dfs.append(df)
                except Exception as e:
                    logger.debug(f"Batch fetch failed ({e}), falling back to individual symbol fetching.")
                    for sym in batch_syms:
                        try:
                            single_df = ak.futures_zh_spot(symbol=sym, market="CF")
                            if not single_df.empty:
                                all_dfs.append(single_df)
                        except Exception:
                            # 忽略不支持的某些特殊合约（如股指期货）
                            pass
            
            if not all_dfs:
                logger.warning("Commodity scan returned empty data across all batches.")
                return
                
            import pandas as pd
            df_all = pd.concat(all_dfs, ignore_index=True)
            
            for _, row in df_all.iterrows():
                symbol = row['symbol']
                name = pool.get(symbol, symbol)
                price = float(row.get('current_price', 0))
                # 涨跌幅有时不同接口列名不同，假设这里有 change_pct
                # 新浪现货通常有 'change_percent'
                pct_chg = 0.0
                if 'change_percent' in row:
                    pct_chg = float(row['change_percent'])
                elif '涨跌幅' in row:
                    pct_chg = float(row['涨跌幅'])
                    
                if abs(pct_chg) >= CommodityScanner.THRESHOLD_PCT:
                    direction = "暴涨" if pct_chg > 0 else "暴跌"
                    logger.info(f"Commodity Alert Triggered: {name} ({symbol}) {direction} {pct_chg}%")
                    
                    # 触发深度归因与映射
                    CommodityScanner._process_anomaly(name, symbol, pct_chg, direction, price)
                    
        except Exception as e:
            logger.error(f"Commodity scan failed: {e}")

    @staticmethod
    def _process_anomaly(name: str, symbol: str, pct_chg: float, direction: str, price: float):
        """
        处理单点异动：新闻聚合 -> 双轨映射 -> 交易思路合成
        """
        # Phase 1: Notify initial alert
        alert_msg = (
            f"⚠️ **大宗商品异动预警**\n"
            f"**品种**: {name} ({symbol})\n"
            f"**幅度**: {direction} {pct_chg}%\n"
            f"**现价**: {price}\n"
            f"⏳ 正在启动全网归因与双轨产业链寻找映射标的..."
        )
        Notifier.broadcast(alert_msg)
        
        # Phase 2: Async Deep Analysis
        from modules.monitor.scanner import analysis_executor
        analysis_executor.submit(
            CommodityScanner._analyze_and_map, 
            name, pct_chg, direction
        )
        
    @staticmethod
    def _analyze_and_map(name: str, pct_chg: float, direction: str):
        try:
            # 1. 资讯聚合 (使用新的 DataManager)
            query = f"{name} 大宗商品 期货 {direction} 原因 最新分析"
            news_context = data_manager.search(query)
            
            # 2. LLM 初步归因
            sys_prompt_attr = "你是一位大宗商品宏观分析师。根据提供的资讯，判断商品异动的核心催化剂。"
            user_prompt_attr = f"品种: {name} {direction} {pct_chg}%\n资讯:\n{news_context}"
            
            attribution: CommodityAttribution = structured_output(
                messages=[
                    {"role": "system", "content": sys_prompt_attr},
                    {"role": "user", "content": user_prompt_attr}
                ],
                response_model=CommodityAttribution
            )
            
            # 3. Two-Pronged Mapping (双轨映射)
            
            # Path A: Local DB Search (Chroma)
            local_mapping_context = ""
            try:
                # Assuming profile_search or generic DB search exists
                from core.agent import Tools
                local_mapping_context = Tools.database_search(name)
            except Exception as e:
                logger.warning(f"Local ChromaDB mapping failed: {e}")
                local_mapping_context = "本地知识库检索失败或无关联数据。"
                
            # Path B: Live LLM Targeted Web Search
            # LLM generates a targeted search query for stocks based on the catalyst
            live_search_query = simple_prompt(
                prompt=f"大宗商品 {name} 因 '{attribution.catalyst}' 大涨。请生成一个精确的 Google 搜索词，用来寻找 A股、港股或美股中最受益的具体上市公司名单（只需返回搜索词本身，不要加引号或多余文字）：",
                temperature=0.3
            ).strip()
            
            live_mapping_context = data_manager.search(live_search_query)
            
            # 4. Synthesize Trading Idea
            sys_prompt_idea = (
                "你是一个顶尖的量化对冲基金经理，精通大宗周期与权益市场的联动。\n"
                "综合『本地静态产业链』与『全网实时搜索结果』，给出一套可直接交易的股票标的列表。\n"
                "必须输出具体的股票Ticker(如 601899.SH，OXY)。如果找不到明确股票，请在逻辑里说明。"
            )
            
            user_prompt_idea = f"""
            事件: {name} {direction} {pct_chg}%
            核心原因: {attribution.catalyst}
            
            【Path A: 本地静态产业链映射】: 
            {local_mapping_context}
            
            【Path B: 全网最新受益股舆情】 (来自查询 '{live_search_query}'): 
            {live_mapping_context}
            """
            
            trading_idea: TradingIdea = structured_output(
                messages=[
                    {"role": "system", "content": sys_prompt_idea},
                    {"role": "user", "content": user_prompt_idea}
                ],
                response_model=TradingIdea
            )
            
            # 5. Format and Send Final Report
            report = (
                f"⛓️‍💥 **大宗商品映射策略: {name}**\n\n"
                f"🔥 **催化剂 (置信度:{attribution.confidence})**:\n{attribution.catalyst}\n\n"
                f"🧠 **交易逻辑**:\n{trading_idea.logic}\n\n"
                f"🎯 **标的池** (交叉验证): \n`" + "`, `".join(trading_idea.target_tickers) + "`\n\n"
                f"🎬 **主理人建议**: {trading_idea.action}"
            )
            
            Notifier.broadcast(report)
            
        except Exception as e:
            logger.error(f"Commodity deep mapping failed for {name}: {e}")
            Notifier.broadcast(f"❌ {name} 大宗商品深度归因失败: {e}")
