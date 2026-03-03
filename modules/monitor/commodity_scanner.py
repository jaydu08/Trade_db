"""
Commodity Scanner
负责监控大宗商品异动，映射关联股票，并生成交易思路。
所有推送内容均为中文，仅推送正向涨幅，每个品种24小时内只推送一次。
"""
import logging
import datetime
import threading
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
    catalyst: str = Field(description="商品价格异动的核心催化剂（中文，最多2句话）。")
    confidence: str = Field(description="置信度：高、中、低。")

class TradingIdea(BaseModel):
    logic: str = Field(description="将商品涨价与权益市场联动的交易逻辑（中文，最多3句话）。")
    target_tickers: List[str] = Field(description="最可能受益的具体股票代码列表（A股/港股/美股）。")
    action: str = Field(description="具体可操作的交易思路（中文）。")

class CommodityScanner:
    """
    大宗商品异动监控器
    """
    # 类级别 24h 去重字典：{symbol: last_alert_datetime}
    _alert_history: Dict[str, datetime.datetime] = {}
    _alert_history_lock = threading.Lock()
    
    # 主力合约池缓存（按天刷新）
    _cached_pool: Dict[str, str] = {}
    _pool_last_update: datetime.date = None
    
    # 触发阈值（仅正向涨幅）
    THRESHOLD_PCT = 2.0
    # 24小时冷却时间
    COOLDOWN_HOURS = 24

    @classmethod
    def _get_active_commodities(cls) -> Dict[str, str]:
        """动态获取当前全市场主力合约列表，按天缓存"""
        today = datetime.date.today()
        if cls._pool_last_update == today and cls._cached_pool:
            return cls._cached_pool
            
        try:
            df_main = ak.futures_display_main_sina()
            if not df_main.empty:
                pool = {}
                for _, row in df_main.iterrows():
                    sym = str(row['symbol'])
                    name = str(row['name']).replace('连续', '')
                    if len(sym) >= 2:
                        pool[sym] = name
                
                cls._cached_pool = pool
                cls._pool_last_update = today
                logger.info(f"已加载 {len(pool)} 个大宗商品主力合约。")
                return pool
        except Exception as e:
            logger.error(f"获取主力合约列表失败: {e}")
            
        return cls._cached_pool

    @classmethod
    def _is_cooldown_active(cls, symbol: str) -> bool:
        """检查该品种是否在24小时冷却期内"""
        with cls._alert_history_lock:
            last = cls._alert_history.get(symbol)
            if last is None:
                return False
            elapsed = (datetime.datetime.now() - last).total_seconds() / 3600
            return elapsed < cls.COOLDOWN_HOURS

    @classmethod
    def _mark_alerted(cls, symbol: str):
        """记录该品种的最近推送时间"""
        with cls._alert_history_lock:
            cls._alert_history[symbol] = datetime.datetime.now()

    @staticmethod
    def scan_and_alert():
        logger.info("开始大宗商品异动扫描...")
        
        try:
            pool = CommodityScanner._get_active_commodities()
            if not pool:
                logger.warning("商品合约池为空，跳过扫描。")
                return
            
            logger.info(f"扫描 {len(pool)} 个大宗商品合约...")
            
            import pandas as pd
            all_dfs = []
            
            for sym in list(pool.keys()):
                try:
                    df = ak.futures_zh_spot(symbol=sym, market="CF")
                    if not df.empty:
                        all_dfs.append(df)
                except Exception:
                    pass
            
            if not all_dfs:
                logger.warning("所有合约行情数据均为空。")
                return
                
            df_all = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"已获取 {len(df_all)} 条大宗商品行情。")
            
            triggered = 0
            for _, row in df_all.iterrows():
                symbol = str(row.get('symbol', ''))
                name = pool.get(symbol, symbol)
                
                # 当前价
                price = 0.0
                for price_col in ['current_price', 'last', 'close', '最新价']:
                    if price_col in row and row[price_col] not in (None, '', 0):
                        try:
                            price = float(row[price_col])
                            break
                        except (ValueError, TypeError):
                            pass
                
                if price <= 0:
                    continue
                
                # 计算涨跌幅（基于昨结算价）
                pct_chg = 0.0
                ref_price = 0.0
                for ref_col in ['last_settle_price', 'last_close', 'settle', 'pre_settle']:
                    if ref_col in row and row[ref_col] not in (None, '', 0):
                        try:
                            ref_price = float(row[ref_col])
                            break
                        except (ValueError, TypeError):
                            pass
                
                if ref_price > 0:
                    pct_chg = round((price - ref_price) / ref_price * 100, 2)
                else:
                    for pct_col in ['change_percent', 'pct_chg', '涨跌幅', 'change_pct']:
                        if pct_col in row and row[pct_col] not in (None, ''):
                            try:
                                pct_chg = float(row[pct_col])
                                break
                            except (ValueError, TypeError):
                                pass
                
                # 仅推送正向涨幅
                if pct_chg < CommodityScanner.THRESHOLD_PCT:
                    continue
                
                # 24小时去重
                if CommodityScanner._is_cooldown_active(symbol):
                    logger.debug(f"{name}({symbol}) 处于24h冷却期，跳过推送。")
                    continue
                
                logger.info(f"大宗商品异动触发: {name}({symbol}) 上涨 {pct_chg}% (现价={price}, 参考={ref_price})")
                triggered += 1
                CommodityScanner._mark_alerted(symbol)
                
                # 异步执行深度归因+产业链分析（合并为单条推送）
                from modules.monitor.scanner import analysis_executor
                analysis_executor.submit(
                    CommodityScanner._analyze_and_push,
                    name, symbol, pct_chg, price
                )
            
            logger.info(f"大宗商品异动扫描完成，共触发 {triggered} 个异动预警。")
                    
        except Exception as e:
            logger.error(f"大宗商品扫描失败: {e}", exc_info=True)

    @staticmethod
    def _analyze_and_push(name: str, symbol: str, pct_chg: float, price: float):
        """
        完成深度归因+产业链映射，合并为单条中文消息推送。
        不发第一条预告消息，归因完成后一次性推送完整报告。
        """
        try:
            # 1. 资讯聚合
            query = f"{name} 大宗商品 期货 涨价 原因 最新分析"
            news_context = data_manager.search(query)
            
            # 2. LLM 归因分析
            sys_prompt_attr = (
                "你是一位专业的大宗商品宏观分析师。"
                "根据提供的资讯，用中文判断商品异动的核心催化剂，输出JSON格式。"
            )
            user_prompt_attr = f"品种: {name} 今日上涨 {pct_chg}%\n资讯:\n{news_context}"
            
            attribution: CommodityAttribution = structured_output(
                messages=[
                    {"role": "system", "content": sys_prompt_attr},
                    {"role": "user", "content": user_prompt_attr}
                ],
                response_model=CommodityAttribution
            )
            
            # 3. 双轨产业链映射
            
            # 路径A：本地 ChromaDB 静态产业链
            local_mapping_context = ""
            try:
                from core.agent import Tools
                local_mapping_context = Tools.database_search(name)
            except Exception as e:
                logger.warning(f"本地产业链检索失败: {e}")
                local_mapping_context = "本地知识库无关联数据。"
            
            # 路径B：LLM生成精准搜索词 + 实时全网搜索
            live_search_query = simple_prompt(
                prompt=(
                    f"大宗商品 {name} 因 「{attribution.catalyst}」 上涨。"
                    f"请生成一个精确的中文搜索词，用来在A股、港股、美股中寻找最受益的具体上市公司。"
                    f"只返回搜索词本身，不要任何解释。"
                ),
                temperature=0.3
            ).strip()
            
            live_mapping_context = data_manager.search(live_search_query)
            
            # 4. 合成交易思路
            sys_prompt_idea = (
                "你是顶尖量化基金经理，精通大宗商品与权益市场联动。"
                "综合静态产业链和实时舆情，给出可直接操作的股票标的。"
                "必须使用中文，标的代码格式如：600519.SH、00700.HK、AAPL。"
                "如找不到明确标的，在逻辑中说明原因。"
                "输出JSON格式。"
            )
            user_prompt_idea = f"""
事件：{name} 今日上涨 {pct_chg}%
核心原因：{attribution.catalyst}

【静态产业链映射】：
{local_mapping_context}

【实时受益股（搜索词：{live_search_query}）】：
{live_mapping_context}
"""
            trading_idea: TradingIdea = structured_output(
                messages=[
                    {"role": "system", "content": sys_prompt_idea},
                    {"role": "user", "content": user_prompt_idea}
                ],
                response_model=TradingIdea
            )
            
            # 5. 组装成单条完整中文消息
            tickers_str = "、".join(trading_idea.target_tickers) if trading_idea.target_tickers else "暂无明确标的"
            report = (
                f"🔥 **大宗商品异动** | {name}\n"
                f"💰 现价 {price}  上涨 **{pct_chg}%**\n"
                f"─────────────────\n"
                f"⚡ **归因**（置信度：{attribution.confidence}）\n"
                f"{attribution.catalyst}\n\n"
                f"🔗 **交易逻辑**\n"
                f"{trading_idea.logic}\n\n"
                f"🎯 **受益标的**\n"
                f"{tickers_str}\n\n"
                f"📌 **操作建议**：{trading_idea.action}"
            )
            
            Notifier.broadcast(report)
            logger.info(f"{name} 大宗商品分析推送完成。")
            
        except Exception as e:
            logger.error(f"{name} 大宗商品深度归因失败: {e}")
            Notifier.broadcast(
                f"⚠️ **大宗商品异动** | {name} 上涨 {pct_chg}%（现价 {price}）\n"
                f"❌ 深度分析失败，请手动关注。"
            )
