"""
Commodity Scanner
负责监控大宗商品异动，映射关联股票，并生成交易思路。
所有推送内容均为中文，仅推送正向涨幅，每个品种24小时内只推送一次。
优化：引入LLM提取同类项进行合并群发，移除冗长的交易逻辑，股票推荐只用中文名。
"""
import logging
import datetime
import threading
import concurrent.futures
from typing import List, Dict, Any
from pydantic import BaseModel, Field

from modules.ingestion.data_factory import data_manager
from core.llm import structured_output
from core.db import get_collection
from modules.monitor.notifier import Notifier
import akshare as ak

logger = logging.getLogger(__name__)

# LLM 解析结构模型: 同类项合并推送版
class CommodityGroup(BaseModel):
    theme_name: str = Field(description="这组大宗商品涨价的共同主题或所属板块（例如：能源化工类、有色金属类等）。")
    included_commodities: List[str] = Field(description="属于该主题的商品名称列表。")
    catalyst: str = Field(description="该类商品价格异动的核心催化剂（中文，最多2句话）。")
    beneficiary_stocks: List[str] = Field(description="最可能受益的具体股票中文名称列表（无需代码，只需中文名称，例如：中国石油、紫金矿业）。")

class GroupedAnalysis(BaseModel):
    groups: List[CommodityGroup] = Field(description="提炼出的同类项分组列表。如果是单一商品，本身构成一个分组。")

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
    
    # 触发阈值（仅正向涨幅）。已按用户需求上调至 5.0%
    THRESHOLD_PCT = 5.0
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
            
            triggered_items = []
            
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
                CommodityScanner._mark_alerted(symbol)
                
                triggered_items.append({
                    "name": name,
                    "symbol": symbol,
                    "pct_chg": pct_chg,
                    "price": price
                })
            
            if triggered_items:
                logger.info(f"本次扫描共有 {len(triggered_items)} 个商品触发异动，开始合并分析...")
                from modules.monitor.scanner import analysis_executor
                analysis_executor.submit(
                    CommodityScanner._analyze_and_push_grouped,
                    triggered_items
                )
            else:
                logger.info("大宗商品异动扫描完成，无新增预警。")
                    
        except Exception as e:
            logger.error(f"大宗商品扫描失败: {e}", exc_info=True)

    @staticmethod
    def _analyze_and_push_grouped(items: List[Dict[str, Any]]):
        """
        对一批刚刚触发的大宗商品进行同类项合并和归因，
        精简输出：移除交易逻辑和操作建议，股票标的改用中文名。
        """
        try:
            items_desc = ", ".join([f"{i['name']}(上涨 {i['pct_chg']}%)" for i in items])
            logger.info(f"正在对以下商品进行组团归因分析: {items_desc}")
            
            # 1. 资讯聚合
            query = f"{items_desc} 大宗商品 期货 涨价 原因 最新分析 股市受益标的"
            news_context = data_manager.search(query)
            
            # 2. LLM 分组及精简提取
            sys_prompt = (
                "你是一位专业的大宗商品宏观分析师与股票策略师。"
                "请将提供的若干个发生异动上涨的大宗商品进行同类项合并（将其归入一到多个逻辑关联的主题），"
                "并根据资讯提炼核心催化剂，最后找出A股和港股中对应的最受益股票（只输出股票的中文简称，比如: 紫金矿业、中国石油）。"
                "要求高度精简，直接切入核心，不需要输出操作建议或交易思路字段。"
            )
            user_prompt = f"本次监控到触发异动上涨的商品及幅度: {items_desc}\n\n相关全网实时资讯:\n{news_context}"
            
            analysis: GroupedAnalysis = structured_output(
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_model=GroupedAnalysis
            )
            
            # 3. 组装最终战报
            report_lines = [f"🔥 **大宗商品异动 (合并版)**"]
            
            for grp in analysis.groups:
                report_lines.append(f"\n🏷️ **板块**: {grp.theme_name}")
                
                # 匹配现价和涨幅，容错处理（LLM有可能没填对原名）
                details = []
                for inc_name in grp.included_commodities:
                    # 尝试找出对应的原item
                    match = next((x for x in items if inc_name in x['name'] or x['name'] in inc_name), None)
                    if match:
                        details.append(f"• {match['name']} (现价: {match['price']}, `+{match['pct_chg']}%`)")
                    else:
                        details.append(f"• {inc_name}")
                        
                report_lines.append("\n".join(details))
                report_lines.append(f"\n⚡ **核心催化剂**: {grp.catalyst}")
                
                stocks_str = "、".join(grp.beneficiary_stocks) if grp.beneficiary_stocks else "暂无明确标的"
                report_lines.append(f"🎯 **受益标的**: {stocks_str}")
                report_lines.append("─────────────────")
                
            report = "\n".join(report_lines).strip("\n─")
            Notifier.broadcast(report)
            logger.info("大宗商品合并分析推送完成。")
            
        except Exception as e:
            logger.error(f"大宗商品深度归因合并失败: {e}", exc_info=True)
            # 降级方案：直接推出名称和涨幅
            fallback_msg = "\n".join([f"• {i['name']} 上涨 {i['pct_chg']}% (现价: {i['price']})" for i in items])
            Notifier.broadcast(
                f"⚠️ **大宗商品异动**\n{fallback_msg}\n\n❌ 智能合并分析失败，请手动关注。"
            )
