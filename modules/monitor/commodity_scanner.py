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

# 固定的五大板块映射关系
CATEGORY_MAP = {
    "贵金属及有色": ["黄金", "白银", "铜", "铝", "锌", "铅", "镍", "锡", "氧化铝", "工业硅", "国际铜", "铸造铝合金", "铂", "钯"],
    "黑色系及建材": ["螺纹钢", "铁矿石", "焦炭", "焦煤", "玻璃", "纯碱", "尿素", "硅铁", "锰硅", "不锈钢", "热轧卷板", "热卷"],
    "能源化工": ["原油", "上海原油", "燃料油", "低硫燃料油", "PTA", "乙二醇", "甲醇", "聚丙烯", "塑料", "PVC", "苯乙烯", "液化石油气", "LPG", "纯苯", "烧碱", "对二甲苯", "瓶片", "丙烯", "天然橡胶", "20号胶", "丁二烯橡胶", "沥青", "纸浆"],
    "农畜产品": ["豆粕", "豆油", "豆一", "豆二", "大豆", "玉米", "生猪", "原木", "淀粉", "鸡蛋", "菜油", "菜籽", "菜粕", "强麦", "粳稻", "白糖", "棉花", "棉纱", "早籼稻", "晚籼稻", "苹果", "红枣", "花生"],
    "航运及特殊": ["集运指数(欧线)", "集运指数", "碳酸锂", "多晶硅"] # 未识别的新品种也会落入此分类
}

# 纯金融衍生品，直接过滤不看
FINANCIAL_FUTURES = ["沪深300指数期货", "5年期国债期货", "上证50指数期货", "中证500指数期货", "2年期国债期货", "中证指数期货", "10年期国债期货", "30年期国债期货"]

# LLM 解析结构模型: 针对固定板块的归因
class SectorAnalysis(BaseModel):
    theme_name: str = Field(description="板块名称（与输入的板块名称严格一致）")
    catalyst: str = Field(description="结合今日资讯，概括该板块内领涨商品集体异动的核心逻辑及催化剂（中文，高度精简，最多2句话）。")
    beneficiary_stocks: List[str] = Field(description="A股和港股中该板块对应的最核心受益股票（只输出股票中文简称，不要任何代码，如：紫金矿业、中国海油，最多3只）。")

class DailyCommodityAnalysis(BaseModel):
    sectors: List[SectorAnalysis] = Field(description="各个板块的深度归因列表")

class CommodityScanner:
    """
    大宗商品每日战报生成器
    每天盘后截取全市场表现，固定分发至 5 个大类，取各自 Top 3 领涨品种。
    """
    
    @classmethod
    def _determine_category(cls, name: str) -> str:
        """根据商品名称确定所属板块，匹配不到的兜底到'航运及特殊'"""
        for cat, items in CATEGORY_MAP.items():
            for item in items:
                if item in name or name in item:
                    return cat
        return "航运及特殊"

    @staticmethod
    def generate_daily_report():
        logger.info("开始生成大宗商品每日截面战报...")
        
        try:
            # 1. 获取全市场主力合约列表
            df_main = ak.futures_display_main_sina()
            if df_main.empty:
                logger.warning("未能获取主力合约列表。")
                return

            pool = {}
            for _, row in df_main.iterrows():
                sym = str(row.get('symbol', ''))
                name = str(row.get('name', '')).replace('连续', '')
                if len(sym) >= 2 and not any(fin in name for fin in FINANCIAL_FUTURES):
                    pool[sym] = name

            logger.info(f"已加载 {len(pool)} 个大事实物商品主力合约。")

            # 2. 获取每个合约的收盘行情
            # 采用并推获取当前全市场截面，使用 ak.futures_zh_spot
            import pandas as pd
            import time
            
            def _fetch_spot(sym):
                try:
                    # 避免过快请求被新浪拦截
                    time.sleep(0.2)
                    df = ak.futures_zh_spot(symbol=sym, market="CF")
                    if not df.empty:
                        return df
                except:
                    pass
                return None

            logger.info("正在分批拉取各合约行情数据...")
            # 降低并发数，防止被封 IP 导致进程挂起
            symbols = list(pool.keys())
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                spot_dfs = list(executor.map(_fetch_spot, symbols))
            
            # 保留 (sym, df) 配对关系，确保 symbol 代码始终来自原始 pool
            valid_pairs = [(sym, df) for sym, df in zip(symbols, spot_dfs) if df is not None]
            if not valid_pairs:
                logger.warning("所有合约行情数据均为空。")
                return
            logger.info(f"成功获取 {len(valid_pairs)} 个合约的行情。")

            # 3. 数据规整与分路存放
            market_data = {cat: [] for cat in CATEGORY_MAP.keys()}

            for sym, df_spot in valid_pairs:
                name = pool.get(sym, sym)  # 从主力合约列表取中文名
                row = df_spot.iloc[0]      # futures_zh_spot 每个代码返回一行
                
                # 当前价
                price = 0.0
                for price_col in ['current_price', 'last', 'close', '最新价']:
                    if price_col in row.index and row[price_col] not in (None, '', 0):
                        try:
                            price = float(row[price_col])
                            break
                        except (ValueError, TypeError):
                            pass
                
                if price <= 0:
                    continue
                
                # 计算涨跌幅
                pct_chg = 0.0
                ref_price = 0.0
                for ref_col in ['last_settle_price', 'last_close', 'settle', 'pre_settle']:
                    if ref_col in row.index and row[ref_col] not in (None, '', 0):
                        try:
                            ref_price = float(row[ref_col])
                            break
                        except (ValueError, TypeError):
                            pass
                
                if ref_price > 0:
                    pct_chg = round((price - ref_price) / ref_price * 100, 2)
                else:
                    for pct_col in ['change_percent', 'pct_chg', '涨跌幅', 'change_pct']:
                        if pct_col in row.index and row[pct_col] not in (None, ''):
                            try:
                                pct_chg = float(row[pct_col])
                                break
                            except (ValueError, TypeError):
                                pass
                
                # 确定归属篹子
                cat = CommodityScanner._determine_category(name)
                market_data[cat].append({
                    "name": name,
                    "symbol": sym,   # 使用源头的期货代码，不是 df 内的列
                    "price": price,
                    "pct_chg": pct_chg
                })

            # 4. 各板块 Top 3 提取
            top_commodities_by_sector = {}
            flat_top_items = []
            
            for cat, items in market_data.items():
                # 按涨幅降序，只保留涨幅 > 3%（过滤小幅波动）
                positive_items = [i for i in items if i['pct_chg'] > 3.0]
                sorted_items = sorted(positive_items, key=lambda x: x['pct_chg'], reverse=True)
                top_3 = sorted_items[:3]
                
                if top_3:
                    top_commodities_by_sector[cat] = top_3
                    flat_top_items.extend([f"{cat}-{i['name']}(+{i['pct_chg']}%)" for i in top_3])

            if not top_commodities_by_sector:
                logger.info("今日大宗市场全系走跌，无正向异动可推。")
                return

            logger.info("各板块 Top 3 抽取完毕，开始 LLM 归因...")
            
            # 5. LLM 面向固定板块归因分析
            items_desc = ", ".join(flat_top_items)
            query = f"{items_desc} 期货 涨价 原因 宏观分析 股市关联"
            news_context = data_manager.search(query)
            
            sys_prompt = (
                "你是一位顶级的大宗商品宏观分析师。系统已从全市场几百个期货合约中，按 5 大基准板块提取了今日领涨的 Top 3 品种。"
                "请结合传入的即时新闻资讯，对目前发生上涨的板块分别给出极其精简的【宏观催化剂】（如：因红海停发导致运费暴涨），"
                "并指出A股/港股中该板块对应的核心【受益标的】。"
            )
            user_prompt = f"今日领涨板块及品种:\n{items_desc}\n\n相关全网实时资讯:\n{news_context}"

            try:
                analysis: DailyCommodityAnalysis = structured_output(
                    messages=[
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_model=DailyCommodityAnalysis
                )
                analysis_dict = {a.theme_name: a for a in analysis.sectors}
            except Exception as e:
                logger.error(f"大宗商品 LLM 归因失败: {e}")
                analysis_dict = {}

            # 6. 组装每日战报
            today_str = datetime.datetime.now().strftime('%Y-%m-%d')
            report_lines = [f"🔥 **TradeDB 大宗商品市场复盘 (Top 3 领涨榜单)**\n📅 {today_str}\n"]
            
            emoji_map = {
                "贵金属及有色": "🟡",
                "黑色系及建材": "⚫",
                "能源化工": "🛢️",
                "农畜产品": "🟢",
                "航运及特殊": "🚢"
            }

            for cat in CATEGORY_MAP.keys():
                items = top_commodities_by_sector.get(cat)
                if not items:
                    continue
                    
                icon = emoji_map.get(cat, "🔹")
                report_lines.append(f"{icon} **【{cat}】**")
                
                items_str = " | ".join([f"{i['name']}({i['symbol']}) {i['price']} `+{i['pct_chg']}%`" for i in items])
                report_lines.append(items_str)
                
                sector_info = analysis_dict.get(cat)
                if sector_info:
                    report_lines.append(f"⚡ **催化剂**: {sector_info.catalyst}")
                    stocks_str = "、".join(sector_info.beneficiary_stocks) if sector_info.beneficiary_stocks else "暂无"
                    report_lines.append(f"🎯 **受益标的**: {stocks_str}")
                else:
                    report_lines.append("⚡ **催化剂**: (AI分板解析受限)")
                    
                report_lines.append("") # 空行分隔
                
            report = "\n".join(report_lines).strip()
            Notifier.broadcast(report)
            logger.info("大宗商品每日战报推送完成。")

        except Exception as e:
            logger.error(f"大宗战报生成失败: {e}", exc_info=True)

    # 兼容老的单例启动接口，防止其他地方调用报错
    @staticmethod
    def scan_and_alert():
        CommodityScanner.generate_daily_report()
