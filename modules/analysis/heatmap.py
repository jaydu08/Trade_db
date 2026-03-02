import logging
import datetime
import concurrent.futures
from typing import List, Dict

import pandas as pd
import numpy as np

from modules.ingestion.akshare_client import akshare_client
from core.llm import simple_prompt
from core.agent import Tools
from modules.monitor.notifier import Notifier

logger = logging.getLogger(__name__)

class MarketHeatMap:
    """
    市场热度榜单服务
    获取各市场涨幅靠前且活跃的个股，并使用 LLM 总结其上涨原因。
    """
    def __init__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def _get_news_and_reason(self, symbol: str, name: str, pct_chg: float, market: str) -> str:
        """获取个股最新消息，并交给 LLM 极简归因"""
        try:
            # 搜索新闻
            query = f"{symbol} {name} stock news latest" if market == 'US' else f"{symbol} {name} 最新消息"
            news_context = Tools.web_search(query)

            # LLM 归因
            prompt = f"""
            标的：{name} ({symbol})
            状态：今日上涨 {pct_chg}%
            【新闻情报】
            {news_context}
            
            任务：
            请用 1-2 句话极其精简地概括该股票今天上涨的原因。例如："【低空经济活跃】多地出台飞行汽车政策支持。"
            如果找不到任何新闻，请回复："未找到明显新闻催化，或受资金及板块面影响。"
            """
            
            reason = simple_prompt(prompt, temperature=0.1)
            # 清理可能的 markdown 格式
            reason = reason.replace("```json", "").replace("```", "").strip()
            return reason
        except Exception as e:
            logger.warning(f"Failed to get reason for {symbol}: {e}")
            return "分析原因失败"

    def _generate_heatmap(self, df: pd.DataFrame, market: str, top_n: int = 10, min_amount: float = 50000000) -> List[Dict]:
        """从行情 DataFrame 中选出榜单"""
        if df.empty:
            return []
            
        columns_map = {
            "代码": "symbol",
            "名称": "name",
            "中文名称": "name",
            "最新价": "price",
            "涨跌幅": "pct_chg",
            "换手率": "turnover",
            "成交额": "amount"
        }
        
        # 兼容不同市场的字段名差异
        renamed_df = df.rename(columns=columns_map)
        
        required_cols = ["symbol", "name", "price", "pct_chg", "amount"]
        for col in required_cols:
            if col not in renamed_df.columns:
                logger.error(f"Missing column '{col}' in '{market}' data.")
                return []
                
        # 换手率列可能不存在(部分美股)，容错处理
        if "turnover" not in renamed_df.columns:
            renamed_df["turnover"] = 0.0

        # 数据清洗: 过滤空值和非数字
        renamed_df = renamed_df.dropna(subset=required_cols)
        renamed_df["pct_chg"] = pd.to_numeric(renamed_df["pct_chg"], errors='coerce').fillna(0)
        renamed_df["amount"] = pd.to_numeric(renamed_df["amount"], errors='coerce').fillna(0)
        renamed_df["turnover"] = pd.to_numeric(renamed_df["turnover"], errors='coerce').fillna(0)

        # 过滤成交额太小的仙股 (默认 5000 万)
        filtered = renamed_df[renamed_df["amount"] >= min_amount].copy()
        
        if market == 'CN':
            # 基础门槛：只看涨幅 >= 3% 的
            filtered = filtered[filtered["pct_chg"] >= 3.0].copy()
            
            # 判断板块涨跌幅限制
            # 沪深主板: 10%, 创业板(30开头)/科创板(68开头): 20%, 北交所(8或4开头): 30%
            limits = pd.Series(10.0, index=filtered.index)
            limits[filtered['symbol'].str.startswith(('30', '68'))] = 20.0
            limits[filtered['symbol'].str.startswith(('8', '4'))] = 30.0
            
            # 过滤一字板 (缩量涨停)
            # 根据各自的涨停限制计算是否为一字板 (接近涨停且换手极低)
            is_yizi = (filtered["pct_chg"] >= (limits - 0.2)) & (filtered["turnover"] < 1.0)
            
            # 归一化涨跌幅：让 10% 的主板涨停 和 20% 的创业板涨停 在完全相同的起跑线比较
            normalized_pct = filtered["pct_chg"] / limits
            
            # 我们给一字板直接大降权，把它挤出榜单
            # 核心算法：异动分数 = 归一化涨幅 * log(成交额/千万) * 换手率
            filtered["heat_score"] = np.where(
                is_yizi,
                0, 
                normalized_pct * np.log1p(filtered["amount"] / 10000000) * filtered["turnover"]
            )
            sorted_df = filtered.sort_values(by="heat_score", ascending=False)
        else:
            # HK / US
            filtered = filtered[filtered["pct_chg"] >= 5.0].copy()
            
            # 如果没有换手率，只看涨幅和成交额的乘数
            if filtered["turnover"].sum() == 0:
                filtered["heat_score"] = filtered["pct_chg"] * np.log1p(filtered["amount"] / 1000000)
            else:
                filtered["heat_score"] = filtered["pct_chg"] * np.log1p(filtered["amount"] / 1000000) * filtered["turnover"]
                
            sorted_df = filtered.sort_values(by="heat_score", ascending=False)
            
        top_stocks = sorted_df.head(top_n).to_dict(orient="records")
        return top_stocks

    def process_and_notify(self, market: str):
        """主入口：获取数据、计算热榜、获取归因、发送通知"""
        logger.info(f"Generating market heat map for {market}...")
        
        # 1. 获取行情
        df = pd.DataFrame()
        try:
            if market == 'CN':
                # 直接使用东方财富A股实时行情接口（原 get_realtime_quotes 已废弃，始终返回空）
                import akshare as ak
                df = ak.stock_zh_a_spot_em()
            elif market == 'HK':
                df = akshare_client.get_stock_info_hk()
            elif market == 'US':
                df = akshare_client.get_stock_info_us()
        except Exception as e:
            logger.error(f"Failed to fetch market data for {market}: {e}")
            return
            
        # 2. 生成榜单
        # HK 和 US 市场的名义成交额计价单位不同，适当调整 min_amount
        min_amt = 50000000 
        n = 10
        if market == 'US': 
            min_amt = 10000000 # 美元
        elif market == 'HK':
            n = 5
        
        top_stocks = self._generate_heatmap(df, market, top_n=n, min_amount=min_amt)
        
        if not top_stocks:
            logger.warning(f"No stocks found for {market} heat map.")
            return

        # 3. 并发获取归因
        futures = {}
        for stock in top_stocks:
            f = self.executor.submit(
                self._get_news_and_reason, 
                stock['symbol'], 
                stock['name'], 
                stock['pct_chg'], 
                market
            )
            futures[f] = stock
            
        results = []
        for f in concurrent.futures.as_completed(futures):
            stock = futures[f]
            reason = f.result()
            stock['reason'] = reason
            results.append(stock)
            
        # 还原回按涨幅排序 (因为 as_completed 不保证顺序)
        results.sort(key=lambda x: x['pct_chg'], reverse=True)

        # 4. 组装消息并发送
        market_names = {'CN': 'A股', 'HK': '港股', 'US': '美股'}
        msg_lines = [f"🔥 **{market_names.get(market, market)} 盘后热门榜单 (Top 10)**"]
        msg_lines.append(f"📅 日期: {datetime.date.today()}\n")
        
        for i, stock in enumerate(results, 1):
            name = stock['name']
            symbol = stock['symbol']
            pct = stock['pct_chg']
            reason = stock['reason']
            msg_lines.append(f"**{i}. {name} ({symbol})**  `+{pct:.2f}%`")
            msg_lines.append(f"💡 {reason}\n")
            
        final_msg = "\n".join(msg_lines)
        
        try:
            Notifier.broadcast(final_msg)
            logger.info(f"Broadcasted heat map for {market}")
        except Exception as e:
            logger.error(f"Failed to broadcast heat map: {e}")

heatmap_service = MarketHeatMap()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    heatmap_service.process_and_notify("CN")
