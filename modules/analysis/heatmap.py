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
            # ────────────────────────────────────────────────────────────────
            # A股热榜算法：百分位排名归一化综合评分
            #
            # 最低门槛：涨幅 >= 5%（过滤北方国际之类平庸涨幅股）
            # 对于不同板块归一化涨幅：
            #   主板 → 除以10, 创业板/科创 → 除以20, 北交所 → 除以30
            # 评分 = 涨幅百分位(0.5) + 成交额百分位(0.3) + 换手率百分位(0.2)
            # 百分位归一化让三个指标量纲统一，避免成交额绝对值数量级碾压涨幅
            # ────────────────────────────────────────────────────────────────

            # 各板块涨停限制（用于归一化涨幅）
            limits = pd.Series(10.0, index=filtered.index)
            limits[filtered['symbol'].str.startswith(('30', '688'))] = 20.0
            limits[filtered['symbol'].str.startswith(('8', '4'))] = 30.0

            # 最低涨幅门槛 5%（防止平庸股入榜）
            filtered = filtered[filtered["pct_chg"] >= 5.0].copy()
            limits = limits.loc[filtered.index]

            if filtered.empty:
                # 弱市降级：放宽到 >= 3%
                logger.warning("CN: 涨幅>=5%无满足条件股票，降级展示>=3%涨幅股票")
                filtered = renamed_df[
                    (renamed_df["amount"] >= min_amount) & (renamed_df["pct_chg"] >= 3.0)
                ].copy()
                limits = pd.Series(10.0, index=filtered.index)
                limits[filtered['symbol'].str.startswith(('30', '688'))] = 20.0
                limits[filtered['symbol'].str.startswith(('8', '4'))] = 30.0

            filtered = filtered[filtered["amount"] >= min_amount].copy()
            limits = limits.loc[filtered.index]

            if filtered.empty:
                logger.warning("CN: 最终候选池为空，跳过热榜")
                return []

            # 归一化涨幅（把主板10%涨停 与 创业板20%涨停 等价视为1.0）
            normalized_pct = (filtered["pct_chg"] / limits)
            # 抹平涨停板之间的微小差价 (例如 19.98%, 19.99%, 10.03%)
            # 将接近甚至略微超过涨停价 (0.96~1.05倍) 的标的，归一化强度全部强制锁定为 1.0
            # 这样它们的 rank_pct 得分完全一致，最终的龙虎榜排序决定权将完美交还给 成交额 和 换手率
            mask = (normalized_pct >= 0.96) & (normalized_pct <= 1.05)
            normalized_pct.loc[mask] = 1.0
            normalized_pct = normalized_pct.clip(0, 1.2)

            # 百分位排名（pct_rank → 0~1，越大越靠前）
            rank_pct     = normalized_pct.rank(pct=True)
            rank_amount  = filtered["amount"].rank(pct=True)
            has_turnover = filtered["turnover"].sum() > 0
            rank_turnover = filtered["turnover"].rank(pct=True) if has_turnover else pd.Series(0.5, index=filtered.index)

            # 综合评分：涨幅50% + 成交额30% + 换手20%
            filtered = filtered.copy()
            filtered["heat_score"] = (
                rank_pct     * 0.50 +
                rank_amount  * 0.30 +
                rank_turnover * 0.20
            )

            sorted_df = filtered.sort_values(by="heat_score", ascending=False)
            logger.info(f"CN热榜: 候选 {len(filtered)} 只，最高涨幅 {filtered['pct_chg'].max():.2f}%, 最低 {filtered['pct_chg'].min():.2f}%")
        else:
            # HK / US：同样改用百分位归一化
            filtered = filtered[filtered["pct_chg"] >= 5.0].copy()

            if filtered.empty:
                return []

            # (已移除) 旧版通过价格和成交额代理市值的硬性过滤
            # 现在改为两段式：先取 Top 50，再调用 Finnhub API 判断真实市值 >= 100M

            rank_pct    = filtered["pct_chg"].rank(pct=True)
            rank_amount = filtered["amount"].rank(pct=True)
            has_turnover = filtered["turnover"].sum() > 0
            rank_turnover = filtered["turnover"].rank(pct=True) if has_turnover else pd.Series(0.5, index=filtered.index)

            filtered = filtered.copy()
            filtered["heat_score"] = (
                rank_pct    * 0.50 +
                rank_amount * 0.30 +
                rank_turnover * 0.20
            )
            sorted_df = filtered.sort_values(by="heat_score", ascending=False)

            # 美股特定：去除权证类 + 同底层杠杆ETF去重（留成交额最大的一个）
            if market == 'US':
                import re as _re
                def _us_key(row):
                    name = str(row.get('name', ''))
                    sym  = str(row.get('symbol', '')).split('.')[-1]
                    # 权证/Rights → 完全排除（返回 None）
                    if _re.search(r'(?i)\b(wt|warrant|rights|rts|units?)\b', name):
                        return None
                    # 杠杆ETF：提取底层ticker归为同组
                    m = _re.search(r'\d[\d.]*[Xx]\s+(?:Long\s+|Short\s+)?([A-Z]{2,6})', name)
                    if m:
                        return 'LETF_' + m.group(1)
                    m2 = _re.search(r'(?:T-Rex|Defiance|ProShares|GraniteShares|Direxion)\s+.*?([A-Z]{2,6})(?:\s|$)', name)
                    if m2:
                        return 'LETF_' + m2.group(1)
                    return sym  # 普通股不合并

                sorted_df = sorted_df.copy()
                sorted_df['_key'] = sorted_df.apply(_us_key, axis=1)
                sorted_df = (
                    sorted_df[sorted_df['_key'].notna()]      # 去除权证
                    .drop_duplicates(subset=['_key'])          # 杠杆ETF只留成交额最大
                    .drop(columns=['_key'])
                )

        # 先取初筛 Top 50（多取一些供下游过滤）
        candidates = sorted_df.head(50).to_dict(orient="records")
        
        if market == 'US':
            import os, requests, concurrent.futures
            finnhub_key = os.getenv("FINNHUB_API_KEY", "")
            if finnhub_key:
                def _check_cap(stk):
                    sym = stk.get('symbol', '').split('.')[-1]
                    try:
                        u = f"https://finnhub.io/api/v1/stock/profile2?symbol={sym}&token={finnhub_key}"
                        r = requests.get(u, timeout=2)
                        data = r.json()
                        cap = data.get('marketCapitalization', 0)
                        # Finnhub 市值单位是百万美元 (Million USD)
                        if cap >= 100:
                            return stk
                        return None
                    except Exception as e:
                        logger.warning(f"Finnhub API error for {sym}: {e}")
                        return stk # 网络报错时谨慎放行
                        
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                    res = list(ex.map(_check_cap, candidates))
                    
                valid_stocks = [s for s in res if s is not None]
                return valid_stocks[:top_n]
                
        return candidates[:top_n]

    def process_and_notify(self, market: str):
        """主入口：获取数据、计算热榜、获取归因、发送通知"""
        logger.info(f"Generating market heat map for {market}...")
        
        # 1. 获取行情
        df = pd.DataFrame()
        try:
            if market == 'CN':
                # 东方财富接口(stock_zh_a_spot_em)在当前服务器被封，改用新浪批量接口
                # _fetch_bulk_sina 已被 HK/US 验证可用，CN同样支持
                from modules.ingestion.akshare_client import AkShareClient
                df = AkShareClient._fetch_bulk_sina('CN')
            elif market == 'HK':
                df = akshare_client.get_stock_info_hk()
            elif market == 'US':
                df = akshare_client.get_stock_info_us()
        except Exception as e:
            logger.error(f"Failed to fetch market data for {market}: {e}")
            return
            
        # 2. 生成榜单配置
        min_amt = 50_000_000   # CN/HK: 5000万人民币
        n = 10
        cn_total_amount = 0.0

        if market == 'CN':
            # 全市场成交额（过滤前对原始数据求和，保留真实总量）
            try:
                cn_total_amount = pd.to_numeric(
                    df['成交额'], errors='coerce'
                ).fillna(0).sum()
            except Exception:
                cn_total_amount = 0.0
        elif market == 'US':
            # 回调至 2000万美元（配合 Finnhub 接口做 1亿美金市值精确过滤）
            min_amt = 20_000_000
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
        
        # 5. 存入长线趋势种子池
        try:
            from modules.monitor.trend_service import TrendService
            pool_items = [{"symbol": r["symbol"], "name": r["name"], "reason": r.get("reason", "")} for r in results]
            TrendService.add_to_pool(market, pool_items)
        except Exception as e:
            logger.error(f"Failed to add heatmap results to TrendSeedPool: {e}")

        # 4. 组装消息并发送
        market_names = {'CN': 'A股', 'HK': '港股', 'US': '美股'}
        msg_lines = [f"🔥 **{market_names.get(market, market)} 盘后热门榜单 (Top {len(results)})**"]
        msg_lines.append(f"📅 日期: {datetime.date.today()}")

        # A股附带全市场成交额
        if market == 'CN' and cn_total_amount > 0:
            vol_str = f"{cn_total_amount/1e8:.0f} 亿" if cn_total_amount < 1e12 else f"{cn_total_amount/1e12:.2f} 万亿"
            msg_lines.append(f"📊 全市场成交额: **{vol_str}**")

        msg_lines.append("")
        
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
