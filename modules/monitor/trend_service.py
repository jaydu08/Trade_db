import logging
import datetime as dt
import re
from difflib import SequenceMatcher
from sqlmodel import select
from typing import List, Dict, Tuple

from core.db import get_ledger_session
from domain.ledger.analytics import TrendSeedPool, DailyRank

logger = logging.getLogger(__name__)

class TrendService:
    @staticmethod
    def add_to_pool(market: str, items: List[Dict]):
        """
        向滚动种子池添加记录
        items: [{"symbol": "...", "name": "...", "reason": "..."}]
        """
        today = dt.date.today()
        
        try:
            with get_ledger_session() as session:
                for item in items:
                    sym = item.get("symbol")
                    if not sym:
                        continue
                        
                    # 检查今天是否已经入库，避免重复
                    stmt = select(TrendSeedPool).where(
                        TrendSeedPool.date == today,
                        TrendSeedPool.market == market,
                        TrendSeedPool.symbol == sym
                    )
                    existing = session.exec(stmt).first()
                    
                    if not existing:
                        seed = TrendSeedPool(
                            date=today,
                            market=market,
                            symbol=sym,
                            name=item.get("name", ""),
                            daily_reason=item.get("reason", "")
                        )
                        session.add(seed)
                # 自动清理 30 天前的数据
                cutoff = today - dt.timedelta(days=30)
                del_stmt = select(TrendSeedPool).where(TrendSeedPool.date < cutoff)
                old_records = session.exec(del_stmt).all()
                for old in old_records:
                    session.delete(old)
                
                # context manager 将在退出时自动 commit
            logger.info(f"TrendSeedPool: Saved {len(items)} items for {market}")
        except Exception as e:
            logger.error(f"Failed to add to TrendSeedPool: {e}")

class TrendCalculator:
    @staticmethod
    def _normalize_symbol_for_api(symbol: str, market: str) -> str:
        """统一符号格式，兼容 US 的 105.TICKER 形态"""
        sym = str(symbol or "").strip()
        if market == "US" and "." in sym:
            # 例如 105.NVDA -> NVDA
            sym = sym.split(".")[-1].strip()
        return sym

    @staticmethod
    def _get_return_from_daily_rank_db(symbol: str, market: str, d_days: int) -> Tuple[float, float]:
        """接口失败时，回退使用本地 DailyRank 价格序列计算 N 日收益"""
        if market == "CF":
            return 0.0, 0.0

        raw = str(symbol or "").strip()
        candidates = [raw]
        if market == "US" and "." in raw:
            candidates.append(raw.split(".")[-1].strip())

        with get_ledger_session() as session:
            stmt = (
                select(DailyRank)
                .where(DailyRank.market == market)
                .where(DailyRank.symbol.in_(candidates))
                .order_by(DailyRank.date.desc())
            )
            rows = list(session.exec(stmt).all())

        # 去重并保留有效价格
        uniq = {}
        for r in rows:
            if r.price and r.price > 0:
                uniq[r.date] = float(r.price)
        if not uniq:
            return 0.0, 0.0

        dates = sorted(uniq.keys())
        current_date = dates[-1]
        current_price = uniq[current_date]
        target_date = current_date - dt.timedelta(days=d_days)

        past_dates = [d for d in dates if d <= target_date]
        if past_dates:
            past_price = uniq[past_dates[-1]]
        else:
            past_price = uniq[dates[0]]

        if past_price <= 0:
            return 0.0, round(current_price, 4)
        ret = round((current_price - past_price) / past_price * 100, 2)
        return ret, round(current_price, 4)

    @staticmethod
    def _normalize_reason(reason: str) -> str:
        """归一化理由文本，用于判定“同类理由”"""
        if not reason:
            return ""
        text = str(reason).strip().lower()
        # 去掉 markdown 与常见噪声词
        text = re.sub(r"[`*_#>\[\]\(\)\|]+", " ", text)
        text = re.sub(r"(未找到明显新闻催化|暂无新闻催化|分析原因失败|ai分板解析受限)", " ", text)
        # 去掉数字/百分号等波动噪声
        text = re.sub(r"[\d\.\-%％]+", " ", text)
        # 仅保留中英文数字
        text = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fa5]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _is_similar_reason(a: str, b: str, threshold: float = 0.82) -> bool:
        """粗粒度相似度判定：用于同票同类理由去重"""
        if not a or not b:
            return False
        if a == b:
            return True
        return SequenceMatcher(None, a, b).ratio() >= threshold

    @staticmethod
    def _aggregate_reasons_with_decay(
        reason_records: List[Tuple[dt.date, str]],
        today: dt.date,
        decay_base: float = 0.88,
    ) -> Tuple[str, float]:
        """
        聚合理由：
        1) 同票同类理由去重（避免连续5天重复理由被5倍累加）
        2) 越新的信号权重越高（指数衰减）
        返回: (聚合理由文本, 信号强度分)
        """
        # 按日期从新到旧，优先保留最新语义与更高权重
        sorted_records = sorted(reason_records, key=lambda x: x[0], reverse=True)

        buckets: List[dict] = []
        for r_date, raw_reason in sorted_records:
            if not raw_reason:
                continue
            if "分析原因失败" in raw_reason:
                continue

            norm = TrendCalculator._normalize_reason(raw_reason)
            if not norm:
                continue

            days_ago = max((today - r_date).days, 0)
            weight = decay_base ** days_ago

            matched = False
            for b in buckets:
                if TrendCalculator._is_similar_reason(norm, b["norm"]):
                    # 同类理由不重复累加，只保留权重更高（更近）的那次
                    if weight > b["weight"]:
                        b["weight"] = weight
                        b["date"] = r_date
                        b["reason"] = raw_reason.strip()
                    matched = True
                    break

            if not matched:
                buckets.append({
                    "norm": norm,
                    "reason": raw_reason.strip(),
                    "weight": weight,
                    "date": r_date,
                })

        if not buckets:
            return "暂无新闻催化", 0.0

        buckets.sort(key=lambda x: x["weight"], reverse=True)
        top_reasons = buckets[:3]
        aggregated_reason = " | ".join([b["reason"] for b in top_reasons])
        signal_strength = round(sum(b["weight"] for b in top_reasons), 3)
        return aggregated_reason, signal_strength

    @staticmethod
    def _get_return(symbol: str, market: str, d_days: int) -> Tuple[float, float]:
        """获取标的 N 天前的真实累计涨幅和现价 (基于日K线)"""
        import akshare as ak
        import pandas as pd
        from datetime import timedelta
        
        try:
            df = None
            api_symbol = TrendCalculator._normalize_symbol_for_api(symbol, market)

            if market == "CN":
                # CN 接口通常需要加 sh/sz 或直接用6位代码
                df = ak.stock_zh_a_hist(symbol=api_symbol, period="daily")
                # 东财线路被限时，回退到新浪日线接口
                if df is None or df.empty:
                    prefixed = f"sh{api_symbol}" if str(api_symbol).startswith("6") else f"sz{api_symbol}"
                    df = ak.stock_zh_a_daily(symbol=prefixed)
            elif market == "US":
                # US 接口接受 AAPL
                df = ak.stock_us_daily(symbol=api_symbol)
            elif market == "HK":
                # HK 接口接受 00700
                df = ak.stock_hk_daily(symbol=api_symbol)
            elif market == "CF":
                # CF 期货历史数据
                df = ak.futures_zh_daily_sina(symbol=api_symbol)
                
            if df is None or df.empty:
                return 0.0, 0.0

            # 字段归一化处理
            date_col = "date" if "date" in df.columns else "日期"
            close_col = "close" if "close" in df.columns else "收盘"
            
            if date_col not in df.columns or close_col not in df.columns:
                return 0.0, 0.0
                
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.sort_values(by=date_col).reset_index(drop=True)
            
            # 最近的收盘价
            current_close = float(df.iloc[-1][close_col])
            current_date = df.iloc[-1][date_col]
            
            # N 天前的实际交易日 (往回找 d_days 个自然日左右的最近交易日)
            target_date = current_date - timedelta(days=d_days)
            # 找到小于等于 target_date 的最后一条记录
            past_df = df[df[date_col] <= target_date]
            if past_df.empty:
                # 获取不到足够长的数据，用第一条
                past_close = float(df.iloc[0][close_col])
            else:
                past_close = float(past_df.iloc[-1][close_col])
                
            if past_close <= 0:
                return 0.0, current_close

            ret = round((current_close - past_close) / past_close * 100, 2)
            return ret, round(current_close, 4)
        except Exception as e:
            logger.debug(f"Calculate return failed for {symbol} ({market}): {e}")
            # 回退：用本地 DailyRank 历史价格序列估算，避免全 0
            try:
                return TrendCalculator._get_return_from_daily_rank_db(symbol, market, d_days)
            except Exception as e2:
                logger.debug(f"DailyRank fallback failed for {symbol} ({market}): {e2}")
                return 0.0, 0.0

    @staticmethod
    def calculate_trend(days: int = 7) -> Dict[str, List[Dict]]:
        """
        计算各市场趋势榜单
        """
        from collections import defaultdict
        import concurrent.futures
        
        cutoff = dt.date.today() - dt.timedelta(days=days)
        
        # 1. 查询种子池
        with get_ledger_session() as session:
            stmt = select(TrendSeedPool).where(TrendSeedPool.date >= cutoff)
            rows = session.exec(stmt).all()
            # 避免 session 关闭后 ORM 对象懒加载导致 DetachedInstanceError
            records = [
                {
                    "market": r.market,
                    "symbol": r.symbol,
                    "name": r.name,
                    "date": r.date,
                    "daily_reason": r.daily_reason,
                }
                for r in rows
            ]
            
        if not records:
            return {}
            
        # 2. 按市场和代码去重合并理由
        grouped = defaultdict(dict)
        for r in records:
            key = (r["market"], r["symbol"])
            if key not in grouped:
                grouped[key] = {
                    "market": r["market"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "reason_records": []
                }
            if r["daily_reason"]:
                grouped[key]["reason_records"].append((r["date"], r["daily_reason"].strip()))
                
        items = list(grouped.values())
        
        # 3. 并发查 N 日涨幅
        def _process(item: dict):
            ret, current_price = TrendCalculator._get_return(item["symbol"], item["market"], days)
            item["return_pct"] = ret
            item["current_price"] = current_price

            # 多日理由做“同类去重 + 新鲜度衰减”
            aggregated_reason, signal_strength = TrendCalculator._aggregate_reasons_with_decay(
                item.get("reason_records", []),
                today=dt.date.today(),
            )
            item["aggregated_reason"] = aggregated_reason
            item["signal_strength"] = signal_strength

            # 趋势总分：在原始累计涨幅基础上，给新鲜有效信号一定加权
            # 目标：老热点理由重复刷屏时，难以长期霸榜
            freshness_factor = min(1.25, 0.85 + signal_strength * 0.18)
            item["trend_score"] = round(ret * freshness_factor, 2)
            return item
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(_process, items))
            
        # 4. 按市场分组排序并取 Top 10
        market_tops = defaultdict(list)
        for res in results:
            market_tops[res["market"]].append(res)
            
        final_tops = {}
        for mkt, stks in market_tops.items():
            stks.sort(key=lambda x: x.get("trend_score", x["return_pct"]), reverse=True)
            # 取出前10名
            final_tops[mkt] = stks[:10]
            
        return dict(final_tops)
