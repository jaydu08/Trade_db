import logging
import datetime
import concurrent.futures
import os
from typing import List, Dict

import pandas as pd
import numpy as np
import requests

from modules.ingestion.akshare_client import akshare_client
from core.llm import simple_prompt
from core.agent import Tools
from modules.monitor.notifier import Notifier
from modules.monitor.news_intel import summarize_symbol_news
from modules.monitor.industry_intel import enrich_industry_labels
from modules.ingestion.market_cap import (
    get_cn_market_metrics,
    get_cn_fund_flow,
    get_hk_market_metrics,
    format_mv_cn,
    format_flow_cn,
    format_mv_hk,
)
from modules.ingestion.us_market_cap import get_us_market_metrics


logger = logging.getLogger(__name__)

class MarketHeatMap:
    """
    市场热度榜单服务
    获取各市场涨幅靠前且活跃的个股，并使用 LLM 总结其上涨原因。
    """
    def __init__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.enable_daily_attribution = str(
            os.getenv("ENABLE_HEATMAP_ATTRIBUTION", "0")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.enable_market_brief = str(
            os.getenv("ENABLE_HEATMAP_MARKET_BRIEF", "1")
        ).strip().lower() in {"1", "true", "yes", "on"}
        # A股热榜参数（可通过环境变量在线调参）
        self.cn_norm_min = float(os.getenv("CN_HEAT_NORM_MIN", "0.60"))
        self.cn_norm_fallback_min = float(os.getenv("CN_HEAT_NORM_FALLBACK_MIN", "0.50"))
        self.cn_near_limit_low = float(os.getenv("CN_HEAT_NEAR_LIMIT_LOW", "0.97"))
        self.cn_near_limit_high = float(os.getenv("CN_HEAT_NEAR_LIMIT_HIGH", "1.03"))
        self.cn_gem_bonus = float(os.getenv("CN_HEAT_GEM_BONUS", "1.10"))
        self.cn_turnover_fetch_cap = int(os.getenv("CN_HEAT_TURNOVER_FETCH_CAP", "220"))
        self.cn_hard_amount_min = float(os.getenv("CN_HARD_AMOUNT_MIN", "200000000"))
        self.cn_hard_total_mv_100m_min = float(os.getenv("CN_HARD_TOTAL_MV_100M_MIN", "50"))
        self.hk_hard_amount_min = float(os.getenv("HK_HARD_AMOUNT_MIN", "250000000"))
        self.us_hard_mcap_musd_min = float(os.getenv("US_HARD_MCAP_MUSD_MIN", "1000"))
        self.us_mcap_fetch_cap = int(os.getenv("US_HEAT_MCAP_FETCH_CAP", "160"))
        self.fomo_upper_shadow_pct = float(os.getenv("FOMO_UPPER_SHADOW_PCT", "0.03"))
        self.fomo_penalty_factor = float(os.getenv("FOMO_PENALTY_FACTOR", "0.95"))
        self.heat_w_news = float(os.getenv("HEATMAP_W_NEWS", "0"))
        self.heat_news_lookback_days = int(os.getenv("HEATMAP_NEWS_LOOKBACK_DAYS", "3") or 3)

        self.cn_mcap_fetch_cap = int(os.getenv("CN_HEAT_MCAP_FETCH_CAP", "260"))
        self.cn_trend_lookback_days = int(os.getenv("CN_HEAT_TREND_LOOKBACK_DAYS", "20"))
        self.cn_regime_pos_ratio = float(os.getenv("CN_HEAT_REGIME_POS_RATIO", "0.60"))
        self.cn_regime_amount_ratio = float(os.getenv("CN_HEAT_REGIME_AMOUNT_RATIO", "1.05"))

        # 震荡市：降低涨幅权重、提高大票与成交额权重
        self.cn_weights_range = self._normalize_weights({
            "pct": float(os.getenv("CN_HEAT_W_PCT_RANGE", "0.14")),
            "amount": float(os.getenv("CN_HEAT_W_AMOUNT_RANGE", "0.26")),
            "turnover": float(os.getenv("CN_HEAT_W_TURNOVER_RANGE", "0.20")),
            "mcap": float(os.getenv("CN_HEAT_W_MCAP_RANGE", "0.30")),
            "trend": float(os.getenv("CN_HEAT_W_TREND_RANGE", "0.10")),
        })

        # 趋势市：大市值权重显著提升，成交/换手适度降权，保留趋势延续参与度
        self.cn_weights_trend = self._normalize_weights({
            "pct": float(os.getenv("CN_HEAT_W_PCT_TREND", "0.18")),
            "amount": float(os.getenv("CN_HEAT_W_AMOUNT_TREND", "0.22")),
            "turnover": float(os.getenv("CN_HEAT_W_TURNOVER_TREND", "0.18")),
            "mcap": float(os.getenv("CN_HEAT_W_MCAP_TREND", "0.30")),
            "trend": float(os.getenv("CN_HEAT_W_TREND_TREND", "0.12")),
        })

    @staticmethod
    def _normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
        clean = {k: max(0.0, float(v or 0.0)) for k, v in weights.items()}
        s = sum(clean.values())
        if s <= 1e-9:
            n = max(1, len(clean))
            return {k: 1.0 / n for k in clean}
        return {k: v / s for k, v in clean.items()}

    def _load_cn_market_amount_regime(self) -> Dict[str, float]:
        """读取A股全市场近7个交易日成交额，用于趋势市量能确认。"""
        try:
            from sqlmodel import select
            from core.db import db_manager
            from domain.ledger.analytics import TrendDailyBar

            start_date = datetime.date.today() - datetime.timedelta(days=30)
            amount_by_date: Dict[datetime.date, float] = {}

            with db_manager.ledger_session() as session:
                rows = session.exec(
                    select(TrendDailyBar).where(
                        TrendDailyBar.market == "CN",
                        TrendDailyBar.date >= start_date,
                    )
                ).all()

            for r in rows:
                d = getattr(r, "date", None)
                amt = float(getattr(r, "amount", 0) or 0)
                if d and amt > 0:
                    amount_by_date[d] = amount_by_date.get(d, 0.0) + amt

            if not amount_by_date:
                return {"today": 0.0, "avg7": 0.0}

            ordered = sorted(amount_by_date.items(), key=lambda x: x[0])
            vals = [float(v or 0.0) for _, v in ordered if float(v or 0.0) > 0]
            if not vals:
                return {"today": 0.0, "avg7": 0.0}

            today_amount = vals[-1]
            prev = vals[-8:-1] if len(vals) >= 8 else vals[:-1]
            avg7 = float(sum(prev) / len(prev)) if prev else 0.0
            return {"today": today_amount, "avg7": avg7}
        except Exception:
            return {"today": 0.0, "avg7": 0.0}

    def _pick_cn_weight_profile(self, all_df: pd.DataFrame) -> Dict[str, float]:
        """按市场状态切换权重：趋势市=上涨占比+量能共振；否则震荡/缩量。"""
        if all_df is None or all_df.empty:
            return self.cn_weights_range

        pct = pd.to_numeric(all_df.get("pct_chg", 0), errors="coerce").fillna(0.0)
        pos_ratio = float((pct > 0).mean()) if len(pct) else 0.0

        regime_amount = self._load_cn_market_amount_regime()
        today_amount = float(regime_amount.get("today", 0) or 0)
        avg7_amount = float(regime_amount.get("avg7", 0) or 0)

        trend_by_breadth = pos_ratio >= self.cn_regime_pos_ratio
        trend_by_amount = avg7_amount > 0 and today_amount >= avg7_amount * self.cn_regime_amount_ratio

        if trend_by_breadth and trend_by_amount:
            return self.cn_weights_trend
        return self.cn_weights_range

    def _build_cn_market_cap_factor(self, filtered: pd.DataFrame) -> pd.Series:
        """构建A股市值因子：总市值越大得分越高（log后百分位）。"""
        if filtered.empty:
            return pd.Series(dtype=float)

        work = filtered.copy()
        out = pd.Series(0.5, index=work.index, dtype=float)

        symbols = []
        top_idx = work.sort_values(by="amount", ascending=False).head(self.cn_mcap_fetch_cap).index
        for idx in top_idx:
            sym = str(work.at[idx, "symbol"]).strip()
            if sym:
                symbols.append((idx, sym))

        if not symbols:
            return out

        def _fetch_one(item):
            idx, symbol = item
            metrics = get_cn_market_metrics(symbol)
            total_mv_100m = float((metrics or {}).get("total_mv_100m", 0) or 0)
            return idx, max(total_mv_100m, 0.0)

        mcap_vals = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
            futures = [ex.submit(_fetch_one, item) for item in symbols]
            for f in concurrent.futures.as_completed(futures):
                try:
                    idx, mv = f.result()
                    if mv > 0:
                        mcap_vals[idx] = mv
                except Exception:
                    continue

        if not mcap_vals:
            return out

        mcap_series = pd.Series(mcap_vals, dtype=float)
        out.loc[mcap_series.index] = np.log1p(mcap_series).rank(pct=True)
        return out

    def _build_cn_trend_continuity_factor(self, filtered: pd.DataFrame) -> pd.Series:
        """构建趋势延续因子：综合5/10日动量 + 近阶段新高特征。"""
        if filtered.empty:
            return pd.Series(dtype=float)

        out = pd.Series(0.5, index=filtered.index, dtype=float)
        symbol_df = filtered.sort_values(by="amount", ascending=False).head(max(200, self.cn_mcap_fetch_cap))
        symbols = [str(x).strip() for x in symbol_df.get("symbol", []).tolist() if str(x).strip()]
        if not symbols:
            return out

        hist_by_symbol = {sym: [] for sym in symbols}
        start_date = datetime.date.today() - datetime.timedelta(days=max(12, self.cn_trend_lookback_days))

        try:
            from sqlmodel import select
            from core.db import db_manager
            from domain.ledger.analytics import TrendDailyBar

            with db_manager.ledger_session() as session:
                rows = session.exec(
                    select(TrendDailyBar).where(
                        TrendDailyBar.market == "CN",
                        TrendDailyBar.date >= start_date,
                        TrendDailyBar.symbol.in_(symbols),
                    )
                ).all()

            for r in rows:
                c = float(getattr(r, "close", 0) or 0)
                h = float(getattr(r, "high", 0) or 0)
                if c > 0:
                    hist_by_symbol.setdefault(r.symbol, []).append((r.date, c, h if h > 0 else c))
        except Exception:
            return out

        raw_scores = {}
        for idx, row in filtered.iterrows():
            symbol = str(row.get("symbol", "")).strip()
            curr = float(row.get("price", 0) or 0)
            hist = sorted(hist_by_symbol.get(symbol, []), key=lambda x: x[0])
            closes = [x[1] for x in hist if x[1] > 0]
            highs = [x[2] for x in hist if x[2] > 0]

            if curr <= 0 and closes:
                curr = closes[-1]
            if curr <= 0:
                raw_scores[idx] = 0.0
                continue

            seq = closes[-20:]
            if (not seq) or abs(curr - seq[-1]) / max(seq[-1], 1e-9) > 1e-3:
                seq = seq + [curr]

            ret5 = (seq[-1] / seq[-6] - 1.0) if len(seq) >= 6 and seq[-6] > 0 else 0.0
            ret10 = (seq[-1] / seq[-11] - 1.0) if len(seq) >= 11 and seq[-11] > 0 else ret5

            recent_high = 0.0
            if highs:
                recent_high = max(highs[-20:])
            if seq:
                recent_high = max(recent_high, max(seq[-20:]))

            breakout = 1.0 if recent_high > 0 and seq[-1] >= recent_high * 0.995 else 0.0
            momentum = 0.6 * ret5 + 0.4 * ret10
            raw_scores[idx] = momentum + 0.08 * breakout

        if not raw_scores:
            return out

        raw = pd.Series(raw_scores, dtype=float)
        if raw.nunique(dropna=True) <= 1:
            out.loc[raw.index] = 0.5
            return out

        out.loc[raw.index] = raw.rank(pct=True)
        return out

    def _build_cn_turnover_factor(self, filtered: pd.DataFrame) -> pd.Series:
        """
        构建A股换手率因子：
        1) 优先使用行情原始换手率
        2) 缺失时使用近似换手率 amount / circ_mv * 100 回填
        最终使用 log1p 后的百分位做评分。
        """
        if filtered.empty:
            return pd.Series(dtype=float)

        work = filtered.copy()
        work["turnover"] = pd.to_numeric(work.get("turnover", 0), errors="coerce").fillna(0.0)
        work["turnover_effective"] = work["turnover"].clip(lower=0.0)

        missing_idx = work.index[work["turnover_effective"] <= 0].tolist()
        if missing_idx:
            # 仅对候选池中缺失换手率的标的补拉市值，避免全市场逐个请求过慢
            symbols = []
            for i in missing_idx[: self.cn_turnover_fetch_cap]:
                symbol = str(work.at[i, "symbol"]).strip()
                if symbol:
                    symbols.append((i, symbol))

            def _fetch_one(item):
                idx, symbol = item
                metrics = get_cn_market_metrics(symbol)
                circ_mv_100m = float((metrics or {}).get("circ_mv_100m", 0) or 0)
                if circ_mv_100m <= 0:
                    return idx, 0.0
                amount = float(work.at[idx, "amount"] or 0)
                turnover_approx = (amount / (circ_mv_100m * 1e8)) * 100.0
                return idx, max(turnover_approx, 0.0)

            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
                futures = [ex.submit(_fetch_one, item) for item in symbols]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        idx, approx = f.result()
                        if approx > 0:
                            work.at[idx, "turnover_effective"] = approx
                    except Exception:
                        continue

        return np.log1p(work["turnover_effective"]).rank(pct=True)

    def _get_news_and_reason(self, symbol: str, name: str, pct_chg: float, market: str) -> str:
        """获取个股最新消息，并交给 LLM 极简归因"""
        try:
            # 双通道搜索：个股 + 市场背景（与异动归因链路保持一致口径）
            if market == 'US':
                q_specific = f"{symbol} stock news why down up today"
                q_market = "US stock market today main drivers tech news"
            elif market == 'HK':
                q_specific = f"{symbol} {name} 港股 股价 异动原因 暴涨 暴跌 财报"
                q_market = "港股 恒生科技 恒指 今日异动 大盘分析"
            else:
                q_specific = f"{symbol} {name} 股票 为什么 涨停 跌停 异动 最新公告"
                q_market = "A股 沪指 创业板 今日异动 板块 领涨"

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                f_specific = pool.submit(Tools.web_search, q_specific)
                f_market = pool.submit(Tools.web_search, q_market)
                specific_news = f_specific.result(timeout=25)
                market_news = f_market.result(timeout=25)

            news_context = f"【个股专有资讯】:\n{specific_news}"
            if (
                len(specific_news) < 100
                or "暂无相关新闻" in specific_news
                or "所有搜索引擎均未返回有效结果" in specific_news
            ):
                news_context += f"\n\n【市场/大盘/板块背景】:\n{market_news}"

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

    def _get_market_brief(self, market: str) -> str:
        """获取市场级别简析（联网搜索 + LLM），失败时返回空字符串。"""
        query_map = {
            "CN": "A股 今天 收盘 盘后 复盘 主线 板块 资金 情绪",
            "HK": "港股 今天 收盘 复盘 恒指 恒生科技 资金流向",
            "US": "US stock market today close recap sectors movers macro",
        }
        query = query_map.get(market)
        if not query:
            return ""

        try:
            ctx = Tools.web_search(query)
        except Exception as e:
            logger.warning("Heatmap market brief search failed for %s: %s", market, e)
            return ""

        if not ctx or "未返回有效结果" in ctx:
            return ""

        prompt = f"""
你是交易员盘后快讯助手。请根据以下检索内容，输出 1-2 句市场简析，总字数不超过 70 字，聚焦当日主线、资金风格和风险点。

市场: {market}
检索内容:
{ctx[:2500]}
"""
        try:
            summary = simple_prompt(prompt, temperature=0.2)
            summary = str(summary or "").replace("`", "").strip()
            return summary[:120]
        except Exception as e:
            logger.warning("Heatmap market brief LLM failed for %s: %s", market, e)
            return ""

    def _persist_daily_rank_from_heatmap(self, market: str, results: List[Dict], rank_type: str = "change_pct"):
        """将热榜结果写入 DailyRank；change_pct 保持 TG 推送口径，daily_hot 供 Web 扩容。"""
        from sqlmodel import select
        from core.db import db_manager
        from domain.ledger.analytics import DailyRank

        today = datetime.date.today()
        rows = []
        for item in results:
            pct = float(item.get("pct_chg", 0) or 0)
            if pct <= 0:
                continue
            rows.append(
                DailyRank(
                    date=today,
                    market=market,
                    rank_type=rank_type,
                    symbol=str(item.get("symbol", "")),
                    name=str(item.get("name", "")),
                    price=float(item.get("price", 0) or 0),
                    change_pct=pct,
                    amount=float(item.get("amount", 0) or 0),
                    turnover_rate=float(item.get("turnover", 0) or 0),
                )
            )

        with db_manager.ledger_session() as session:
            existing = session.exec(
                select(DailyRank).where(
                    DailyRank.date == today,
                    DailyRank.market == market,
                    DailyRank.rank_type == rank_type,
                )
            ).all()
            for old in existing:
                session.delete(old)
            if rows:
                session.add_all(rows)
            session.commit()

        logger.info("DailyRank synced from heatmap: market=%s rank_type=%s rows=%s date=%s", market, rank_type, len(rows), today)

    def _load_recent_avg_amount_map(self, market: str, symbols: List[str], lookback_days: int = 12) -> Dict[str, float]:
        if not symbols:
            return {}
        try:
            from sqlmodel import select
            from core.db import db_manager
            from domain.ledger.analytics import TrendDailyBar

            start_date = datetime.date.today() - datetime.timedelta(days=max(6, int(lookback_days or 12)))
            with db_manager.ledger_session() as session:
                rows = session.exec(
                    select(TrendDailyBar).where(
                        TrendDailyBar.market == market,
                        TrendDailyBar.date >= start_date,
                        TrendDailyBar.symbol.in_(symbols),
                    )
                ).all()

            hist = {s: [] for s in symbols}
            for r in rows:
                amt = float(getattr(r, "amount", 0) or 0)
                if amt > 0:
                    hist.setdefault(str(r.symbol), []).append(amt)

            out = {}
            for sym, arr in hist.items():
                if arr:
                    out[sym] = float(sum(arr[-5:]) / max(1, len(arr[-5:])))
            return out
        except Exception:
            return {}

    def _apply_fomo_penalty(self, market: str, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return frame

        work = frame.copy()
        if "open" not in work.columns:
            work["open"] = 0.0
        if "high" not in work.columns:
            work["high"] = 0.0
        if "volume" not in work.columns:
            work["volume"] = 0.0

        work["open"] = pd.to_numeric(work.get("open", 0), errors="coerce").fillna(0.0)
        work["high"] = pd.to_numeric(work.get("high", 0), errors="coerce").fillna(0.0)
        work["volume"] = pd.to_numeric(work.get("volume", 0), errors="coerce").fillna(0.0)
        work["amount"] = pd.to_numeric(work.get("amount", 0), errors="coerce").fillna(0.0)
        work["price"] = pd.to_numeric(work.get("price", 0), errors="coerce").fillna(0.0)

        symbols = [str(x).strip() for x in work.get("symbol", []).tolist() if str(x).strip()]
        ma5_map = self._load_recent_avg_amount_map(market, symbols, lookback_days=12)

        fomo_flags = []
        for _, row in work.iterrows():
            top = max(float(row.get("open", 0) or 0), float(row.get("price", 0) or 0))
            high = float(row.get("high", 0) or 0)
            upper_shadow_pct = ((high - top) / top) if (top > 0 and high > top) else 0.0

            sym = str(row.get("symbol", "") or "").strip()
            liq_today = float(row.get("volume", 0) or 0)
            if liq_today <= 0:
                liq_today = float(row.get("amount", 0) or 0)
            liq_ma5 = float(ma5_map.get(sym, 0) or 0)

            is_fomo = bool(upper_shadow_pct > self.fomo_upper_shadow_pct and liq_ma5 > 0 and liq_today > liq_ma5)
            fomo_flags.append(is_fomo)

        work["fomo_flag"] = fomo_flags
        if "heat_score" in work.columns:
            work.loc[work["fomo_flag"], "heat_score"] = work.loc[work["fomo_flag"], "heat_score"] * self.fomo_penalty_factor
        return work

    def _apply_cn_hard_funnel(self, frame: pd.DataFrame, amount_floor: float) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()

        floor = max(float(amount_floor or 0), float(self.cn_hard_amount_min or 0))
        work = frame[pd.to_numeric(frame.get("amount", 0), errors="coerce").fillna(0.0) >= floor].copy()
        if work.empty:
            return work

        idx_syms = []
        for idx, row in work.iterrows():
            sym = str(row.get("symbol", "")).strip()
            if sym:
                idx_syms.append((idx, sym))

        if not idx_syms:
            return pd.DataFrame()

        total_mv_map = {}

        def _fetch_one(item):
            idx, sym = item
            m = get_cn_market_metrics(sym)
            return idx, float((m or {}).get("total_mv_100m", 0) or 0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(idx_syms))) as ex:
            futures = [ex.submit(_fetch_one, it) for it in idx_syms]
            for f in concurrent.futures.as_completed(futures):
                try:
                    idx, mv = f.result()
                    if mv > 0:
                        total_mv_map[idx] = mv
                except Exception:
                    continue

       # 市值获取失败（0）不剔除，仅不参与市值因子加权；
        # 仅对"成功拿到市值"的标的执行市值硬过滤。
        work["total_mv_100m"] = work.index.to_series().map(total_mv_map).fillna(0.0)
        known_mask = work["total_mv_100m"] > 0
        floor = float(self.cn_hard_total_mv_100m_min or 0)
        work = work[(~known_mask) | (work["total_mv_100m"] >= floor)].copy()

        logger.info(
            "CN hard funnel: candidates=%s known_mv=%s kept=%s floor_100m=%.0f",
            len(frame), int(known_mask.sum()), len(work), floor,
        )
        return work

    def _apply_hk_hard_funnel(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        return frame[pd.to_numeric(frame.get("amount", 0), errors="coerce").fillna(0.0) >= float(self.hk_hard_amount_min or 0)].copy()

    def _apply_us_hard_funnel(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()

        work = frame.copy()
        if "amount" in work.columns:
            work = work.sort_values(by="amount", ascending=False)

        idx_syms = []
        for idx, row in work.head(max(1, int(self.us_mcap_fetch_cap or 160))).iterrows():
            sym = str(row.get("symbol", "")).split(".")[-1].strip()
            if sym:
                idx_syms.append((idx, sym))

        cap_map = {}

        def _fetch_cap(item):
            idx, sym = item
            m = get_us_market_metrics(sym)
            cap = float((m or {}).get("market_cap_musd", 0) or 0)
            return idx, cap

        if idx_syms:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(idx_syms))) as ex:
                futures = [ex.submit(_fetch_cap, it) for it in idx_syms]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        idx, cap = f.result()
                        if cap > 0:
                            cap_map[idx] = cap
                    except Exception:
                        continue

        # 市值获取失败（0）不剔除，仅不给超大市值加权；
        # 仅对"成功拿到市值"的标的执行市值硬过滤。
        # 新增：无市值且名称像杠杆ETF产品的 → 过滤（避免新发杠杆ETF混入）
        work["market_cap_musd"] = work.index.to_series().map(cap_map).fillna(0.0)
        known_mask = work["market_cap_musd"] > 0
        floor = float(self.us_hard_mcap_musd_min or 0)
        
        import re as _re
        is_leveraged = work["name"].astype(str).str.contains(
            r'(?i)\b(2x|3x|1\.5x|bull|bear|leveraged|inverse)\b|\d[\d.]*[Xx]\s+(?:long|short|bull|bear)',
            regex=True, na=False
        )
        # 保留：有市值>=floor 的 或 无市值但不是杠杆ETF的
        work = work[(work["market_cap_musd"] >= floor) | (~known_mask & ~is_leveraged)].copy()
        work["market_cap_100m_usd"] = work["market_cap_musd"] / 100.0

        logger.info(
            "US hard funnel: candidates=%s known_cap=%s kept=%s floor_musd=%.0f",
            len(frame), int(known_mask.sum()), len(work), floor,
        )
        return work

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
            "成交额": "amount",
            "开盘": "open",
            "最高": "high",
            "成交量": "volume"
        }
        
        # 兼容不同市场的字段名差异
        renamed_df = df.rename(columns=columns_map)
        
        required_cols = ["symbol", "name", "price", "pct_chg", "amount"]
        for col in required_cols:
            if col not in renamed_df.columns:
                logger.error(f"Missing column '{col}' in '{market}' data.")
                return []
                
        # 部分字段可能不存在，容错处理
        if "turnover" not in renamed_df.columns:
            renamed_df["turnover"] = 0.0
        if "open" not in renamed_df.columns:
            renamed_df["open"] = 0.0
        if "high" not in renamed_df.columns:
            renamed_df["high"] = 0.0
        if "volume" not in renamed_df.columns:
            renamed_df["volume"] = 0.0

        # 数据清洗: 过滤空值和非数字
        renamed_df = renamed_df.dropna(subset=required_cols)
        renamed_df["pct_chg"] = pd.to_numeric(renamed_df["pct_chg"], errors='coerce').fillna(0)
        renamed_df["amount"] = pd.to_numeric(renamed_df["amount"], errors='coerce').fillna(0)
        renamed_df["turnover"] = pd.to_numeric(renamed_df["turnover"], errors='coerce').fillna(0)
        renamed_df["open"] = pd.to_numeric(renamed_df["open"], errors='coerce').fillna(0)
        renamed_df["high"] = pd.to_numeric(renamed_df["high"], errors='coerce').fillna(0)
        renamed_df["volume"] = pd.to_numeric(renamed_df["volume"], errors='coerce').fillna(0)

        # 过滤成交额太小的仙股 (默认 5000 万)
        filtered = renamed_df[renamed_df["amount"] >= min_amount].copy()
        
        if market == 'CN':
            filtered = self._apply_cn_hard_funnel(renamed_df, min_amount)
            if filtered.empty:
                logger.warning("CN hard funnel produced no candidates")
                return []

            # ────────────────────────────────────────────────────────────────
            # A股热榜算法（五因子模式）：
            # 1) 归一化涨幅（按板块涨停幅度归一）
            # 2) log(成交额) 百分位
            # 3) 换手率百分位（缺失时用市值近似回填）
            # ────────────────────────────────────────────────────────────────

            # 各板块涨停限制（用于归一化涨幅）
            limits = pd.Series(10.0, index=filtered.index)
            limits[filtered["symbol"].str.startswith(("30", "688"))] = 20.0
            limits[filtered["symbol"].str.startswith(("8", "4"))] = 30.0

            normalized_pct = filtered["pct_chg"] / limits

            # 入池门槛（可配置）：归一化涨幅 >= 0.60
            pool_mask = normalized_pct >= self.cn_norm_min
            filtered = filtered[pool_mask].copy()
            limits = limits.loc[filtered.index]
            normalized_pct = normalized_pct.loc[filtered.index]

            # 弱市降级（可配置）：默认放宽到 0.50
            if filtered.empty:
                logger.warning(
                    "CN: normalized_pct>=%.2f 无满足标的，降级到 >=%.2f",
                    self.cn_norm_min,
                    self.cn_norm_fallback_min,
                )
                fallback_df = filtered.copy()
                fallback_limits = pd.Series(10.0, index=fallback_df.index)
                fallback_limits[fallback_df["symbol"].str.startswith(("30", "688"))] = 20.0
                fallback_limits[fallback_df["symbol"].str.startswith(("8", "4"))] = 30.0
                fallback_norm = fallback_df["pct_chg"] / fallback_limits
                fallback_pool = fallback_norm >= self.cn_norm_fallback_min
                filtered = fallback_df[fallback_pool].copy()
                limits = fallback_limits.loc[filtered.index]
                normalized_pct = fallback_norm.loc[filtered.index]

            limits = limits.loc[filtered.index]
            normalized_pct = normalized_pct.loc[filtered.index]

            if filtered.empty:
                logger.warning("CN: 最终候选池为空，跳过热榜")
                return []

            # 近涨停抹平，避免 19.98%/19.99% 这类噪声干扰排序
            mask = (normalized_pct >= self.cn_near_limit_low) & (normalized_pct <= self.cn_near_limit_high)
            normalized_pct.loc[mask] = 1.0
            normalized_pct = normalized_pct.clip(0, 1.2)

            rank_pct = normalized_pct.rank(pct=True)
            rank_amount = np.log1p(filtered["amount"]).rank(pct=True)
            rank_turnover = self._build_cn_turnover_factor(filtered)
            rank_mcap = self._build_cn_market_cap_factor(filtered)
            rank_trend = self._build_cn_trend_continuity_factor(filtered)

            w = self._pick_cn_weight_profile(renamed_df)
            regime = "trend" if w == self.cn_weights_trend else "range"

            filtered = filtered.copy()
            filtered["heat_score"] = (
                rank_pct * w["pct"] +
                rank_amount * w["amount"] +
                rank_turnover * w["turnover"] +
                rank_mcap * w["mcap"] +
                rank_trend * w["trend"]
            )

            # 创业板/科创板 且涨幅 >10% 轻量加成
            bonus_mask = (
                filtered["symbol"].str.startswith(("30", "688")) &
                (filtered["pct_chg"] > 10.0)
            )
            if bonus_mask.any():
                filtered.loc[bonus_mask, "heat_score"] *= self.cn_gem_bonus
                logger.info(
                    "CN热榜: 创业板/科创板涨幅>10%%加成%.2fx，命中 %d 支",
                    self.cn_gem_bonus,
                    bonus_mask.sum(),
                )

            filtered = self._apply_fomo_penalty("CN", filtered)
            sorted_df = filtered.sort_values(by="heat_score", ascending=False)
            logger.info(
                "CN热榜(五因子): regime=%s 候选=%s 最高涨幅=%.2f%% 最低涨幅=%.2f%% 权重[pct/amount/turnover/mcap/trend]=[%.2f/%.2f/%.2f/%.2f/%.2f]",
                regime,
                len(filtered),
                filtered["pct_chg"].max(),
                filtered["pct_chg"].min(),
                w["pct"],
                w["amount"],
                w["turnover"],
                w["mcap"],
                w["trend"],
            )
        else:
            # HK / US：同样改用百分位归一化
            filtered = filtered[filtered["pct_chg"] >= 5.0].copy()

            if market == 'HK':
                filtered = self._apply_hk_hard_funnel(filtered)
            elif market == 'US':
                filtered = self._apply_us_hard_funnel(filtered)

            if filtered.empty:
                return []

            # (已移除) 旧版通过价格和成交额代理市值的硬性过滤
            # 现在改为两段式：先取 Top 50，再调用 Finnhub API 判断真实市值 >= 100M

            rank_pct    = filtered["pct_chg"].rank(pct=True)
            rank_amount = filtered["amount"].rank(pct=True)
            has_turnover = filtered["turnover"].sum() > 0
            rank_turnover = filtered["turnover"].rank(pct=True) if has_turnover else pd.Series(0.5, index=filtered.index)

            filtered = filtered.copy()
            if market == 'HK':
                # 港股: 重成交额→流动性龙头优先 (amount 与市值强相关)
                filtered["heat_score"] = (
                    rank_pct      * 0.25 +
                    rank_amount   * 0.50 +
                    rank_turnover * 0.25
                )
            elif market == 'US':
                # 美股: pct 45% + amount 35% + turnover 20%，叠加市值乘数双重提权大票
                filtered["heat_score"] = (
                    rank_pct      * 0.45 +
                    rank_amount   * 0.35 +
                    rank_turnover * 0.20
                )
            else:
                filtered["heat_score"] = (
                    rank_pct    * 0.50 +
                    rank_amount * 0.30 +
                    rank_turnover * 0.20
                )

            if market == 'US' and "market_cap_musd" in filtered.columns:
                # For stocks with mcap data, use mcap multiplier
                mult = np.where(filtered["market_cap_musd"] >= 300000.0, 2.0,
                                np.where(filtered["market_cap_musd"] >= 100000.0, 1.6,
                                         np.where(filtered["market_cap_musd"] >= 50000.0, 1.3, 1.0)))
                # Fallback: stocks with mcap=0 but very high amount likely mega-caps
                # Use amount percentile as proxy multiplier (1.0~1.5)
                no_cap_mask = filtered["market_cap_musd"] <= 0
                if no_cap_mask.any():
                    amt_rank = filtered.loc[no_cap_mask, "amount"].rank(pct=True)
                    amt_mult = np.where(amt_rank >= 0.9, 1.5,
                                        np.where(amt_rank >= 0.7, 1.3,
                                                 np.where(amt_rank >= 0.5, 1.15, 1.0)))
                    mult_series = pd.Series(mult, index=filtered.index)
                    mult_series.loc[no_cap_mask] = amt_mult
                    mult = mult_series.values
                filtered["heat_score"] = filtered["heat_score"] * mult

            filtered = self._apply_fomo_penalty(market, filtered)
            sorted_df = filtered.sort_values(by="heat_score", ascending=False)

            # 美股特定：去除权证类 + 底层存在时删除杠杆ETF + 同底层杠杆ETF去重
            if market == 'US':
                import re as _re

                _LETF_RE1 = _re.compile(r'\d[\d.]*[Xx]\s+(?:Long\s+|Short\s+)?([A-Z]{2,6})')
                _LETF_RE2 = _re.compile(r'(?:T-Rex|Defiance|ProShares|GraniteShares|Direxion)\s+.*?([A-Z]{2,6})(?:\s|$)')
                _LETF_RE3 = _re.compile(r'(?i)\b(2x|3x|1\.5x|bull|bear|leveraged|inverse)\b|\d[\d.]*[Xx]\s+(?:long|short|bull|bear)')
                _WARRANT_RE = _re.compile(r'(?i)\b(wt|warrant|rights|rts|units?)\b')

                def _extract_letf_underlying(name: str) -> str:
                    """从杠杆ETF名称提取底层ticker，非杠杆ETF返回空字符串"""
                    for pat in (_LETF_RE1, _LETF_RE2):
                        m = pat.search(name)
                        if m:
                            return m.group(1)
                    m3 = _LETF_RE3.search(name)
                    if m3:
                        # 尝试从名称中提取大写简称作为底层（守望法）
                        tokens = _re.findall(r'\b[A-Z]{1,5}\b', name)
                        for t in tokens:
                            if t not in ('ETF', 'US', 'USD', 'NYSE', 'NASDAQ'):
                                return t
                    return ""

                def _is_warrant(name: str) -> bool:
                    return bool(_WARRANT_RE.search(name))

                sorted_df = sorted_df.copy()

                # 1) 去除权证
                sorted_df = sorted_df[~sorted_df['name'].apply(_is_warrant)]

                # 2) 收集所有"非杠杆ETF"的 symbol
                ordinary_syms: set = set()
                for _, row in sorted_df.iterrows():
                    name = str(row.get('name', ''))
                    sym  = str(row.get('symbol', '')).split('.')[-1]
                    if not _extract_letf_underlying(name):
                        ordinary_syms.add(sym)

                # 3) 删除底层在普通股列表中的杠杆ETF
                keep_mask = []
                for _, row in sorted_df.iterrows():
                    name = str(row.get('name', ''))
                    underlying = _extract_letf_underlying(name)
                    if underlying and underlying in ordinary_syms:
                        keep_mask.append(False)   # 删除该杠杆ETF
                    else:
                        keep_mask.append(True)
                sorted_df = sorted_df[pd.Series(keep_mask, index=sorted_df.index)].copy()

                # 4) 同底层杠杆ETF去重（留成交额最大的一个）
                letf_idx_by_underlying: dict = {}
                for idx, row in sorted_df.iterrows():
                    name = str(row.get('name', ''))
                    underlying = _extract_letf_underlying(name)
                    if underlying:
                        letf_idx_by_underlying.setdefault(underlying, []).append(idx)
                for underlying, indices in letf_idx_by_underlying.items():
                    if len(indices) > 1:
                        best_idx = sorted_df.loc[indices, 'amount'].idxmax()
                        for idx in indices:
                            if idx != best_idx:
                                sorted_df = sorted_df.drop(idx)

        # 先取初筛 Top 50（多取一些供下游过滤）
        candidates = sorted_df.head(50).to_dict(orient="records")
        
        ranked = self._apply_news_intensity_rank(candidates)
        return ranked[:top_n]

    def _apply_news_intensity_rank(self, stocks: List[Dict]) -> List[Dict]:
        """对候选标的叠加新闻强度分，作为热度排序附加因子。"""
        if not stocks:
            return stocks

        weight = max(0.0, min(0.5, float(self.heat_w_news or 0)))

        # 新闻权重为 0 时直接用 heat_score 排序，跳过新闻查询
        if weight <= 0:
            for stock in stocks:
                stock["news_strength"] = 0.0
                stock["news_count_3d"] = 0
                stock["heat_score_v2"] = float(stock.get("heat_score", 0) or 0)
            stocks.sort(key=lambda x: (float(x.get("heat_score_v2", 0) or 0), float(x.get("pct_chg", 0) or 0)), reverse=True)
            return stocks

        lookback = max(1, int(self.heat_news_lookback_days or 3))

        def _fetch(stock: Dict):
            symbol = str(stock.get("symbol", "")).strip()
            if not symbol:
                return stock, {"intensity_score": 0.0, "total": 0}
            meta = summarize_symbol_news(symbol, lookback_days=lookback, max_items=18)
            return stock, meta

        workers = min(10, len(stocks))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            pairs = list(ex.map(_fetch, stocks))

        base_scores = pd.Series([float((st or {}).get("heat_score", 0) or 0) for st, _ in pairs])
        base_rank = base_scores.rank(pct=True) if base_scores.nunique() > 1 else pd.Series([0.5] * len(pairs))

        enriched: List[Dict] = []
        for i, (stock, meta) in enumerate(pairs):
            news_strength = float((meta or {}).get("intensity_score", 0) or 0)
            news_count = int((meta or {}).get("total", 0) or 0)
            stock["news_strength"] = round(news_strength, 3)
            stock["news_count_3d"] = news_count
            if weight > 0:
                stock["heat_score_v2"] = round((1 - weight) * float(base_rank.iloc[i]) + weight * news_strength, 4)
            else:
                stock["heat_score_v2"] = round(float(base_rank.iloc[i]), 4)
            enriched.append(stock)

        enriched.sort(key=lambda x: (float(x.get("heat_score_v2", 0) or 0), float(x.get("pct_chg", 0) or 0)), reverse=True)
        return enriched

    def _enrich_market_metrics(self, market: str, stocks: List[Dict]):
        """为推送标的补齐市值与资金流字段。"""
        if not stocks:
            return

        if market == "CN":
            trade_date = datetime.date.today().strftime("%Y%m%d")

            def _fetch_one(stock: Dict):
                symbol = str(stock.get("symbol", "")).strip()
                if not symbol:
                    return None
                metrics = get_cn_market_metrics(symbol)
                flow = get_cn_fund_flow(symbol, trade_date=trade_date)
                return stock, metrics, flow

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(stocks))) as ex:
                futures = [ex.submit(_fetch_one, s) for s in stocks]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        out = f.result()
                        if not out:
                            continue
                        stock, metrics, flow = out

                        if metrics:
                            total_mv = float(metrics.get("total_mv_100m", 0) or 0)
                            circ_mv = float(metrics.get("circ_mv_100m", 0) or 0)
                            turnover = float(metrics.get("turnover_rate", 0) or 0)

                            if total_mv > 0:
                                stock["total_mv_100m"] = total_mv
                            if circ_mv > 0:
                                stock["circ_mv_100m"] = circ_mv
                            if float(stock.get("turnover", 0) or 0) <= 0 and turnover > 0:
                                stock["turnover"] = turnover

                        if flow:
                            main_inflow = float(flow.get("main_net_inflow_100m", 0) or 0)
                            if main_inflow != 0:
                                stock["main_net_inflow_100m"] = main_inflow
                    except Exception:
                        continue
            return

        if market == "HK":
            def _fetch_one(stock: Dict):
                symbol = str(stock.get("symbol", "")).strip()
                if not symbol:
                    return None
                metrics = get_hk_market_metrics(symbol)
                return stock, metrics

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(stocks))) as ex:
                futures = [ex.submit(_fetch_one, s) for s in stocks]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        out = f.result()
                        if not out:
                            continue
                        stock, metrics = out
                        if not metrics:
                            continue

                        cap_hkd = float(metrics.get("market_cap_100m_hkd", 0) or 0)
                        cap_usd = float(metrics.get("market_cap_100m_usd", 0) or 0)
                        if cap_hkd > 0:
                            stock["market_cap_100m_hkd"] = cap_hkd
                        if cap_usd > 0:
                            stock["market_cap_100m_usd"] = cap_usd
                    except Exception:
                        continue

    def _format_cap_text(self, stock: Dict, market: str) -> str:
        if market == "CN":
            total_mv = float(stock.get("total_mv_100m", 0) or 0)
            circ_mv = float(stock.get("circ_mv_100m", 0) or 0)
            mv_txt = format_mv_cn(total_mv, circ_mv)
            return f"  🏦 {mv_txt}" if mv_txt else ""

        if market == "HK":
            cap_hkd = float(stock.get("market_cap_100m_hkd", 0) or 0)
            cap_usd = float(stock.get("market_cap_100m_usd", 0) or 0)
            mv_txt = format_mv_hk(cap_hkd, cap_usd)
            return f"  🏦 {mv_txt}" if mv_txt else ""

        if market == "US":
            cap_100m = float(stock.get("market_cap_100m_usd", 0) or 0)
            if cap_100m > 0:
                return f"  🏦 市值:{cap_100m:.1f}亿美元"

        return ""

    def _format_flow_text(self, stock: Dict, market: str) -> str:
        if market != "CN":
            return ""
        main_inflow = float(stock.get("main_net_inflow_100m", 0) or 0)
        flow_txt = format_flow_cn(main_inflow)
        return f"  💸 {flow_txt}" if flow_txt else ""

    def process_and_notify(self, market: str):
        """主入口：获取数据、计算热榜、获取归因、发送通知"""
        logger.info(f"Generating market heat map for {market}...")
        
        # 1. 获取行情
        df = pd.DataFrame()
        try:
            from modules.ingestion.akshare_client import AkShareClient
            if market == 'CN':
                # 东方财富接口(stock_zh_a_spot_em)在当前服务器被封，改用新浪批量接口
                # _fetch_bulk_sina 已被 HK/US 验证可用，CN同样支持
                df = AkShareClient._fetch_bulk_sina('CN')
            elif market == 'HK':
                df = akshare_client.get_stock_info_hk()
                if df.empty:
                    df = AkShareClient._to_rank_schema(AkShareClient._safe_call(["stock_hk_spot"]))
            elif market == 'US':
                df = akshare_client.get_stock_info_us()
                if df.empty:
                    df = AkShareClient._to_rank_schema(AkShareClient._safe_call(["stock_us_spot"]))
        except Exception as e:
            logger.error(f"Failed to fetch market data for {market}: {e}")
            return
            
        # 2. 生成榜单配置：TG 推送仍保持精简，Web 当日强势额外落库更宽候选池。
        min_amt = 50_000_000   # CN/HK: 5000万人民币
        push_n = 10
        persist_n = int(os.getenv("DAILY_HOT_PERSIST_TOP_N", "80") or 80)
        persist_n = max(push_n, min(200, persist_n))
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
            push_n = 5

        persist_stocks = self._generate_heatmap(df, market, top_n=persist_n, min_amount=min_amt)
        top_stocks = persist_stocks[:push_n]
        
        if not top_stocks:
            logger.warning(f"No stocks found for {market} heat map. Fallback to daily top ranks.")
            fallback_df = akshare_client.get_daily_top_ranks(
                market=market, rank_type="change_pct", top_n=persist_n
            )
            if fallback_df.empty:
                logger.warning(f"Fallback daily ranks still empty for {market}.")
                return
            persist_stocks = [
                {
                    "symbol": str(row.get("symbol", "")),
                    "name": str(row.get("name", "")),
                    "price": float(row.get("price", 0) or 0),
                    "pct_chg": float(row.get("change_pct", 0) or 0),
                    "amount": float(row.get("amount", 0) or 0),
                    "turnover": float(row.get("turnover_rate", 0) or 0),
                }
                for _, row in fallback_df.iterrows()
            ]
            top_stocks = persist_stocks[:push_n]

        persist_results = [dict(stock) for stock in persist_stocks]

        # 3. 日报智能归因（默认关闭）
        results = []
        if self.enable_daily_attribution:
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

            for f in concurrent.futures.as_completed(futures):
                stock = futures[f]
                reason = f.result()
                stock['reason'] = reason
                results.append(stock)
        else:
            for stock in top_stocks:
                enriched = dict(stock)
                enriched["reason"] = ""
                results.append(enriched)
            
        # 还原回按涨幅排序 (因为 as_completed 不保证顺序)
        results.sort(key=lambda x: x['pct_chg'], reverse=True)

        # 补齐市值字段（用于推送展示）
        self._enrich_market_metrics(market, results)

        # 细分行业标签（保留作为催化剂全空时的最终兜底）
        try:
            enrich_industry_labels(results, market)
        except Exception as e:
            logger.warning(f"Failed to enrich industry labels ({market}): {e}")

        # 催化剂(马甲)标签
        try:
            from modules.monitor.catalyst_service import resolve_catalysts
            resolve_catalysts(results, market)
        except Exception as e:
            logger.warning(f"Failed to resolve catalysts ({market}): {e}")
            for s in results:
                s.setdefault("catalyst_tags", "催化剂解析异常")
                s.setdefault("catalyst_source", f"error:{e}")

        # 技术形态标签
        try:
            from modules.monitor.pattern_tagger import tag_patterns
            tag_patterns(results, market)
        except Exception as e:
            logger.warning(f"Failed to tag patterns ({market}): {e}")
            for s in results:
                s.setdefault("pattern_tag", "")

        # 4. 宽候选池落库给 Web 当日强势；TG 推送仍只使用 results。
        persist_map = {str(r.get("symbol", "")): dict(r) for r in persist_results if str(r.get("symbol", ""))}
        for r in results:
            sym = str(r.get("symbol", ""))
            if sym:
                persist_map[sym] = dict(r)
        persist_rows = sorted(persist_map.values(), key=lambda x: float(x.get("pct_chg", 0) or 0), reverse=True)

        try:
            self._persist_daily_rank_from_heatmap(market, results, rank_type="change_pct")
            self._persist_daily_rank_from_heatmap(market, persist_rows, rank_type="daily_hot")
        except Exception as e:
            logger.error(f"Failed to persist DailyRank from heatmap ({market}): {e}")
        
        # 5. 存入长线趋势种子池和日线快照；heatmap 保持 TG 原口径，daily_hot 供 Web 扩容。
        try:
            from modules.monitor.trend_service import TrendService
            pool_items = []
            for r in results:
                reason = str(r.get("reason", "") or "").strip()
                catalyst = str(r.get("catalyst_tags", "") or "").strip()
                pattern = str(r.get("pattern_tag", "") or "").strip()
                if not reason:
                    if catalyst and pattern:
                        reason = f"{catalyst} | {pattern}"
                    else:
                        reason = catalyst or pattern or ""
                pool_items.append({"symbol": r["symbol"], "name": r["name"], "reason": reason})
            TrendService.add_to_pool(market, pool_items)

            def _to_bar_items(rows):
                return [
                    {
                        "symbol": r.get("symbol", ""),
                        "name": r.get("name", ""),
                        "price": float(r.get("price", 0) or 0),
                        "pct_chg": float(r.get("pct_chg", 0) or 0),
                        "amount": float(r.get("amount", 0) or 0),
                        "turnover_rate": float(r.get("turnover", 0) or 0),
                        "catalyst_tags": str(r.get("catalyst_tags", "") or ""),
                    }
                    for r in rows
                ]

            TrendService.save_daily_bars(market, _to_bar_items(results), source="heatmap")
            TrendService.save_daily_bars(market, _to_bar_items(persist_rows), source="daily_hot")
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

        if self.enable_market_brief:
            try:
                brief = self._get_market_brief(market)
                if brief:
                    msg_lines.append(f"🧭 市场简析: {brief}")
            except Exception as e:
                logger.warning("Build market brief failed for %s: %s", market, e)

        msg_lines.append("")
        
        for i, stock in enumerate(results, 1):
            name = stock['name']
            symbol = stock['symbol']
            pct = stock['pct_chg']
            price = stock.get('price', 0)
            if market == 'US':
                display = f"({symbol})"
            else:
                display = f"{name} ({symbol})"
            # 需求1: 推送标的加上具体价格
            price_str = f"{price:.2f}" if price else "N/A"
            cap_text = self._format_cap_text(stock, market)
            flow_text = self._format_flow_text(stock, market)
            fomo_text = "  ⚠️ [高位派发疑似]" if bool(stock.get("fomo_flag", False)) else ""

            # 催化剂(马甲)
            catalyst = str(stock.get("catalyst_tags", "") or "").strip()
            # 催化剂为空时回退到行业标签
            if not catalyst:
                ind = str(stock.get("industry_label", "") or "").strip()
                catalyst = ind if ind else ""
            catalyst_text = f"  马甲:[{catalyst}]" if catalyst else ""

            msg_lines.append(f"**{i}. {display}**  💰 现价: {price_str}  `{pct:+.2f}%`{cap_text}{flow_text}{catalyst_text}{fomo_text}")
            if self.enable_daily_attribution:
                reason = stock.get('reason', '')
                msg_lines.append(f"💡 {reason}\n")
            else:
                msg_lines.append("")
            
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
