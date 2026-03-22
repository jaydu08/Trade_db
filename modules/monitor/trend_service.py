import logging
import datetime as dt
import re
import concurrent.futures
from difflib import SequenceMatcher
from sqlmodel import select
from typing import List, Dict, Tuple

from core.db import get_ledger_session
from domain.ledger.analytics import TrendSeedPool, DailyRank, TrendDailyBar

logger = logging.getLogger(__name__)

class TrendService:
    EOD_SOURCES = {"daily_rank", "trend_pool_refresh_eod", "commodity", "heatmap", "selftest"}
    POOL_RETENTION_DAYS = 60
    POOL_SYMBOL_CAPS = {"CN": 100, "US": 100, "HK": 50, "CF": 30}

    @staticmethod
    def _enforce_pool_symbol_cap(session, market: str):
        """按市场限制 trend 种子池标的数量，优先保留最近出现的标的。"""
        cap = TrendService.POOL_SYMBOL_CAPS.get(market)
        if not cap:
            return

        cutoff = dt.date.today() - dt.timedelta(days=TrendService.POOL_RETENTION_DAYS)
        rows = session.exec(
            select(TrendSeedPool).where(
                TrendSeedPool.market == market,
                TrendSeedPool.date >= cutoff,
            )
        ).all()
        if not rows:
            return

        last_seen = {}
        for r in rows:
            prev = last_seen.get(r.symbol)
            if prev is None or r.date > prev:
                last_seen[r.symbol] = r.date

        if len(last_seen) <= cap:
            return

        keep_symbols = {
            sym
            for sym, _ in sorted(last_seen.items(), key=lambda x: (x[1], x[0]), reverse=True)[:cap]
        }
        drop_symbols = set(last_seen.keys()) - keep_symbols
        if not drop_symbols:
            return

        to_delete = session.exec(
            select(TrendSeedPool).where(
                TrendSeedPool.market == market,
                TrendSeedPool.symbol.in_(list(drop_symbols)),
            )
        ).all()
        for row in to_delete:
            session.delete(row)
        logger.info(
            "TrendSeedPool cap enforced: market=%s cap=%s dropped_symbols=%s",
            market,
            cap,
            len(drop_symbols),
        )

    @staticmethod
    def _is_anomalous_close(
        session,
        market: str,
        symbol: str,
        close: float,
        pct_chg: float,
        sample_n: int = 20,
    ) -> bool:
        """
        港/美价格异常保护：
        - 与近期中位数相比出现异常倍数跳变
        - 且当日涨跌幅并未体现同等异常（通常意味着口径错位）
        """
        if market not in {"HK", "US"} or close <= 0:
            return False

        stmt = (
            select(TrendDailyBar)
            .where(TrendDailyBar.market == market)
            .where(TrendDailyBar.symbol == symbol)
            .order_by(TrendDailyBar.date.desc())
        )
        rows = list(session.exec(stmt).all())[:sample_n]
        hist = [float(r.close) for r in rows if r.close and float(r.close) > 0]
        if len(hist) < 5:
            return False

        hist_sorted = sorted(hist)
        median = hist_sorted[len(hist_sorted) // 2]
        if median <= 0:
            return False

        ratio = close / median
        # 异常价倍数 + 非异常涨跌幅 => 高概率为错误口径（如币种错位）
        if (ratio > 2.5 or ratio < 0.4) and abs(float(pct_chg or 0)) <= 25:
            logger.warning(
                "TrendDailyBar anomaly blocked: market=%s symbol=%s close=%s median=%s ratio=%.3f pct=%s",
                market, symbol, close, median, ratio, pct_chg,
            )
            return True
        return False

    @staticmethod
    def _pick_trusted_quote(symbol: str, market: str):
        """
        趋势场景下的可信行情源策略：
        - US: 仅信任 Finnhub（避免其他源口径偏差）
        - 其他市场: 接受 DataManager 正常返回
        """
        from modules.ingestion.data_factory import data_manager
        quote = data_manager.get_quote(symbol, market)
        if not quote:
            return None
        if market == "US" and quote.get("provider") != "Finnhub":
            return None
        return quote

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
                # 自动清理 60 天前的数据
                cutoff = today - dt.timedelta(days=TrendService.POOL_RETENTION_DAYS)
                del_stmt = select(TrendSeedPool).where(TrendSeedPool.date < cutoff)
                old_records = session.exec(del_stmt).all()
                for old in old_records:
                    session.delete(old)

                # 硬顶控制：按市场限制池内标的总数
                TrendService._enforce_pool_symbol_cap(session, market)
                
                # context manager 将在退出时自动 commit
            logger.info(f"TrendSeedPool: Saved {len(items)} items for {market}")
        except Exception as e:
            logger.error(f"Failed to add to TrendSeedPool: {e}")

    @staticmethod
    def save_daily_bars(market: str, items: List[Dict], source: str = ""):
        """
        保存趋势标的的每日行情快照（滚动保留 180 天）。
        items: [{"symbol","name","price","pct_chg","amount","turnover_rate","open","high","low"}]
        """
        today = dt.date.today()
        cutoff = today - dt.timedelta(days=180)

        try:
            if source not in TrendService.EOD_SOURCES:
                logger.info(
                    "Skip TrendDailyBar write for non-EOD source: market=%s source=%s items=%s",
                    market,
                    source,
                    len(items),
                )
                return
            with get_ledger_session() as session:
                for item in items:
                    sym = str(item.get("symbol", "")).strip()
                    if not sym:
                        continue

                    close = float(item.get("price", 0) or 0)
                    if close <= 0:
                        continue

                    pct = float(item.get("pct_chg", 0) or 0)
                    open_price = float(item.get("open", 0) or 0)
                    if open_price <= 0:
                        den = 1 + pct / 100.0
                        open_price = close / den if den > 0 else close

                    high = float(item.get("high", 0) or 0)
                    low = float(item.get("low", 0) or 0)
                    amount = float(item.get("amount", 0) or 0)
                    turnover_rate = float(item.get("turnover_rate", 0) or 0)

                    # US 优先用可信实时源覆盖（防止口径偏差）
                    if market == "US":
                        trusted = TrendService._pick_trusted_quote(sym, market)
                        if trusted and float(trusted.get("price", 0) or 0) > 0:
                            close = float(trusted.get("price"))
                            pct = float(trusted.get("pct_chg", pct) or pct)
                            if open_price <= 0:
                                den = 1 + pct / 100.0
                                open_price = close / den if den > 0 else close

                    # 价格异常保护（港/美）
                    if TrendService._is_anomalous_close(session, market, sym, close, pct):
                        continue

                    stmt = select(TrendDailyBar).where(
                        TrendDailyBar.date == today,
                        TrendDailyBar.market == market,
                        TrendDailyBar.symbol == sym,
                    )
                    existing = session.exec(stmt).first()

                    if existing:
                        # 当日已写入则不回写，保持“落盘一次”语义
                        continue
                    else:
                        bar = TrendDailyBar(
                            date=today,
                            market=market,
                            symbol=sym,
                            name=item.get("name", ""),
                            close=close,
                            open=open_price,
                            high=high if high > 0 else max(close, open_price),
                            low=low if low > 0 else min(close, open_price),
                            amount=amount,
                            turnover_rate=turnover_rate,
                            source=source,
                        )
                        session.add(bar)

                old_stmt = select(TrendDailyBar).where(TrendDailyBar.date < cutoff)
                for row in session.exec(old_stmt).all():
                    session.delete(row)
            logger.info(f"TrendDailyBar: saved {len(items)} items for {market}, source={source}")
        except Exception as e:
            logger.error(f"Failed to save TrendDailyBar for {market}: {e}")

    @staticmethod
    def refresh_pool_daily_bars(
        markets: List[str],
        lookback_days: int = 60,
        source: str = "trend_pool_refresh_eod",
        alert_on_zero: bool = False,
    ) -> Dict[str, Dict[str, int]]:
        """
        每日补齐趋势池标的的快照价格（按市场批量拉取实时行情）。
        目标：即使标的不在当日热榜，也能持续更新 trend 价格序列。
        """
        from modules.ingestion.data_factory import data_manager
        today = dt.date.today()
        cutoff = today - dt.timedelta(days=lookback_days)
        summary: Dict[str, Dict[str, int]] = {}

        with get_ledger_session() as session:
            stmt = select(TrendSeedPool).where(TrendSeedPool.date >= cutoff)
            rows = session.exec(stmt).all()
            records = [
                {"market": r.market, "symbol": r.symbol, "name": r.name}
                for r in rows
                if r.market in markets and r.symbol
            ]

        by_market: Dict[str, Dict[str, str]] = {}
        for r in records:
            by_market.setdefault(r["market"], {})
            by_market[r["market"]][r["symbol"]] = r.get("name", "")

        for market in markets:
            sym_map = by_market.get(market, {})
            if not sym_map:
                summary[market] = {"candidates": 0, "quoted": 0, "saved": 0}
                continue

            symbols = list(sym_map.keys())
            payloads: List[Dict] = []

            def _fetch(sym: str):
                quote = TrendService._pick_trusted_quote(sym, market)
                if not quote:
                    return None
                price = float(quote.get("price", 0) or 0)
                if price <= 0:
                    return None
                return {
                    "symbol": sym,
                    "name": sym_map.get(sym, ""),
                    "price": price,
                    "pct_chg": float(quote.get("pct_chg", 0) or 0),
                    "amount": 0.0,
                    "turnover_rate": 0.0,
                }

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(symbols))) as executor:
                future_map = {executor.submit(_fetch, sym): sym for sym in symbols}
                try:
                    done_iter = concurrent.futures.as_completed(future_map, timeout=120)
                    for future in done_iter:
                        try:
                            item = future.result(timeout=0)
                            if item:
                                payloads.append(item)
                        except Exception:
                            continue
                except concurrent.futures.TimeoutError:
                    logger.warning("Trend pool refresh timeout for market=%s, use partial results.", market)

            if payloads:
                TrendService.save_daily_bars(market, payloads, source=source)

            summary[market] = {
                "candidates": len(symbols),
                "quoted": len(payloads),
                "saved": len(payloads),
            }

            if len(symbols) > 0 and len(payloads) == 0:
                logger.error(
                    "Trend pool EOD refresh got zero quotes: market=%s candidates=%s",
                    market,
                    len(symbols),
                )

        logger.info("Trend pool daily refresh finished: %s", summary)
        if alert_on_zero:
            try:
                from modules.monitor.notifier import Notifier
                bad = [
                    f"{m}:0/{v.get('candidates', 0)}"
                    for m, v in summary.items()
                    if v.get("candidates", 0) > 0 and v.get("quoted", 0) == 0
                ]
                if bad:
                    Notifier.broadcast(
                        "⚠️ Trend收盘价采集异常："
                        + "，".join(bad)
                        + "（请检查行情源网络/DNS）"
                    )
            except Exception as e:
                logger.warning("Trend refresh alert failed: %s", e)
        return summary

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
    def _get_return_from_daily_rank_db(symbol: str, market: str, d_days: int) -> Tuple[float, float, str]:
        """接口失败时，回退使用本地 DailyRank 价格序列计算 N 日收益"""
        if market == "CF":
            return 0.0, 0.0, ""

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
            # 在 session 生命周期内提取纯数据，避免 detached ORM 访问
            rows = [(r.date, float(r.price)) for r in session.exec(stmt).all() if r.price and r.price > 0]

        # 去重并保留有效价格
        uniq = {}
        for d, p in rows:
            uniq[d] = p
        if not uniq:
            return 0.0, 0.0, ""

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
            return 0.0, round(current_price, 4), str(current_date)
        ret = round((current_price - past_price) / past_price * 100, 2)
        return ret, round(current_price, 4), str(current_date)

    @staticmethod
    def _get_return_from_daily_bar_db(symbol: str, market: str, d_days: int) -> Tuple[float, float, str]:
        """优先使用本地 TrendDailyBar 序列计算 N 日收益"""
        raw = str(symbol or "").strip()
        candidates = [raw]
        if market == "US" and "." in raw:
            candidates.append(raw.split(".")[-1].strip())

        with get_ledger_session() as session:
            stmt = (
                select(TrendDailyBar)
                .where(TrendDailyBar.market == market)
                .where(TrendDailyBar.symbol.in_(candidates))
                .order_by(TrendDailyBar.date.desc())
            )
            # 在 session 生命周期内提取纯数据，避免 detached ORM 访问
            rows = [(r.date, float(r.close)) for r in session.exec(stmt).all() if r.close and r.close > 0]

        uniq = {}
        for d, c in rows:
            uniq[d] = c
        if not uniq:
            return 0.0, 0.0, ""

        dates = sorted(uniq.keys())
        current_date = dates[-1]
        current_price = uniq[current_date]
        target_date = current_date - dt.timedelta(days=d_days)
        past_dates = [d for d in dates if d <= target_date]
        past_price = uniq[past_dates[-1]] if past_dates else uniq[dates[0]]

        if past_price <= 0:
            return 0.0, round(current_price, 4), str(current_date)
        ret = round((current_price - past_price) / past_price * 100, 2)
        return ret, round(current_price, 4), str(current_date)

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
    def _get_return(symbol: str, market: str, d_days: int) -> Tuple[float, float, str]:
        """获取标的 N 天前累计涨幅和现价（仅基于本地收盘价库）"""
        # 先走本地 TrendDailyBar（收盘快照）
        try:
            db_ret = TrendCalculator._get_return_from_daily_bar_db(symbol, market, d_days)
            if db_ret[:2] != (0.0, 0.0):
                return db_ret
        except Exception as e:
            logger.debug(f"TrendDailyBar fallback failed for {symbol} ({market}): {e}")

        # 再回退 DailyRank 历史价格序列；不再调用外部实时/历史接口，避免口径和可达性波动
        try:
            return TrendCalculator._get_return_from_daily_rank_db(symbol, market, d_days)
        except Exception as e2:
            logger.debug(f"DailyRank fallback failed for {symbol} ({market}): {e2}")
            return 0.0, 0.0, ""

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
            ret, current_price, price_date = TrendCalculator._get_return(item["symbol"], item["market"], days)
            item["return_pct"] = ret
            item["current_price"] = current_price
            item["price_date"] = price_date

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
