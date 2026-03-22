import logging
import datetime as dt
import re
import concurrent.futures
from difflib import SequenceMatcher
from sqlmodel import select
from typing import List, Dict, Tuple, Optional

from core.db import get_ledger_session
from domain.ledger.analytics import TrendSeedPool, DailyRank, TrendDailyBar

logger = logging.getLogger(__name__)

class TrendService:
    EOD_SOURCES = {
        "daily_rank",
        "trend_pool_refresh_eod",
        "trend_pool_history_backfill",
        "commodity",
        "heatmap",
        "selftest",
    }
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
    def _to_date(value) -> Optional[dt.date]:
        if value is None:
            return None
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        s = str(value).strip()
        if not s:
            return None
        try:
            return dt.datetime.fromisoformat(s[:10]).date()
        except Exception:
            return None

    @staticmethod
    def _normalize_symbol_for_history(symbol: str, market: str) -> str:
        sym = str(symbol or "").strip()
        if market == "US" and "." in sym:
            sym = sym.split(".")[-1].strip()
        return sym

    @staticmethod
    def _fetch_symbol_history(market: str, symbol: str):
        """
        获取单标的历史日线（优先项目内已验证可用接口）。
        """
        import akshare as ak

        sym = TrendService._normalize_symbol_for_history(symbol, market)
        if not sym:
            return None

        if market == "CN":
            prefixed = f"sh{sym}" if str(sym).startswith("6") else f"sz{sym}"
            try:
                return ak.stock_zh_a_daily(symbol=prefixed)
            except Exception:
                try:
                    return ak.stock_zh_a_hist(symbol=sym, period="daily")
                except Exception:
                    return None
        if market == "HK":
            try:
                return ak.stock_hk_daily(symbol=sym)
            except Exception:
                return None
        if market == "US":
            try:
                return ak.stock_us_daily(symbol=sym)
            except Exception:
                return None
        if market == "CF":
            try:
                return ak.futures_zh_daily_sina(symbol=sym)
            except Exception:
                return None
        return None

    @staticmethod
    def _build_history_items(market: str, symbol: str, name: str, df, cutoff: dt.date) -> List[Dict]:
        if df is None or getattr(df, "empty", True):
            return []

        # 字段兼容（akshare 各接口列名有差异）
        cols = set(str(c).lower() for c in list(df.columns))
        date_col = "date" if "date" in cols else ("日期" if "日期" in df.columns else None)
        open_col = "open" if "open" in cols else ("开盘" if "开盘" in df.columns else None)
        high_col = "high" if "high" in cols else ("最高" if "最高" in df.columns else None)
        low_col = "low" if "low" in cols else ("最低" if "最低" in df.columns else None)
        close_col = "close" if "close" in cols else ("收盘" if "收盘" in df.columns else None)
        amount_col = "amount" if "amount" in cols else ("成交额" if "成交额" in df.columns else None)
        turnover_col = "turnover" if "turnover" in cols else ("换手率" if "换手率" in df.columns else None)

        if not date_col or not close_col:
            return []

        items: List[Dict] = []
        for _, row in df.iterrows():
            d = TrendService._to_date(row.get(date_col))
            if not d or d < cutoff:
                continue
            close = float(row.get(close_col, 0) or 0)
            if close <= 0:
                continue
            open_price = float(row.get(open_col, 0) or 0) if open_col else close
            high = float(row.get(high_col, 0) or 0) if high_col else max(close, open_price)
            low = float(row.get(low_col, 0) or 0) if low_col else min(close, open_price)
            amount = float(row.get(amount_col, 0) or 0) if amount_col else 0.0
            turnover = float(row.get(turnover_col, 0) or 0) if turnover_col else 0.0
            pct = 0.0
            if open_price > 0:
                pct = round((close - open_price) / open_price * 100, 4)
            items.append(
                {
                    "date": d,
                    "symbol": symbol,
                    "name": name or "",
                    "open": open_price if open_price > 0 else close,
                    "high": high if high > 0 else max(close, open_price),
                    "low": low if low > 0 else min(close, open_price),
                    "close": close,
                    "amount": amount,
                    "turnover_rate": turnover,
                    "pct_chg": pct,
                }
            )
        return items

    @staticmethod
    def _save_historical_bars(market: str, symbol: str, items: List[Dict], source: str) -> int:
        """
        仅插入缺失历史，不覆盖已有记录。
        """
        if not items:
            return 0
        cutoff = dt.date.today() - dt.timedelta(days=180)
        saved = 0
        try:
            with get_ledger_session() as session:
                existing_dates = {
                    r.date
                    for r in session.exec(
                        select(TrendDailyBar).where(
                            TrendDailyBar.market == market,
                            TrendDailyBar.symbol == symbol,
                            TrendDailyBar.date >= cutoff,
                        )
                    ).all()
                }

                for item in sorted(items, key=lambda x: x["date"]):
                    d = item["date"]
                    if d in existing_dates:
                        continue
                    close = float(item.get("close", 0) or 0)
                    if close <= 0:
                        continue
                    pct = float(item.get("pct_chg", 0) or 0)
                    if source != "trend_pool_history_backfill":
                        if TrendService._is_anomalous_close(session, market, symbol, close, pct):
                            continue
                    open_price = float(item.get("open", 0) or 0)
                    base_open = open_price if open_price > 0 else close
                    high = float(item.get("high", 0) or 0)
                    low = float(item.get("low", 0) or 0)
                    amount = float(item.get("amount", 0) or 0)
                    turnover = float(item.get("turnover_rate", 0) or 0)
                    session.add(
                        TrendDailyBar(
                            date=d,
                            market=market,
                            symbol=symbol,
                            name=str(item.get("name", "") or ""),
                            open=base_open,
                            high=high if high > 0 else max(close, base_open),
                            low=low if low > 0 else min(close, base_open),
                            close=close,
                            amount=amount,
                            turnover_rate=turnover,
                            source=source,
                        )
                    )
                    existing_dates.add(d)
                    saved += 1

                old_rows = session.exec(
                    select(TrendDailyBar).where(TrendDailyBar.market == market, TrendDailyBar.date < cutoff)
                ).all()
                for row in old_rows:
                    session.delete(row)
        except Exception as e:
            logger.error("Failed saving historical bars: market=%s symbol=%s err=%s", market, symbol, e)
            return 0
        return saved

    @staticmethod
    def _get_symbols_need_backfill(market: str, symbols: List[str], cutoff: dt.date, lookback_days: int) -> List[str]:
        if not symbols:
            return []
        earliest: Dict[str, dt.date] = {}
        with get_ledger_session() as session:
            rows = session.exec(
                select(TrendDailyBar.symbol, TrendDailyBar.date).where(
                    TrendDailyBar.market == market,
                    TrendDailyBar.symbol.in_(symbols),
                    TrendDailyBar.date >= cutoff,
                )
            ).all()
        for sym, d in rows:
            if sym not in earliest or d < earliest[sym]:
                earliest[sym] = d

        tolerance_days = 3 if lookback_days >= 14 else 1
        threshold = cutoff + dt.timedelta(days=tolerance_days)
        return [sym for sym in symbols if sym not in earliest or earliest[sym] > threshold]

    @staticmethod
    def backfill_pool_history(markets: List[str], lookback_days: int = 60) -> Dict[str, Dict[str, int]]:
        """
        给趋势池标的补齐近 lookback_days 天的历史交易日收盘序列。
        只插入缺失日期，不覆盖已落盘数据。
        """
        today = dt.date.today()
        cutoff = today - dt.timedelta(days=lookback_days)
        summary: Dict[str, Dict[str, int]] = {}

        with get_ledger_session() as session:
            rows = session.exec(select(TrendSeedPool).where(TrendSeedPool.date >= cutoff)).all()
            recs = [
                {"market": r.market, "symbol": r.symbol, "name": r.name}
                for r in rows
                if r.market in markets and r.symbol
            ]

        by_market: Dict[str, Dict[str, str]] = {}
        for r in recs:
            by_market.setdefault(r["market"], {})
            by_market[r["market"]][r["symbol"]] = r.get("name", "")

        for market in markets:
            sym_map = by_market.get(market, {})
            symbols = list(sym_map.keys())
            if not symbols:
                summary[market] = {"candidates": 0, "targets": 0, "saved_rows": 0}
                continue

            targets = TrendService._get_symbols_need_backfill(market, symbols, cutoff, lookback_days)
            saved_rows = 0

            def _work(sym: str) -> int:
                df = TrendService._fetch_symbol_history(market, sym)
                items = TrendService._build_history_items(market, sym, sym_map.get(sym, ""), df, cutoff)
                return TrendService._save_historical_bars(market, sym, items, source="trend_pool_history_backfill")

            if targets:
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(6, len(targets))) as executor:
                    future_map = {executor.submit(_work, sym): sym for sym in targets}
                    for fut in concurrent.futures.as_completed(future_map, timeout=360):
                        try:
                            saved_rows += int(fut.result() or 0)
                        except Exception:
                            continue

            summary[market] = {
                "candidates": len(symbols),
                "targets": len(targets),
                "saved_rows": saved_rows,
            }

        logger.info("Trend pool history backfill finished: %s", summary)
        return summary

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
        backfill_history: bool = True,
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
                summary[market] = {"candidates": 0, "quoted": 0, "saved": 0, "backfill_targets": 0, "backfill_saved_rows": 0}
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
                "backfill_targets": 0,
                "backfill_saved_rows": 0,
            }

            if len(symbols) > 0 and len(payloads) == 0:
                logger.error(
                    "Trend pool EOD refresh got zero quotes: market=%s candidates=%s",
                    market,
                    len(symbols),
                )

        if backfill_history:
            try:
                backfill = TrendService.backfill_pool_history(markets, lookback_days=lookback_days)
                for market, payload in backfill.items():
                    summary.setdefault(market, {})
                    summary[market]["backfill_targets"] = payload.get("targets", 0)
                    summary[market]["backfill_saved_rows"] = payload.get("saved_rows", 0)
            except Exception as e:
                logger.error("Trend pool history backfill failed: %s", e, exc_info=True)

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
