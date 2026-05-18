import datetime as dt
import logging
import os
import sqlite3
from typing import Dict, List

import requests

from modules.monitor.notifier import Notifier

logger = logging.getLogger(__name__)


class USPremarketScannerService:
    """美股盘前猎手：扫描高成交额+大波动+大市值标的并推送。
    
    数据源: Sina美股行情API (替代已被限流的Yahoo Finance v7)
    市值: 复用 Finnhub/Yahoo 缓存 (get_us_market_metrics)
    """

    SINA_HQ_URL = "http://hq.sinajs.cn/list="
    SINA_HEADERS = {"Referer": "http://finance.sina.com.cn"}

    @staticmethod
    def _to_float(v, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _normalize_us_symbol(symbol: str) -> str:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return ""
        if "." in sym and sym.split(".", 1)[0].isdigit():
            sym = sym.split(".")[-1].strip()
        # Sina uses "$" for class-share separators, but accepts gb_brk$b.
        sym = sym.replace("/", ".").replace("-", ".")
        return sym

    @staticmethod
    def _is_excluded_us_asset(symbol: str, name: str = "") -> bool:
        """Drop obvious non-common-stock symbols before pre-market scanning."""
        sym = USPremarketScannerService._normalize_us_symbol(symbol)
        if not sym or len(sym) > 10:
            return True
        low = str(name or "").lower()
        upper = sym.upper()

        leveraged_tickers = {
            "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "TNA", "TZA",
            "UVXY", "SVXY", "LABU", "LABD", "BOIL", "KOLD", "DUST", "NUGT",
            "JNUG", "JDST", "FAS", "FAZ", "TECL", "TECS", "WEBL", "WEBS",
            "FNGU", "FNGD", "YINN", "YANG", "NVD", "NVDL", "NVDS", "NVDU",
            "TSLL", "TSLS", "TSLQ", "TSDD", "TSLZ", "TSLT", "MSTX", "MSTZ",
            "MSTU", "MUU", "AMDL", "AMDS", "BABX", "AAPU", "AAPD", "GGLL",
            "GGLS", "CONL", "ZSL", "AGQ", "UCO", "SCO", "GUSH", "DRIP",
        }
        if upper in leveraged_tickers:
            return True
        if any(x in upper for x in ["_WS", ".WS", "_WT", ".WT", "_RT", ".RT"]):
            return True
        if upper.endswith((".U", "_U")):
            return True
        # Five-character tickers ending in W/WW are overwhelmingly warrants in this universe.
        if len(upper) >= 5 and upper.endswith("W"):
            return True
        if any(k in low for k in ["warrant", " wt", " right", " unit", " units"]):
            return True
        if any(k in low for k in ["2x", "3x", "bull", "bear", "inverse", "leveraged"]):
            return True
        # Most ETF/ETN products do not have useful equity market-cap semantics for this scanner.
        if any(k in low for k in [" etf", "etf ", " etn", "etn "]):
            return True
        return False

    @staticmethod
    def _load_us_symbols(limit: int) -> List[str]:
        """Build a quality-first US pre-market universe.

        Priority order: large-cap baseline, user/watch positions, recent trend seeds,
        recent high-turnover ranks/bars, then filtered meta.db fallback.
        """
        max_total = max(50, int(limit or 1800))
        max_rank = int(os.getenv("US_PREMARKET_RANK_SYMBOL_LIMIT", "900") or 900)
        max_bar = int(os.getenv("US_PREMARKET_BAR_SYMBOL_LIMIT", "900") or 900)
        max_seed = int(os.getenv("US_PREMARKET_SEED_SYMBOL_LIMIT", "400") or 400)

        out: List[str] = []
        seen = set()
        stats = {"baseline": 0, "manual": 0, "seed": 0, "rank": 0, "bar": 0, "meta": 0, "excluded": 0}

        def add(symbol: str, name: str = "", source: str = "meta"):
            if len(out) >= max_total:
                return
            sym = USPremarketScannerService._normalize_us_symbol(symbol)
            if (not sym) or sym in seen:
                return
            if USPremarketScannerService._is_excluded_us_asset(sym, name):
                stats["excluded"] += 1
                return
            seen.add(sym)
            out.append(sym)
            stats[source] = stats.get(source, 0) + 1

        try:
            from modules.monitor.trend_service import TrendService

            for item in TrendService._baseline_seed_items().get("US", []):
                add(item.get("symbol", ""), item.get("name", ""), "baseline")

            for source_items in (TrendService._watchlist_seed_items(), TrendService._paper_trade_seed_items()):
                for item in source_items.get("US", []):
                    add(item.get("symbol", ""), item.get("name", ""), "manual")
        except Exception as e:
            logger.debug("US premarket: priority symbols failed: %s", e)

        try:
            from core.db import get_ledger_session
            from domain.ledger.analytics import DailyRank, TrendDailyBar, TrendSeedPool
            from sqlmodel import select

            today = dt.date.today()
            with get_ledger_session() as session:
                seed_rows = session.exec(
                    select(TrendSeedPool.symbol, TrendSeedPool.name)
                    .where(TrendSeedPool.market == "US")
                    .where(TrendSeedPool.date >= today - dt.timedelta(days=120))
                    .order_by(TrendSeedPool.date.desc())
                    .limit(max_seed * 3)
                ).all()
                for symbol, name in seed_rows:
                    add(symbol, name, "seed")
                    if stats["seed"] >= max_seed:
                        break

                rank_rows = session.exec(
                    select(DailyRank.symbol, DailyRank.name)
                    .where(DailyRank.market == "US")
                    .where(DailyRank.date >= today - dt.timedelta(days=30))
                    .order_by(DailyRank.amount.desc(), DailyRank.date.desc())
                    .limit(max_rank * 3)
                ).all()
                for symbol, name in rank_rows:
                    add(symbol, name, "rank")
                    if stats["rank"] >= max_rank:
                        break

                bar_rows = session.exec(
                    select(TrendDailyBar.symbol, TrendDailyBar.name)
                    .where(TrendDailyBar.market == "US")
                    .where(TrendDailyBar.date >= today - dt.timedelta(days=60))
                    .order_by(TrendDailyBar.amount.desc(), TrendDailyBar.date.desc())
                    .limit(max_bar * 3)
                ).all()
                for symbol, name in bar_rows:
                    add(symbol, name, "bar")
                    if stats["bar"] >= max_bar:
                        break
        except Exception as e:
            logger.warning("US premarket: local high-liquidity universe failed: %s", e)

        try:
            conn = sqlite3.connect("data/meta.db")
            cur = conn.cursor()
            cur.execute("SELECT symbol, name FROM asset WHERE market=?", ("US",))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            logger.warning("US premarket: load meta fallback failed: %s", e)
            rows = []

        for row in rows:
            if len(out) >= max_total:
                break
            symbol = str((row or [""])[0] or "")
            name = str((row or ["", ""])[1] or "") if len(row or []) > 1 else ""
            add(symbol, name, "meta")

        logger.info(
            "US premarket universe built: total=%s baseline=%s manual=%s seed=%s rank=%s bar=%s meta=%s excluded=%s limit=%s",
            len(out), stats.get("baseline", 0), stats.get("manual", 0), stats.get("seed", 0),
            stats.get("rank", 0), stats.get("bar", 0), stats.get("meta", 0), stats.get("excluded", 0), max_total,
        )
        return out

    @staticmethod
    def _fetch_sina_batch(symbols: List[str], timeout_sec: int) -> List[Dict]:
        """Use Sina API to batch-fetch US pre-market quotes.

        Sina gb_* fields are easy to mix up: parts[1] is the regular-session
        close/last base, while parts[21]/[27] are pre-market price/volume.
        """
        if not symbols:
            return []
        # Convert to Sina format: gb_aapl, gb_msft, ...
        sina_syms = [f"gb_{s.lower().replace('.', '$')}" for s in symbols]
        try:
            url = f"{USPremarketScannerService.SINA_HQ_URL}{','.join(sina_syms)}"
            resp = requests.get(url, headers=USPremarketScannerService.SINA_HEADERS, timeout=timeout_sec)
            text = resp.text or ""
        except Exception as e:
            logger.warning("US premarket: sina batch failed size=%s err=%s", len(symbols), e)
            return []

        results = []
        for line in text.splitlines():
            if '=""' in line or '="' not in line:
                continue
            try:
                parts = line.split('="')[1].split('";')[0].split(',')
                sina_id = line.split('="')[0].split('hq_str_')[1]
                # Extract original symbol from sina_id (gb_aapl -> AAPL)
                raw_sym = sina_id.replace('gb_', '').replace('$', '.').upper()
                if len(parts) < 28:
                    continue

                name = parts[0]
                regular_close = USPremarketScannerService._to_float(parts[1])
                prev_close = USPremarketScannerService._to_float(parts[26] if len(parts) > 26 else 0) or regular_close
                pre_price = USPremarketScannerService._to_float(parts[21])
                pre_volume = USPremarketScannerService._to_float(parts[27])
                sina_pct = USPremarketScannerService._to_float(parts[22])
                pre_change = USPremarketScannerService._to_float(parts[23])
                pre_time = parts[24] if len(parts) > 24 else ""

                if pre_price <= 0 or prev_close <= 0 or pre_volume <= 0:
                    continue

                # 盘前涨幅必须基于“盘前价 vs 昨收/常规收盘价”重新计算，避免误用常规盘涨跌幅字段。
                pct_chg = round((pre_price - prev_close) / prev_close * 100, 2)
                if abs(pct_chg - sina_pct) > 0.2:
                    logger.debug(
                        "US premarket pct recalculated: symbol=%s sina_pct=%.2f calc_pct=%.2f prev=%.4f pre=%.4f",
                        raw_sym, sina_pct, pct_chg, prev_close, pre_price,
                    )
                amount = pre_volume * pre_price

                results.append({
                    "symbol": raw_sym,
                    "name": name,
                    "price": pre_price,
                    "prev_close": prev_close,
                    "regular_close": regular_close,
                    "pct": pct_chg,
                    "pre_change": round(pre_price - prev_close, 4),
                    "sina_pre_change": pre_change,
                    "volume": pre_volume,
                    "amount": amount,
                    "pre_time": pre_time,
                })
            except Exception:
                continue
        return results

    @staticmethod
    def _build_candidate(row: Dict, min_mcap: float, min_amt: float, min_abs_pct: float) -> Dict:
        symbol = row.get("symbol", "")
        name = row.get("name", "")
        price = row.get("price", 0.0)
        pct = row.get("pct", 0.0)
        volume = row.get("volume", 0.0)
        amount = row.get("amount", 0.0)

        if price <= 0 or volume <= 0:
            return {}
        if amount < min_amt:
            return {}
        if abs(pct) < min_abs_pct:
            return {}

        # Fetch market cap from cached Finnhub/Yahoo fallback
        mcap = 0.0
        try:
            from modules.ingestion.us_market_cap import get_us_market_metrics
            mc = get_us_market_metrics(symbol)
            mcap = (mc.get("market_cap_musd", 0.0) or 0.0) * 1_000_000  # Convert M USD -> USD
        except Exception:
            pass

        if mcap < min_mcap:
            return {}

        return {
            "symbol": symbol,
            "name": name,
            "pre_price": price,
            "prev_close": row.get("prev_close", 0.0),
            "pre_vol": volume,
            "pre_notional": amount,
            "pct": pct,
            "market_cap": mcap,
            "pre_time": row.get("pre_time", ""),
        }

    @staticmethod
    def scan_and_notify() -> Dict:
        enabled = str(os.getenv("ENABLE_US_PREMARKET_SCANNER", "1")).strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            logger.info("US premarket scanner disabled")
            return {"enabled": False, "scanned": 0, "selected": 0}

        symbol_limit = int(os.getenv("US_PREMARKET_SYMBOL_LIMIT", "1800") or 1800)
        batch_size = int(os.getenv("US_PREMARKET_BATCH_SIZE", "400") or 400)
        timeout_sec = int(os.getenv("US_PREMARKET_TIMEOUT_SEC", "10") or 10)

        min_amt = float(os.getenv("US_PREMARKET_MIN_NOTIONAL_USD", "5000000") or 5000000)
        min_abs_pct = float(os.getenv("US_PREMARKET_MIN_ABS_PCT", "5") or 5)
        min_mcap = float(os.getenv("US_PREMARKET_MIN_MCAP_USD", "1000000000") or 1000000000)
        top_n = int(os.getenv("US_PREMARKET_TOP_N", "5") or 5)

        symbols = USPremarketScannerService._load_us_symbols(limit=max(1, symbol_limit))
        if not symbols:
            logger.warning("US premarket: symbol universe empty")
            return {"enabled": True, "scanned": 0, "selected": 0}

        # Batch fetch from Sina API
        all_quotes: List[Dict] = []
        for i in range(0, len(symbols), max(1, batch_size)):
            batch = symbols[i:i + batch_size]
            all_quotes.extend(
                USPremarketScannerService._fetch_sina_batch(batch, timeout_sec=timeout_sec)
            )

        # Pre-filter by price/amount/pct BEFORE expensive mcap lookup
        pre_filtered = [
            q for q in all_quotes
            if q.get("amount", 0) >= min_amt and abs(q.get("pct", 0)) >= min_abs_pct
        ]
        logger.info("US premarket: %d quotes fetched, %d passed pre-filter", len(all_quotes), len(pre_filtered))

        # Build candidates (includes mcap lookup only for pre-filtered stocks)
        candidates: List[Dict] = []
        for row in pre_filtered:
            candidate = USPremarketScannerService._build_candidate(
                row, min_mcap=min_mcap, min_amt=min_amt, min_abs_pct=min_abs_pct,
            )
            if candidate:
                candidates.append(candidate)

        candidates.sort(key=lambda x: (x["pre_notional"], abs(x["pct"])), reverse=True)
        top = candidates[: max(1, top_n)]

        if not top:
            logger.info("US premarket: no candidate matched thresholds (pre_filtered=%d)", len(pre_filtered))
            return {
                "enabled": True,
                "scanned": len(all_quotes),
                "selected": 0,
                "symbol_universe": len(symbols),
            }

        sh_now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
        lines = [f"\U0001f6a8 美股盘前猎手 (Top {len(top)})", f"北京时间: {sh_now}", ""]

        for i, item in enumerate(top, 1):
            direction = "\u2b06\ufe0f" if item["pct"] >= 0 else "\u2b07\ufe0f"
            lines.append(
                "{i}. {symbol} {direction} 盘前{pct:+.2f}%  盘前价:{price:.2f}  昨收:{prev:.2f}  成交额:{notional:.1f}M  市值:{mcap:.1f}B".format(
                    i=i,
                    symbol=item.get("symbol", ""),
                    direction=direction,
                    pct=float(item.get("pct", 0) or 0),
                    price=float(item.get("pre_price", 0) or 0),
                    prev=float(item.get("prev_close", 0) or 0),
                    notional=float(item.get("pre_notional", 0) or 0) / 1e6,
                    mcap=float(item.get("market_cap", 0) or 0) / 1e9,
                )
            )
            if item["name"]:
                lines.append(f"   {item['name']}")
            lines.append("")

        Notifier.broadcast("\n".join(lines).strip())
        logger.info(
            "US premarket pushed: selected=%s scanned=%s universe=%s",
            len(top), len(all_quotes), len(symbols),
        )
        return {
            "enabled": True,
            "scanned": len(all_quotes),
            "selected": len(top),
            "symbol_universe": len(symbols),
        }


us_premarket_scanner_service = USPremarketScannerService()
