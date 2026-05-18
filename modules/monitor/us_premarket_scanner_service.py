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
    def _load_us_symbols(limit: int) -> List[str]:
        try:
            conn = sqlite3.connect("data/meta.db")
            cur = conn.cursor()
            cur.execute("SELECT symbol FROM asset WHERE market=?", ("US",))
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            logger.warning("US premarket: load symbols failed: %s", e)
            return []

        out: List[str] = []
        seen = set()
        for row in rows:
            sym = str((row or [""])[0] or "").split(".")[-1].strip().upper()
            if (not sym) or (sym in seen):
                continue
            seen.add(sym)
            out.append(sym)
            if len(out) >= limit:
                break
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
