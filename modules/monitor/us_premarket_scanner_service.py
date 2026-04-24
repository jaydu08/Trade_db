import datetime as dt
import logging
import os
import sqlite3
from typing import Dict, List

import requests

from modules.monitor.notifier import Notifier

logger = logging.getLogger(__name__)


class USPremarketScannerService:
    """美股盘前猎手：扫描高成交额+大波动+大市值标的并推送。"""

    QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

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
    def _fetch_quote_batch(symbols: List[str], timeout_sec: int) -> List[Dict]:
        if not symbols:
            return []
        try:
            resp = requests.get(
                USPremarketScannerService.QUOTE_URL,
                params={"symbols": ",".join(symbols)},
                timeout=timeout_sec,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            payload = resp.json() if resp is not None else {}
            rows = ((payload or {}).get("quoteResponse") or {}).get("result") or []
            return rows if isinstance(rows, list) else []
        except Exception as e:
            logger.warning("US premarket: quote batch failed size=%s err=%s", len(symbols), e)
            return []

    @staticmethod
    def _build_candidate(row: Dict, min_mcap: float, min_amt: float, min_abs_pct: float) -> Dict:
        symbol = str(row.get("symbol", "") or "").strip().upper()
        name = str(row.get("shortName", "") or row.get("longName", "") or "").strip()

        pre_price = USPremarketScannerService._to_float(row.get("preMarketPrice"), 0.0)
        pre_vol = USPremarketScannerService._to_float(row.get("preMarketVolume"), 0.0)
        prev_close = USPremarketScannerService._to_float(row.get("regularMarketPreviousClose"), 0.0)
        pct = USPremarketScannerService._to_float(row.get("preMarketChangePercent"), 0.0)

        if pct == 0.0 and pre_price > 0 and prev_close > 0:
            pct = (pre_price / prev_close - 1.0) * 100.0

        mcap = USPremarketScannerService._to_float(row.get("marketCap"), 0.0)
        pre_notional = pre_price * pre_vol

        if pre_price <= 0 or pre_vol <= 0:
            return {}
        if mcap < min_mcap:
            return {}
        if pre_notional < min_amt:
            return {}
        if abs(pct) < min_abs_pct:
            return {}

        return {
            "symbol": symbol,
            "name": name,
            "pre_price": pre_price,
            "pre_vol": pre_vol,
            "pre_notional": pre_notional,
            "pct": pct,
            "market_cap": mcap,
        }

    @staticmethod
    def scan_and_notify() -> Dict:
        enabled = str(os.getenv("ENABLE_US_PREMARKET_SCANNER", "1")).strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            logger.info("US premarket scanner disabled")
            return {"enabled": False, "scanned": 0, "selected": 0}

        symbol_limit = int(os.getenv("US_PREMARKET_SYMBOL_LIMIT", "1800") or 1800)
        batch_size = int(os.getenv("US_PREMARKET_BATCH_SIZE", "120") or 120)
        timeout_sec = int(os.getenv("US_PREMARKET_TIMEOUT_SEC", "8") or 8)

        min_amt = float(os.getenv("US_PREMARKET_MIN_NOTIONAL_USD", "5000000") or 5000000)
        min_abs_pct = float(os.getenv("US_PREMARKET_MIN_ABS_PCT", "5") or 5)
        min_mcap = float(os.getenv("US_PREMARKET_MIN_MCAP_USD", "1000000000") or 1000000000)
        top_n = int(os.getenv("US_PREMARKET_TOP_N", "5") or 5)

        symbols = USPremarketScannerService._load_us_symbols(limit=max(1, symbol_limit))
        if not symbols:
            logger.warning("US premarket: symbol universe empty")
            return {"enabled": True, "scanned": 0, "selected": 0}

        rows: List[Dict] = []
        for i in range(0, len(symbols), max(1, batch_size)):
            rows.extend(
                USPremarketScannerService._fetch_quote_batch(
                    symbols[i:i + batch_size],
                    timeout_sec=timeout_sec,
                )
            )

        candidates: List[Dict] = []
        for row in rows:
            candidate = USPremarketScannerService._build_candidate(
                row,
                min_mcap=min_mcap,
                min_amt=min_amt,
                min_abs_pct=min_abs_pct,
            )
            if candidate:
                candidates.append(candidate)

        candidates.sort(key=lambda x: (x["pre_notional"], abs(x["pct"])), reverse=True)
        top = candidates[: max(1, top_n)]

        if not top:
            logger.info("US premarket: no candidate matched thresholds")
            return {
                "enabled": True,
                "scanned": len(rows),
                "selected": 0,
                "symbol_universe": len(symbols),
            }

        sh_now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
        lines = [f"🚨 美股盘前猎手 (Top {len(top)})", f"北京时间: {sh_now}", ""]

        for i, item in enumerate(top, 1):
            direction = "UP" if item["pct"] >= 0 else "DOWN"
            lines.append(
                "{i}. {symbol} {direction} {pct:+.2f}%  盘前价:{price:.2f}  盘前额:{notional:.2f}M USD  市值:{mcap:.2f}B USD".format(
                    i=i,
                    symbol=item.get("symbol", ""),
                    direction=direction,
                    pct=float(item.get("pct", 0) or 0),
                    price=float(item.get("pre_price", 0) or 0),
                    notional=float(item.get("pre_notional", 0) or 0) / 1e6,
                    mcap=float(item.get("market_cap", 0) or 0) / 1e9,
                )
            )
            if item["name"]:
                lines.append(item["name"])
            lines.append("")

        Notifier.broadcast("\n".join(lines).strip())
        logger.info(
            "US premarket pushed: selected=%s scanned_rows=%s universe=%s",
            len(top), len(rows), len(symbols),
        )
        return {
            "enabled": True,
            "scanned": len(rows),
            "selected": len(top),
            "symbol_universe": len(symbols),
        }


us_premarket_scanner_service = USPremarketScannerService()
