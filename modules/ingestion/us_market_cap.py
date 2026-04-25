import logging
import os
from typing import Dict

import requests

from core.cache import cached

logger = logging.getLogger(__name__)


def _to_float(value) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


@cached("us_market_metrics", ttl=1800)
def get_us_market_metrics(symbol: str) -> Dict[str, float]:
    """美股市值（优先 Finnhub，失败回退 Yahoo Finance，本地缓存兜底）。"""
    raw = str(symbol or "").split(".")[-1].strip().upper()
    if not raw:
        return {}

    key = os.getenv("FINNHUB_API_KEY", "").strip()
    if key:
        try:
            resp = requests.get(
                "https://finnhub.io/api/v1/stock/profile2",
                params={"symbol": raw, "token": key},
                timeout=6,
            )
            if getattr(resp, "status_code", 0) == 200:
                payload = resp.json() if resp is not None else {}
                cap_m = _to_float((payload or {}).get("marketCapitalization"))
                if cap_m > 0:
                    return {
                        "provider": "finnhub",
                        "symbol": raw,
                        "market_cap_musd": round(cap_m, 4),
                        "market_cap_100m_usd": round(cap_m / 100.0, 4),
                        "finnhub_industry": str((payload or {}).get("finnhubIndustry", "") or ""),
                    }
            elif getattr(resp, "status_code", 0) in (401, 403, 429):
                logger.info("US market cap finnhub unavailable: symbol=%s status=%s", raw, resp.status_code)
        except Exception as e:
            logger.debug("US market cap finnhub failed for %s: %s", raw, e)

    try:
        from modules.ingestion.yfinance_client import yfinance_client

        data = yfinance_client.get_financials(raw)
        cap = _to_float((data or {}).get("market_cap"))
        if cap > 0:
            cap_musd = cap / 1000000.0
            return {
                "provider": "yfinance",
                "symbol": raw,
                "market_cap_musd": round(cap_musd, 4),
                "market_cap_100m_usd": round(cap_musd / 100.0, 4),
            }
    except Exception as e:
        logger.debug("US market cap yfinance fallback failed for %s: %s", raw, e)

    return {}
