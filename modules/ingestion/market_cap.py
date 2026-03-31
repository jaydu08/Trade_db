import datetime as dt
import logging
import os
from typing import Dict, Optional

import requests

from core.cache import cached

logger = logging.getLogger(__name__)

_TENCENT_HEADERS = {
    "Referer": "https://qt.gtimg.cn/",
    "User-Agent": "Mozilla/5.0",
}
_TUSHARE_API = "http://api.tushare.pro"

_MONEYFLOW_FORBIDDEN = {
    "tushare": False,
    "joinquant": False,
    "rqdata": False,
}


def _to_float(value) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


def _fmt_cn_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().lower()
    if s.startswith(("sh", "sz", "bj")):
        return s
    if s.startswith(("6", "9")):
        return f"sh{s}"
    if s.startswith(("0", "2", "3", "8", "4")):
        return f"sz{s}"
    return f"sz{s}"


def _fmt_ts_code(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if "." in s:
        return s
    if s.startswith(("6", "9")):
        return f"{s}.SH"
    if s.startswith(("8", "4")):
        return f"{s}.BJ"
    return f"{s}.SZ"


def _fmt_jq_code(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if s.endswith((".XSHE", ".XSHG")):
        return s
    if s.endswith((".SZ", ".BJ")):
        return s.split(".")[0] + ".XSHE"
    if s.endswith(".SH"):
        return s.split(".")[0] + ".XSHG"
    if s.startswith(("6", "9")):
        return f"{s}.XSHG"
    return f"{s}.XSHE"


def _fmt_rq_code(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if s.endswith((".XSHE", ".XSHG")):
        return s
    if s.endswith(".SZ"):
        return s.split(".")[0] + ".XSHE"
    if s.endswith(".SH"):
        return s.split(".")[0] + ".XSHG"
    if s.startswith(("6", "9")):
        return f"{s}.XSHG"
    return f"{s}.XSHE"


def _to_trade_date(value: Optional[dt.date | dt.datetime | str]) -> str:
    if value is None:
        return dt.date.today().strftime("%Y%m%d")
    if isinstance(value, dt.datetime):
        return value.date().strftime("%Y%m%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y%m%d")
    raw = str(value).strip()
    if not raw:
        return dt.date.today().strftime("%Y%m%d")
    digits = raw.replace("-", "")
    if len(digits) >= 8:
        return digits[:8]
    return dt.date.today().strftime("%Y%m%d")


def _to_date_str_ymd(value: str) -> str:
    d = _to_trade_date(value)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


@cached("cn_market_metrics", ttl=300)
def get_cn_market_metrics(symbol: str) -> Dict[str, float]:
    """从腾讯行情接口获取 A 股市值相关字段。"""
    sym = _fmt_cn_symbol(symbol)
    url = f"https://qt.gtimg.cn/q={sym}"

    try:
        resp = requests.get(url, headers=_TENCENT_HEADERS, timeout=8)
        text = (resp.text or "").strip()
        if not text or '=""' in text:
            return {}

        payload = text.split('="', 1)[1].rsplit('";', 1)[0]
        parts = payload.split("~")

        if len(parts) < 46:
            return {}

        turnover_rate = _to_float(parts[38])
        total_mv_100m = _to_float(parts[44])
        circ_mv_100m = _to_float(parts[45])

        return {
            "turnover_rate": turnover_rate,
            "total_mv_100m": total_mv_100m,
            "circ_mv_100m": circ_mv_100m,
        }
    except Exception as e:
        logger.debug("Tencent market metrics failed for %s: %s", symbol, e)
        return {}


def _flow_by_tushare(symbol: str, trade_date: str) -> Dict[str, float]:
    if _MONEYFLOW_FORBIDDEN["tushare"]:
        return {}

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        return {}

    payload = {
        "api_name": "moneyflow",
        "token": token,
        "params": {
            "ts_code": _fmt_ts_code(symbol),
            "trade_date": _to_trade_date(trade_date),
        },
        "fields": (
            "ts_code,trade_date,net_mf_amount,buy_lg_amount,buy_elg_amount,"
            "sell_lg_amount,sell_elg_amount"
        ),
    }

    try:
        resp = requests.post(_TUSHARE_API, json=payload, timeout=8)
        data = resp.json() if resp is not None else {}
        if not isinstance(data, dict):
            return {}

        code = int(data.get("code", -1))
        if code == 40203:
            _MONEYFLOW_FORBIDDEN["tushare"] = True
            logger.info("Tushare moneyflow permission denied; disabled in this process.")
            return {}
        if code != 0:
            return {}

        result = data.get("data") or {}
        fields = result.get("fields") or []
        items = result.get("items") or []
        if not fields or not items:
            return {}

        row = dict(zip(fields, items[0]))
        net_mf_amount_wy = _to_float(row.get("net_mf_amount"))
        if net_mf_amount_wy == 0:
            buy_lg = _to_float(row.get("buy_lg_amount"))
            buy_elg = _to_float(row.get("buy_elg_amount"))
            sell_lg = _to_float(row.get("sell_lg_amount"))
            sell_elg = _to_float(row.get("sell_elg_amount"))
            net_mf_amount_wy = (buy_lg + buy_elg) - (sell_lg + sell_elg)

        return {
            "main_net_inflow_100m": round(net_mf_amount_wy / 10000.0, 4),
            "trade_date": str(row.get("trade_date") or ""),
            "provider": "tushare",
        }
    except Exception as e:
        logger.debug("Tushare fund flow failed for %s: %s", symbol, e)
        return {}


def _flow_by_joinquant(symbol: str, trade_date: str) -> Dict[str, float]:
    if _MONEYFLOW_FORBIDDEN["joinquant"]:
        return {}

    user = os.getenv("JOINQUANT_USERNAME", "").strip() or os.getenv("JQDATA_USERNAME", "").strip()
    pwd = os.getenv("JOINQUANT_PASSWORD", "").strip() or os.getenv("JQDATA_PASSWORD", "").strip()
    if not user or not pwd:
        return {}

    try:
        import jqdatasdk as jq
    except Exception:
        return {}

    try:
        ok = bool(jq.auth(user, pwd))
        if not ok:
            _MONEYFLOW_FORBIDDEN["joinquant"] = True
            return {}

        code = _fmt_jq_code(symbol)
        date_ymd = _to_date_str_ymd(trade_date)
        df = jq.get_money_flow(
            security_list=[code],
            start_date=date_ymd,
            end_date=date_ymd,
            fields=["date", "sec_code", "net_amount_main"],
        )
        if df is None or getattr(df, "empty", True):
            return {}

        row = df.iloc[0]
        # 聚宽该字段通常为万元
        net_main_wy = _to_float(row.get("net_amount_main", 0))
        if net_main_wy == 0:
            return {}

        return {
            "main_net_inflow_100m": round(net_main_wy / 10000.0, 4),
            "trade_date": str(row.get("date") or date_ymd),
            "provider": "joinquant",
        }
    except Exception as e:
        msg = str(e)
        if "权限" in msg or "permission" in msg.lower() or "auth" in msg.lower():
            _MONEYFLOW_FORBIDDEN["joinquant"] = True
            logger.info("JoinQuant moneyflow permission/auth failed; disabled in this process.")
        logger.debug("JoinQuant fund flow failed for %s: %s", symbol, e)
        return {}


def _flow_by_rqdata(symbol: str, trade_date: str) -> Dict[str, float]:
    if _MONEYFLOW_FORBIDDEN["rqdata"]:
        return {}

    user = os.getenv("RQDATA_USERNAME", "").strip() or os.getenv("RICEQUANT_USERNAME", "").strip()
    pwd = os.getenv("RQDATA_PASSWORD", "").strip() or os.getenv("RICEQUANT_PASSWORD", "").strip()
    if not user or not pwd:
        return {}

    try:
        import rqdatac
    except Exception:
        return {}

    try:
        rqdatac.init(user, pwd)
        code = _fmt_rq_code(symbol)
        d = _to_date_str_ymd(trade_date)

        # 不同版本字段可能不同，做兼容探测
        raw = None
        if hasattr(rqdatac, "get_money_flow"):
            raw = rqdatac.get_money_flow(order_book_ids=[code], start_date=d, end_date=d)
        elif hasattr(rqdatac, "get_capital_flow"):
            raw = rqdatac.get_capital_flow(order_book_ids=[code], start_date=d, end_date=d)

        if raw is None:
            return {}

        # DataFrame / MultiIndex DataFrame 兼容
        import pandas as pd

        if isinstance(raw, pd.DataFrame):
            if raw.empty:
                return {}
            row = raw.iloc[0]
            for col in [
                "main_net_inflow", "net_main_inflow", "net_amount_main", "main_net_amount",
            ]:
                if col in raw.columns:
                    v = _to_float(row.get(col, 0))
                    if v == 0:
                        continue
                    # rqdata 常见口径为元，转亿元
                    return {
                        "main_net_inflow_100m": round(v / 100000000.0, 4),
                        "trade_date": d,
                        "provider": "rqdata",
                    }

        return {}
    except Exception as e:
        msg = str(e)
        if "权限" in msg or "permission" in msg.lower() or "auth" in msg.lower():
            _MONEYFLOW_FORBIDDEN["rqdata"] = True
            logger.info("RQData moneyflow permission/auth failed; disabled in this process.")
        logger.debug("RQData fund flow failed for %s: %s", symbol, e)
        return {}


@cached("cn_fund_flow", ttl=600)
def get_cn_fund_flow(symbol: str, trade_date: Optional[str] = None) -> Dict[str, float]:
    """A 股主力净流入（亿元），多源回退：Tushare -> JoinQuant -> RQData。"""
    td = _to_trade_date(trade_date)
    providers = [p.strip().lower() for p in os.getenv("FLOW_PROVIDERS", "tushare,joinquant,rqdata").split(",") if p.strip()]

    for p in providers:
        if p == "tushare":
            res = _flow_by_tushare(symbol, td)
        elif p in {"joinquant", "jq", "jqdatasdk"}:
            res = _flow_by_joinquant(symbol, td)
        elif p in {"rqdata", "ricequant", "rq"}:
            res = _flow_by_rqdata(symbol, td)
        else:
            continue

        if res and float(res.get("main_net_inflow_100m", 0) or 0) != 0:
            return res

    return {}


def format_mv_cn(total_mv_100m: float, circ_mv_100m: float) -> Optional[str]:
    if total_mv_100m > 0 and circ_mv_100m > 0:
        return f"总/流值:{total_mv_100m:.0f}/{circ_mv_100m:.0f}亿"
    if total_mv_100m > 0:
        return f"总市值:{total_mv_100m:.0f}亿"
    if circ_mv_100m > 0:
        return f"流通市值:{circ_mv_100m:.0f}亿"
    return None


def format_flow_cn(main_net_inflow_100m: float) -> Optional[str]:
    if main_net_inflow_100m == 0:
        return None
    return f"主力净流入:{main_net_inflow_100m:+.2f}亿"
