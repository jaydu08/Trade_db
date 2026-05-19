"""
FastAPI Web Interface for Trade_db
Port 9800, JWT auth, serves Vue3 SPA static files.
"""
import os
import datetime as dt
import logging
import time
import threading
from typing import Optional, List, Dict

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from jose import jwt, JWTError

from core.cache import get_cache, set_cache

logger = logging.getLogger(__name__)

# --------------- Config ---------------
JWT_SECRET = os.getenv("WEB_JWT_SECRET", "trade_db_secret_key_2026")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 365
WEB_USERNAME = os.getenv("WEB_USERNAME", "game2du")
WEB_PORT = int(os.getenv("WEB_PORT", "9800"))
WEB_NEWS_CACHE_TTL_HOURS = int(os.getenv("WEB_NEWS_CACHE_TTL_HOURS", "8"))  # 6-12h recommended
WEB_NEWS_CACHE_TTL_HOURS = max(6, min(12, WEB_NEWS_CACHE_TTL_HOURS))
WEB_NEWS_CACHE_TTL_SEC = WEB_NEWS_CACHE_TTL_HOURS * 3600
WEB_NEWS_AUTO_REFRESH = os.getenv("WEB_NEWS_AUTO_REFRESH", "1") in ("1", "true", "True")
WEB_NEWS_SUMMARY_LLM = os.getenv("WEB_NEWS_SUMMARY_LLM", "1") in ("1", "true", "True")

# News cache: {symbol: {"summary": str, "ts": float}}
_news_cache: Dict[str, Dict] = {}
_news_refreshing = set()
_news_lock = threading.Lock()

# --------------- App ---------------
app = FastAPI(title="Trade_db Web", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------- Models ---------------
class LoginRequest(BaseModel):
    username: str

class LoginResponse(BaseModel):
    token: str
    username: str

class WatchlistAddRequest(BaseModel):
    symbol: str
    market: str
    name: str = ""

class TagUpdateRequest(BaseModel):
    tags: str

class BuyRequest(BaseModel):
    symbol: str
    market: str = ""
    target_days: Optional[int] = None
    reason: str = ""

class SellRequest(BaseModel):
    symbol: str
    market: str = ""

# --------------- Auth ---------------
def create_token(username: str) -> str:
    expire = dt.datetime.utcnow() + dt.timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def get_current_user(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = auth[7:]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expired or invalid")

# --------------- Routes: Auth ---------------
@app.post("/api/login", response_model=LoginResponse)
def login(req: LoginRequest):
    if req.username != WEB_USERNAME:
        raise HTTPException(status_code=403, detail="Invalid username")
    token = create_token(req.username)
    return LoginResponse(token=token, username=req.username)

# --------------- Routes: Watchlist ---------------
def _enrich_watchlist_realtime(items_raw: dict) -> list:
    """Enrich watchlist items with real-time quotes, market cap, drawdown."""
    import asyncio
    import concurrent.futures
    from modules.probing.async_prober import async_prober
    from modules.ingestion.market_cap import get_cn_market_metrics
    from modules.ingestion.us_market_cap import get_us_market_metrics, get_us_market_metrics_light
    from modules.ingestion.market_cap import get_hk_market_metrics

    # Prepare items for async quote fetch
    probe_items = []
    for key, item in items_raw.items():
        probe_items.append({"symbol": item.get("symbol", ""), "market": item.get("market", "CN")})

    # Batch fetch real-time quotes - use thread to avoid event loop conflicts
    quotes = {}
    def _fetch_quotes():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(async_prober.get_quotes_async(probe_items))
        finally:
            loop.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_fetch_quotes)
            quotes = future.result(timeout=30)
    except Exception as e:
        logger.warning(f"Watchlist quote fetch failed: {e}")

    # Build result with enrichment
    _backfill_entry_prices = {}
    result = []
    for key, item in items_raw.items():
        symbol = item.get("symbol", "")
        market = item.get("market", "CN")
        quote_key = f"{market}:{symbol}"
        q = quotes.get(quote_key, {})

        # Market cap
        mcap_total = 0.0
        mcap_float = 0.0
        try:
            if market == "CN":
                mc = get_cn_market_metrics(symbol)
                mcap_total = mc.get("total_mv_100m", 0.0)
                mcap_float = mc.get("circ_mv_100m", 0.0)
            elif market == "US":
                mc = get_us_market_metrics(symbol)
                mcap_total = mc.get("market_cap_100m_usd", 0.0)
                mcap_float = mcap_total  # US stocks: float ≈ total
            elif market == "HK":
                mc = get_hk_market_metrics(symbol)
                mcap_total = mc.get("market_cap_100m_hkd", 0.0) or mc.get("market_cap_100m_usd", 0.0)
                mcap_float = mc.get("circ_mv_100m_hkd", 0.0) or mcap_total
        except Exception:
            pass

        # Total change since added
        entry_price = item.get("entry_price", 0)
        current_price = q.get("price", 0)
        # If entry_price was never recorded, try to backfill from historical close on added_at date
        if (not entry_price or entry_price <= 0) and current_price and current_price > 0:
            entry_price = _get_historical_entry_price(symbol, market, item.get("added_at", "")) or current_price
            item["entry_price"] = entry_price
            _backfill_entry_prices[key] = entry_price

        total_change = 0.0
        if entry_price and current_price and entry_price > 0:
            total_change = round((current_price - entry_price) / entry_price * 100, 2)

        # Max drawdown: (entry_price - lowest) / entry_price, includes intraday low
        # Only consider data AFTER added_at date
        intraday_low = q.get("day_low", 0.0)
        added_at_str = item.get("added_at", "")
        max_drawdown = _calc_max_drawdown(symbol, market, entry_price, intraday_low, added_at_str)

        result.append({
            "key": key,
            "symbol": symbol,
            "name": item.get("name", "") or q.get("name", ""),
            "market": market,
            "added_at": item.get("added_at", ""),
            "tags": item.get("tags", ""),
            "rating": item.get("rating", 0),
            "price": current_price,
            "day_change": q.get("pct_chg", 0.0),
            "amount": q.get("amount", 0.0),
            "market_cap": mcap_total,
            "float_cap": mcap_float,
            "entry_price": entry_price,
            "total_change": total_change,
            "max_drawdown": max_drawdown,
            "news_summary": _get_news_summary(symbol, item.get("name", ""), market),
        })
    # Persist backfilled entry_prices to watchlist.json
    if _backfill_entry_prices:
        try:
            from modules.monitor.repository import WatchlistRepository
            repo = WatchlistRepository()
            all_items = repo.load_all()
            for k, ep in _backfill_entry_prices.items():
                if k in all_items:
                    all_items[k]["entry_price"] = ep
            repo.save_all(all_items)
        except Exception:
            pass

    return result


def _get_historical_entry_price(symbol: str, market: str, added_at_str: str) -> float:
    """Get the close price on or near the added_at date.
    Tries TrendDailyBar first, then Sina US daily K-line for US stocks."""
    import datetime as _dt

    added_date = None
    if added_at_str:
        try:
            added_date = _dt.datetime.strptime(added_at_str[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return 0.0
    if not added_date:
        return 0.0

    # Try TrendDailyBar first
    try:
        from core.db import db_manager
        from domain.ledger.analytics import TrendDailyBar
        from sqlmodel import Session, select

        with Session(db_manager.ledger_engine) as session:
            stmt = (
                select(TrendDailyBar.close)
                .where(TrendDailyBar.symbol == symbol)
                .where(TrendDailyBar.market == market)
                .where(TrendDailyBar.date >= added_date)
                .order_by(TrendDailyBar.date)
                .limit(1)
            )
            result = session.exec(stmt).first()
            if result and result > 0:
                return float(result)
    except Exception:
        pass

    # Fallback: Sina US/HK daily K-line
    if market == "US":
        return _get_sina_us_close_on_date(symbol, added_date)
    return 0.0


def _get_sina_us_close_on_date(symbol: str, target_date) -> float:
    """Get US stock close price near target_date from Sina daily K-line."""
    import datetime as _dt
    try:
        import urllib.request, json
        url = f'http://stock.finance.sina.com.cn/usstock/api/json_v2.php/US_MinKService.getDailyK?symbol={symbol.lower()}&type=daily&range=250'
        req = urllib.request.Request(url, headers={'Referer': 'http://finance.sina.com.cn'})
        text = urllib.request.urlopen(req, timeout=8).read().decode('utf-8')
        data = json.loads(text)
        best = None
        for d in data:
            dd = _dt.datetime.strptime(d['d'], '%Y-%m-%d').date()
            if dd <= target_date:
                best = d
        if best:
            return float(best['c'])
        # Use first date after target
        for d in data:
            dd = _dt.datetime.strptime(d['d'], '%Y-%m-%d').date()
            if dd >= target_date:
                return float(d['c'])
    except Exception:
        pass
    return 0.0

def _get_news_summary(symbol: str, name: str, market: str) -> str:
    """Get cached news summary for a stock.
    双层缓存：内存 + 磁盘；刷新失败不覆盖旧值，避免前端突然空白。"""
    cache_key = f"{market}:{symbol}"
    disk_key = f"web_news_summary:{cache_key}"
    now = time.time()

    # 1) in-memory hot cache
    cached = _news_cache.get(cache_key)
    if cached and (now - float(cached.get("ts", 0))) < WEB_NEWS_CACHE_TTL_SEC:
        return str(cached.get("summary", ""))

    # 2) disk persistent cache
    disk_cached = get_cache(disk_key)
    if isinstance(disk_cached, dict):
        dsum = str(disk_cached.get("summary", "") or "")
        dts = float(disk_cached.get("ts", 0) or 0)
        if dsum:
            _news_cache[cache_key] = {"summary": dsum, "ts": dts or now}
            # 命中持久缓存时直接返回，保证首屏稳定
            if (now - dts) < WEB_NEWS_CACHE_TTL_SEC:
                return dsum

    # 3) refresh control
    fallback_summary = str((_news_cache.get(cache_key) or {}).get("summary", "") or "")
    if not WEB_NEWS_AUTO_REFRESH:
        return fallback_summary or "加载中"

    def _refresh():
        try:
            from modules.ingestion.data_factory import data_manager
            query = name or symbol
            raw = data_manager.search(query=f"{query} 最新", limit_per_source=2, timeout=8)

            summary = ""
            if raw and len(raw) > 10:
                if WEB_NEWS_SUMMARY_LLM:
                    summary = _llm_summarize_news(raw, name or symbol)
                else:
                    # 无 LLM 时，用搜索结果首条做轻量摘要
                    lines = [ln.strip() for ln in str(raw).split("\n") if ln.strip()]
                    for ln in lines:
                        if ln.startswith("【"):
                            continue
                        summary = ln[:30]
                        break

            summary = str(summary or "").strip()
            prev = _news_cache.get(cache_key) or {}
            prev_summary = str(prev.get("summary", "") or "")
            final_summary = summary or prev_summary

            if final_summary:
                payload = {"summary": final_summary, "ts": time.time()}
                _news_cache[cache_key] = payload
                set_cache(disk_key, payload, ttl=WEB_NEWS_CACHE_TTL_SEC)
        except Exception as e:
            logger.debug(f"News refresh failed for {cache_key}: {e}")
        finally:
            with _news_lock:
                _news_refreshing.discard(cache_key)

    with _news_lock:
        if cache_key in _news_refreshing:
            return fallback_summary or "加载中"
        _news_refreshing.add(cache_key)

    threading.Thread(target=_refresh, daemon=True).start()
    return fallback_summary or "加载中"


def _llm_summarize_news(news_text: str, stock_name: str) -> str:
    """Use LLM to summarize news to ~20 chars."""
    try:
        from core.llm import simple_prompt
        prompt = f"以下是关于{stock_name}的最新新闻，请用不超过20个中文字总结最关键的一条动态，只输出总结内容：\n{news_text[:500]}"
        result = simple_prompt(prompt)
        return result.strip()[:30] if result else ""
    except Exception:
        # Fallback: extract first sentence
        lines = [l.strip() for l in news_text.split('\n') if l.strip()]
        return lines[0][:25] if lines else ""

def _calc_max_drawdown(symbol: str, market: str, entry_price: float = 0, intraday_low: float = 0, added_at_str: str = "") -> float:
    """Calculate max drawdown from entry_price to lowest price SINCE added_at.
    Considers both historical daily closes AND current intraday low.
    Only counts when price drops below entry_price."""
    if not entry_price or entry_price <= 0:
        return 0.0
    try:
        import datetime as _dt
        from core.db import db_manager
        from domain.ledger.analytics import TrendDailyBar
        from sqlmodel import Session, select, or_

        # Parse added_at to filter only post-add data
        added_date = None
        if added_at_str:
            try:
                added_date = _dt.datetime.strptime(added_at_str[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        # If added TODAY, no meaningful drawdown can be computed yet
        today = _dt.date.today()
        if added_date and added_date >= today:
            return 0.0

        # US stocks are stored as "105.NVDA" or "106.DELL" in TrendDailyBar
        sym_candidates = [symbol]
        if market == "US":
            sym_candidates += [f"105.{symbol}", f"106.{symbol}"]

        with Session(db_manager.ledger_engine) as session:
            stmt = (
                select(TrendDailyBar.close)
                .where(TrendDailyBar.symbol.in_(sym_candidates))
                .where(TrendDailyBar.market == market)
            )
            if added_date:
                # Use strict > (not >=): the entry day's close doesn't represent post-add performance
                # If added on a non-trading day, this also avoids including stale data
                stmt = stmt.where(TrendDailyBar.date > added_date)
            stmt = stmt.order_by(TrendDailyBar.date)
            closes = [r for r in session.exec(stmt).all() if r and r > 0]

        # Fallback: fetch from Sina daily K-line if no TrendDailyBar data
        if not closes and market == "US" and added_date:
            closes = _get_sina_us_closes_since(symbol, added_date)

        # Include intraday low in comparison
        # But ONLY if market is likely open right now (otherwise stale from last session)
        # And ONLY if there are closes data (meaning at least one trading day has passed)
        candidates = closes[:]
        if closes and intraday_low and intraday_low > 0:
            try:
                from zoneinfo import ZoneInfo
                tz_map = {"US": "America/New_York", "HK": "Asia/Hong_Kong", "CN": "Asia/Shanghai"}
                tz = ZoneInfo(tz_map.get(market, "Asia/Shanghai"))
                now_local = _dt.datetime.now(tz)
                is_weekday = 0 <= now_local.weekday() <= 4
                hour = now_local.hour
                # Generous market hours: Mon-Fri within extended trading window
                is_market_hours = hour >= 8 and hour < 21
                if is_weekday and is_market_hours:
                    candidates.append(intraday_low)
            except Exception:
                candidates.append(intraday_low)  # fallback: include if we can't determine

        if not candidates:
            return 0.0

        lowest = min(candidates)
        if lowest >= entry_price:
            return 0.0  # never dropped below cost

        return round((entry_price - lowest) / entry_price * 100, 2)
    except Exception:
        return 0.0


def _get_sina_us_closes_since(symbol: str, since_date) -> list:
    """Get US stock daily close prices since a date from Sina K-line API."""
    import datetime as _dt
    try:
        import urllib.request, json
        url = f'http://stock.finance.sina.com.cn/usstock/api/json_v2.php/US_MinKService.getDailyK?symbol={symbol.lower()}&type=daily&range=250'
        req = urllib.request.Request(url, headers={'Referer': 'http://finance.sina.com.cn'})
        text = urllib.request.urlopen(req, timeout=8).read().decode('utf-8')
        data = json.loads(text)
        closes = []
        for d in data:
            dd = _dt.datetime.strptime(d['d'], '%Y-%m-%d').date()
            if dd > since_date:
                c = float(d.get('c', 0))
                if c > 0:
                    closes.append(c)
        return closes
    except Exception:
        return []


@app.get("/api/watchlist")
def get_watchlist(user: str = Depends(get_current_user)):
    from modules.monitor.repository import WatchlistRepository
    repo = WatchlistRepository()
    items_raw = repo.load_all()
    result = _enrich_watchlist_realtime(items_raw)
    return {"items": result}

@app.post("/api/watchlist")
def add_watchlist(req: WatchlistAddRequest, user: str = Depends(get_current_user)):
    from modules.monitor.repository import WatchlistRepository
    import asyncio
    from modules.probing.async_prober import async_prober

    repo = WatchlistRepository()
    key = f"{req.market}:{req.symbol}"
    existing = repo.load_all()
    if key in existing:
        raise HTTPException(status_code=409, detail="Already exists")

    # Get current price for entry_price tracking
    entry_price = 0.0
    try:
        import concurrent.futures
        def _fetch_entry_price():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    async_prober.get_quotes_async([{"symbol": req.symbol, "market": req.market}])
                )
            finally:
                loop.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_fetch_entry_price)
            quotes = future.result(timeout=15)
        q = quotes.get(f"{req.market}:{req.symbol}", {})
        entry_price = q.get("price", 0.0)
        fetched_name = q.get("name", "")
    except Exception:
        fetched_name = ""
        pass

    item = {
        "symbol": req.symbol,
        "market": req.market,
        "name": req.name or fetched_name or req.symbol,
        "added_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "last_alert_at": None,
        "alert_threshold_pct": 5,
        "is_active": True,
        "tags": "",
        "entry_price": entry_price,
    }
    existing[key] = item
    repo.save_all(existing)
    return {"ok": True, "key": key, "entry_price": entry_price}

@app.get("/api/search-stock")
def search_stock(q: str = Query(default=""), market: str = Query(default=""), user: str = Depends(get_current_user)):
    """Fuzzy search stocks by name/code for add dialog."""
    from core.db import db_manager
    from domain.meta.asset import Asset
    from sqlmodel import Session, select, col

    results = []
    if not q or len(q) < 1:
        return {"results": results}

    try:
        with Session(db_manager.meta_engine) as session:
            stmt = select(Asset.symbol, Asset.name, Asset.market)
            if market:
                stmt = stmt.where(Asset.market == market)
            # Search by symbol prefix or name contains
            stmt = stmt.where(
                (col(Asset.symbol).startswith(q.upper())) |
                (col(Asset.name).contains(q))
            ).limit(20)
            rows = session.exec(stmt).all()
            for r in rows:
                results.append({"symbol": r[0], "name": r[1] or "", "market": r[2] or ""})
    except Exception as e:
        logger.warning(f"Search stock failed: {e}")

    return {"results": results}

@app.delete("/api/watchlist/{key}")
def del_watchlist(key: str, user: str = Depends(get_current_user)):
    from modules.monitor.repository import WatchlistRepository
    repo = WatchlistRepository()
    existing = repo.load_all()
    # key format: "US:AAPL" but URL encodes : so accept both
    key = key.replace("%3A", ":").replace("%3a", ":")
    if key not in existing:
        raise HTTPException(status_code=404, detail="Not found")
    del existing[key]
    repo.save_all(existing)
    return {"ok": True}

@app.patch("/api/watchlist/{key}/tags")
def update_tags(key: str, req: TagUpdateRequest, user: str = Depends(get_current_user)):
    from modules.monitor.repository import WatchlistRepository
    repo = WatchlistRepository()
    existing = repo.load_all()
    key = key.replace("%3A", ":").replace("%3a", ":")
    if key not in existing:
        raise HTTPException(status_code=404, detail="Not found")
    existing[key]["tags"] = req.tags
    repo.save_all(existing)
    return {"ok": True}

class RatingUpdateRequest(BaseModel):
    rating: int = 0  # 0-5 stars

@app.patch("/api/watchlist/{key}/rating")
def update_rating(key: str, req: RatingUpdateRequest, user: str = Depends(get_current_user)):
    from modules.monitor.repository import WatchlistRepository
    repo = WatchlistRepository()
    existing = repo.load_all()
    key = key.replace("%3A", ":").replace("%3a", ":")
    if key not in existing:
        raise HTTPException(status_code=404, detail="Not found")
    existing[key]["rating"] = max(0, min(5, req.rating))
    repo.save_all(existing)
    return {"ok": True}

# --------------- Routes: Holds ---------------
@app.get("/api/holds")
def get_holds(user: str = Depends(get_current_user)):
    from modules.paper_trading.service import PaperTradingService
    svc = PaperTradingService()
    chat_id = int(os.getenv("ALLOWED_USER_IDS", "0").split(",")[0])
    trades = svc.get_active_trades(chat_id)
    result = []
    for t in trades:
        days_held = (dt.date.today() - t.entry_date).days if t.entry_date else 0
        result.append({
            "id": t.id,
            "symbol": t.symbol,
            "name": t.name,
            "market": t.market,
            "entry_price": t.entry_price,
            "entry_date": str(t.entry_date) if t.entry_date else "",
            "target_days": t.target_days,
            "entry_reason": t.entry_reason or "",
            "days_held": days_held,
            "pnl_pct": t.pnl_pct,
            "review_text": t.review_text or "",
        })
    return {"items": result}

@app.get("/api/trades/history")
def get_trade_history(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    market: str = Query(default=""),
    symbol: str = Query(default=""),
    review_status: str = Query(default=""),
    sort: str = Query(default="exit_date_desc"),
    user: str = Depends(get_current_user),
):
    allowed_sort = {
        "exit_date_desc", "exit_date_asc",
        "entry_date_desc", "entry_date_asc",
        "pnl_desc", "pnl_asc",
        "hold_days_desc", "hold_days_asc",
    }
    if sort not in allowed_sort:
        raise HTTPException(status_code=400, detail=f"sort must be one of {sorted(allowed_sort)}")

    from modules.paper_trading.service import PaperTradingService
    svc = PaperTradingService()
    scope = os.getenv("WEB_TRADE_HISTORY_SCOPE", "all").strip().lower()
    chat_id = None if scope in {"all", "admin"} else int(os.getenv("ALLOWED_USER_IDS", "0").split(",")[0])
    return svc.get_trade_history(
        chat_id=chat_id,
        page=page,
        page_size=page_size,
        market=market,
        symbol=symbol,
        review_status=review_status,
        sort=sort,
    )

@app.post("/api/trade/buy")
def trade_buy(req: BuyRequest, user: str = Depends(get_current_user)):
    from modules.paper_trading.service import PaperTradingService
    svc = PaperTradingService()
    chat_id = int(os.getenv("ALLOWED_USER_IDS", "0").split(",")[0])
    ok, msg, trade = svc.open_position(
        query=req.symbol, chat_id=chat_id,
        target_days=req.target_days, reason=req.reason
    )
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"ok": True, "message": msg}

@app.post("/api/trade/sell")
def trade_sell(req: SellRequest, user: str = Depends(get_current_user)):
    from modules.paper_trading.service import PaperTradingService
    svc = PaperTradingService()
    chat_id = int(os.getenv("ALLOWED_USER_IDS", "0").split(",")[0])
    ok, msg, trade = svc.close_position(query=req.symbol, chat_id=chat_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"ok": True, "message": msg}

# --------------- Routes: Trend ---------------
def _normalize_trend_market(market: str, allowed_markets) -> str:
    market_filter = str(market or "").strip().upper()
    if not market_filter:
        return ""
    if market_filter not in allowed_markets:
        raise HTTPException(status_code=400, detail=f"market must be one of {sorted(allowed_markets)}")
    return market_filter

@app.get("/api/trend")
def get_trend(
    days: int = Query(default=7, ge=3, le=180),
    limit: int = Query(default=100, ge=10, le=200),
    market: str = Query(default=""),
    user: str = Depends(get_current_user),
):
    allowed_days = {3, 7, 14, 30, 60, 90, 180}
    if days not in allowed_days:
        raise HTTPException(status_code=400, detail=f"days must be one of {sorted(allowed_days)}")
    market_filter = _normalize_trend_market(market, {"CN", "HK", "US", "CF"})

    from modules.monitor.trend_service import TrendCalculator
    from modules.ingestion.institutional_factor import get_institutional_change_map
    from core.db import get_ledger_session
    from domain.ledger.analytics import TrendSeedPool, TrendDailyBar
    from sqlmodel import select

    calc = TrendCalculator()
    raw = calc.calculate_trend(days=days, topn_override=limit)
    if market_filter:
        raw = {market_filter: raw.get(market_filter, [])}

    cutoff = dt.date.today() - dt.timedelta(days=max(30, days))
    symbols_by_market = {}
    for mkt, items in raw.items():
        syms = {
            str((item or {}).get("symbol", "") or "").strip()
            for item in (items or [])
            if str((item or {}).get("symbol", "") or "").strip()
        }
        if syms:
            symbols_by_market[mkt] = syms

    days_on_list_map = {}
    heatmap_tag_map = {}

    if symbols_by_market:
        with get_ledger_session() as session:
            for mkt, syms in symbols_by_market.items():
                seed_rows = session.exec(
                    select(TrendSeedPool.symbol, TrendSeedPool.date)
                    .where(TrendSeedPool.market == mkt)
                    .where(TrendSeedPool.symbol.in_(list(syms)))
                    .where(TrendSeedPool.date >= cutoff)
                ).all()

                date_bucket = {}
                for sym, d in seed_rows:
                    key = (mkt, str(sym or "").strip())
                    if not key[1]:
                        continue
                    date_bucket.setdefault(key, set()).add(str(d))
                for key, dset in date_bucket.items():
                    days_on_list_map[key] = len(dset)

                bar_rows = session.exec(
                    select(TrendDailyBar.symbol, TrendDailyBar.date, TrendDailyBar.catalyst_tags)
                    .where(TrendDailyBar.market == mkt)
                    .where(TrendDailyBar.symbol.in_(list(syms)))
                    .where(TrendDailyBar.source == "heatmap")
                    .where(TrendDailyBar.date >= cutoff)
                    .order_by(TrendDailyBar.date.desc())
                ).all()

                for sym, _d, tag in bar_rows:
                    tag_text = str(tag or "").strip()
                    if not tag_text:
                        continue
                    key = (mkt, str(sym or "").strip())
                    if key[1] and key not in heatmap_tag_map:
                        heatmap_tag_map[key] = tag_text

    result = {}
    for mkt, items in raw.items():
        transformed = []

        inst_symbols = [
            str((it or {}).get("symbol", "") or "").strip()
            for it in (items or [])
            if str((it or {}).get("symbol", "") or "").strip()
        ]
        inst_map = {}
        try:
            inst_map = get_institutional_change_map(mkt, inst_symbols)
        except Exception as e:
            logger.debug(f"Trend inst factor failed: market={mkt} err={e}")
        for item in items:
            symbol = str(item.get("symbol", "") or "").strip()
            key = (mkt, symbol)

            reason_recs = item.get("reason_records", [])
            fallback_days = len(set(str(d) for d, _ in reason_recs)) if reason_recs else 0
            days_on_list = days_on_list_map.get(key, fallback_days if fallback_days > 0 else 1)

            trend_tag = str(item.get("aggregated_reason", "") or item.get("catalyst_tags", "") or "").strip()
            heatmap_tag = str(heatmap_tag_map.get(key, "") or "").strip()
            if heatmap_tag and heatmap_tag not in trend_tag:
                if (not trend_tag) or (trend_tag in {"暂无新闻催化", "Unknown", "unknown"}):
                    catalyst_tags = heatmap_tag
                else:
                    catalyst_tags = f"{trend_tag} | {heatmap_tag}"
            else:
                catalyst_tags = trend_tag or heatmap_tag

            market_cap = 0.0
            if mkt == "CN":
                market_cap = float(item.get("total_mv_100m", 0) or 0)
            elif mkt == "HK":
                market_cap = float(item.get("market_cap_100m_hkd", 0) or 0)
            elif mkt == "US":
                market_cap = float(item.get("market_cap_musd", 0) or 0) / 100.0

            inst_payload = inst_map.get(symbol, {}) or {}
            capital_signal = inst_payload.get("capital_signal")
            if not isinstance(capital_signal, dict):
                capital_signal = {"score": 0.0, "items": []}

            transformed.append({
                "symbol": symbol,
                "name": item.get("name", ""),
                "price": item.get("current_price", 0),
                "return_pct": item.get("return_pct", 0),
                "trend_score": item.get("trend_score", 0),
                "catalyst_tags": catalyst_tags,
                "days_on_list": days_on_list,
                "signal_strength": item.get("signal_strength", 0),
                "mcap_mult": item.get("mcap_mult", 1.0),
                "price_date": item.get("price_date", ""),
                "market_cap": market_cap,
                "inst_factor": float(inst_payload.get("inst_factor", 0) or 0),
                "inst_label": str(inst_payload.get("inst_label", "") or ""),
                "inst_change_pp": float(inst_payload.get("inst_change_pp", 0) or 0),
                "inst_delta_abs": float(inst_payload.get("inst_delta_abs", inst_payload.get("inst_change_pp", 0)) or 0),
                "inst_delta_pct": float(inst_payload.get("inst_delta_pct", 0) or 0),
                "inst_start_value": float(inst_payload.get("inst_start_value", 0) or 0),
                "inst_end_value": float(inst_payload.get("inst_end_value", 0) or 0),
                "inst_start_date": str(inst_payload.get("inst_start_date", "") or ""),
                "inst_end_date": str(inst_payload.get("inst_end_date", inst_payload.get("inst_date", "")) or ""),
                "inst_metric_unit": str(inst_payload.get("inst_metric_unit", "percentage_point") or "percentage_point"),
                "inst_date": str(inst_payload.get("inst_date", "") or ""),
                "inst_source": str(inst_payload.get("inst_source", "") or ""),
                "inst_text": str(inst_payload.get("inst_text", "") or ""),
                "inst_direction": str(inst_payload.get("inst_direction", "") or ""),
                "inst_holding": inst_payload.get("inst_holding", {}) or {},
                "capital_signal": capital_signal,
                "capital_signal_items": capital_signal.get("items", []) or [],
                "capital_signal_score": float(capital_signal.get("score", 0) or 0),
                "capital_signal_coverage": inst_payload.get("capital_signal_coverage", {}) or {},
            })
        result[mkt] = transformed
    return {"days": days, "limit": limit, "market": market_filter, "markets": result}


@app.get("/api/trend/slow")
def get_slow_trend(
    limit: int = Query(default=100, ge=10, le=200),
    market: str = Query(default=""),
    user: str = Depends(get_current_user),
):
    """慢趋势机构票 demo：复用现有 trend 候选池，单独做高门槛过滤，不触发推送。"""
    market_filter = _normalize_trend_market(market, {"CN", "HK", "US"})
    cache_key = f"api_trend_slow_demo_v5_{market_filter or 'ALL'}_{limit}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    from modules.monitor.trend_service import TrendCalculator, TrendService
    from modules.ingestion.institutional_factor import get_institutional_change_map
    from core.db import get_ledger_session
    from domain.ledger.analytics import TrendDailyBar, DailyRank, TrendSeedPool
    from sqlmodel import select
    from modules.ingestion.market_cap import get_cn_market_metrics, get_hk_market_metrics
    from modules.ingestion.us_market_cap import get_us_market_metrics, get_us_market_metrics_light
    import math

    def _env_float(name: str, default: float) -> float:
        try:
            raw = os.getenv(name, "")
            return float(raw) if str(raw).strip() else float(default)
        except Exception:
            return float(default)

    def _env_int(name: str, default: int) -> int:
        try:
            raw = os.getenv(name, "")
            return int(raw) if str(raw).strip() else int(default)
        except Exception:
            return int(default)

    def _display_symbol(symbol: str, market: str) -> str:
        sym = str(symbol or "").strip()
        if market == "US" and "." in sym:
            return sym.split(".")[-1].strip()
        return sym

    def _symbol_candidates(symbol: str, market: str) -> List[str]:
        raw = str(symbol or "").strip()
        if not raw:
            return []
        out = [raw]
        if market == "US":
            ticker = raw.split(".")[-1].strip() if "." in raw else raw
            out.extend([ticker, f"105.{ticker}", f"106.{ticker}", f"107.{ticker}"])
        seen = set()
        deduped = []
        for s in out:
            if s and s not in seen:
                deduped.append(s)
                seen.add(s)
        return deduped

    def _latest_amount(symbol: str, market: str) -> float:
        candidates = _symbol_candidates(symbol, market)
        if not candidates:
            return 0.0
        cutoff = dt.date.today() - dt.timedelta(days=30)
        amount = 0.0
        with get_ledger_session() as session:
            bar_rows = session.exec(
                select(TrendDailyBar.amount)
                .where(TrendDailyBar.market == market)
                .where(TrendDailyBar.symbol.in_(candidates))
                .where(TrendDailyBar.date >= cutoff)
                .order_by(TrendDailyBar.date.desc())
                .limit(5)
            ).all()
            for v in bar_rows:
                try:
                    amount = max(amount, float(v or 0))
                except Exception:
                    pass
            if market != "CF":
                rank_rows = session.exec(
                    select(DailyRank.amount)
                    .where(DailyRank.market == market)
                    .where(DailyRank.symbol.in_(candidates))
                    .where(DailyRank.date >= cutoff)
                    .order_by(DailyRank.date.desc())
                    .limit(5)
                ).all()
                for v in rank_rows:
                    try:
                        amount = max(amount, float(v or 0))
                    except Exception:
                        pass
        return amount

    def _local_series(symbol: str, market: str) -> List[Dict]:
        candidates = _symbol_candidates(symbol, market)
        if not candidates:
            return []
        cutoff = dt.date.today() - dt.timedelta(days=130)
        points = {}

        def _upsert(d, close, amount=0.0):
            try:
                c = float(close or 0)
                if not d or c <= 0:
                    return
                amt = float(amount or 0)
            except Exception:
                return
            cur = points.get(d) or {"date": d, "close": c, "amount": 0.0}
            cur["close"] = c
            if amt > float(cur.get("amount", 0) or 0):
                cur["amount"] = amt
            points[d] = cur

        with get_ledger_session() as session:
            bar_rows = session.exec(
                select(TrendDailyBar.date, TrendDailyBar.close, TrendDailyBar.amount)
                .where(TrendDailyBar.market == market)
                .where(TrendDailyBar.symbol.in_(candidates))
                .where(TrendDailyBar.date >= cutoff)
                .order_by(TrendDailyBar.date.desc())
                .limit(130)
            ).all()
            for d, close, amount in bar_rows:
                _upsert(d, close, amount)
            if market != "CF":
                rank_rows = session.exec(
                    select(DailyRank.date, DailyRank.price, DailyRank.amount)
                    .where(DailyRank.market == market)
                    .where(DailyRank.symbol.in_(candidates))
                    .where(DailyRank.date >= cutoff)
                    .order_by(DailyRank.date.desc())
                    .limit(130)
                ).all()
                for d, close, amount in rank_rows:
                    _upsert(d, close, amount)
        return sorted(points.values(), key=lambda x: x["date"])

    def _series_return(series: List[Dict], days: int):
        if not series:
            return 0.0, 0.0, ""
        cur = series[-1]
        cur_date = cur["date"]
        cur_price = float(cur.get("close", 0) or 0)
        target = cur_date - dt.timedelta(days=days)
        past = None
        for p in series:
            if p["date"] <= target:
                past = p
            else:
                break
        past = past or series[0]
        past_price = float(past.get("close", 0) or 0)
        if cur_price <= 0 or past_price <= 0:
            return 0.0, cur_price, str(cur_date)
        return round((cur_price - past_price) / past_price * 100, 2), round(cur_price, 4), str(cur_date)

    def _build_slow_candidates() -> Dict[str, List[Dict]]:
        """Build an independent slow-trend universe from local seeds and high-liquidity history."""
        markets = [market_filter] if market_filter else ["CN", "HK", "US"]
        max_per_market = max(60, min(500, _env_int("SLOW_TREND_CANDIDATE_MAX_PER_MARKET", 240)))
        stores: Dict[str, Dict[str, Dict]] = {m: {} for m in markets}

        def _add(market: str, symbol: str, name: str = "", reason: str = "", amount: float = 0.0, date=None, priority: int = 0):
            mkt = str(market or "").strip().upper()
            raw_sym = str(symbol or "").strip()
            if mkt not in stores or not raw_sym:
                return
            display = _display_symbol(raw_sym, mkt)
            key = display or raw_sym
            bucket = stores[mkt]
            rec = bucket.get(key)
            if not rec:
                rec = {
                    "market": mkt,
                    "symbol": raw_sym,
                    "display_symbol": display,
                    "name": str(name or ""),
                    "reason_records": [],
                    "amount_hint": 0.0,
                    "date_hint": None,
                    "priority": 0,
                    "source_tags": set(),
                }
                bucket[key] = rec
            if name and not rec.get("name"):
                rec["name"] = str(name)
            if reason:
                rec["reason_records"].append((date or dt.date.today(), str(reason).strip()))
                rec["source_tags"].add(str(reason).strip()[:16])
            try:
                amt = float(amount or 0)
                if amt > float(rec.get("amount_hint", 0) or 0):
                    rec["amount_hint"] = amt
            except Exception:
                pass
            if date and (not rec.get("date_hint") or date > rec.get("date_hint")):
                rec["date_hint"] = date
            if priority > int(rec.get("priority", 0) or 0):
                rec["priority"] = priority

        try:
            for market, items in TrendService._baseline_seed_items().items():
                for it in items:
                    _add(market, it.get("symbol", ""), it.get("name", ""), it.get("reason", "大票趋势基线"), priority=10)
        except Exception as e:
            logger.debug("Slow trend baseline candidates failed: %s", e)

        for collector, priority, fallback_reason in (
            (TrendService._watchlist_seed_items, 9, "自选观察"),
            (TrendService._paper_trade_seed_items, 9, "模拟持仓"),
        ):
            try:
                for market, items in collector().items():
                    for it in items:
                        _add(market, it.get("symbol", ""), it.get("name", ""), it.get("reason", fallback_reason), priority=priority)
            except Exception as e:
                logger.debug("Slow trend manual candidates failed: %s", e)

        today = dt.date.today()
        seed_cutoff = today - dt.timedelta(days=180)
        rank_cutoff = today - dt.timedelta(days=60)
        bar_cutoff = today - dt.timedelta(days=130)
        with get_ledger_session() as session:
            seed_rows = session.exec(
                select(TrendSeedPool.market, TrendSeedPool.symbol, TrendSeedPool.name, TrendSeedPool.daily_reason, TrendSeedPool.date)
                .where(TrendSeedPool.market.in_(markets))
                .where(TrendSeedPool.date >= seed_cutoff)
                .order_by(TrendSeedPool.date.desc())
                .limit(max_per_market * len(markets) * 4)
            ).all()
            for market, symbol, name, reason, d in seed_rows:
                _add(market, symbol, name, reason or "趋势种子池", date=d, priority=6)

            for market in markets:
                rank_rows = session.exec(
                    select(DailyRank.symbol, DailyRank.name, DailyRank.amount, DailyRank.date)
                    .where(DailyRank.market == market)
                    .where(DailyRank.date >= rank_cutoff)
                    .order_by(DailyRank.amount.desc(), DailyRank.date.desc())
                    .limit(max_per_market * 5)
                ).all()
                for symbol, name, amount, d in rank_rows:
                    _add(market, symbol, name, "高成交额排行榜", amount=amount, date=d, priority=4)

                bar_rows = session.exec(
                    select(TrendDailyBar.symbol, TrendDailyBar.name, TrendDailyBar.amount, TrendDailyBar.date)
                    .where(TrendDailyBar.market == market)
                    .where(TrendDailyBar.date >= bar_cutoff)
                    .order_by(TrendDailyBar.amount.desc(), TrendDailyBar.date.desc())
                    .limit(max_per_market * 5)
                ).all()
                for symbol, name, amount, d in bar_rows:
                    _add(market, symbol, name, "高成交额日线池", amount=amount, date=d, priority=3)

        out: Dict[str, List[Dict]] = {}
        for market, recs in stores.items():
            arr = list(recs.values())
            arr.sort(
                key=lambda x: (
                    int(x.get("priority", 0) or 0),
                    float(x.get("amount_hint", 0) or 0),
                    x.get("date_hint") or dt.date.min,
                ),
                reverse=True,
            )
            for rec in arr:
                rec["source_tags"] = ",".join(sorted(rec.get("source_tags") or []))
            out[market] = arr[:max_per_market]
        return out

    min20 = _env_float("SLOW_TREND_RETURN_20D_MIN", 12.0)
    min60 = _env_float("SLOW_TREND_RETURN_60D_MIN", 30.0)
    amount_floor = {
        "CN": _env_float("SLOW_TREND_CN_AMOUNT_MIN", 1_000_000_000.0),
        "HK": _env_float("SLOW_TREND_HK_AMOUNT_MIN", 500_000_000.0),
        "US": _env_float("SLOW_TREND_US_AMOUNT_MIN", 200_000_000.0),
    }
    cap_floor = {
        "CN": _env_float("SLOW_TREND_CN_MCAP_MIN_100M", 800.0),
        "HK": _env_float("SLOW_TREND_HK_MCAP_MIN_100M_HKD", 1500.0),
        "US": _env_float("SLOW_TREND_US_MCAP_MIN_MUSD", 50_000.0),
    }
    hk_price_min = _env_float("SLOW_TREND_HK_PRICE_MIN", 5.0)
    ma_gap_min = _env_float("SLOW_TREND_MA_GAP_MIN", 2.0)
    high_gap_min = _env_float("SLOW_TREND_HIGH_GAP_MIN", -12.0)

    raw = _build_slow_candidates()
    filtered = {}

    for mkt, items in raw.items():
        if mkt not in {"CN", "HK", "US"}:
            continue
        rows = []
        for item in items or []:
            symbol = str(item.get("symbol", "") or "").strip()
            if not symbol:
                continue
            display_symbol = _display_symbol(symbol, mkt)
            series = _local_series(symbol, mkt)
            if len(series) < 30 or (series[-1]["date"] - series[0]["date"]).days < 45:
                continue
            ret20, current_price, price_date = _series_return(series, 20)
            ret60, _, _ = _series_return(series, 60)
            if ret60 < min60 or ret20 < min20:
                continue
            if mkt == "HK" and current_price < hk_price_min:
                continue

            closes = [float(p.get("close", 0) or 0) for p in series if float(p.get("close", 0) or 0) > 0]
            if len(closes) < 30:
                continue
            ma20 = sum(closes[-20:]) / min(20, len(closes))
            ma60_window = closes[-60:] if len(closes) >= 60 else closes
            ma60 = sum(ma60_window) / len(ma60_window)
            ma_gap_pct = (ma20 / ma60 - 1.0) * 100 if ma60 > 0 else 0.0
            high60 = max(ma60_window)
            high_gap_pct = (current_price / high60 - 1.0) * 100 if high60 > 0 else 0.0
            if ma_gap_pct < ma_gap_min or current_price < ma20 or high_gap_pct < high_gap_min:
                continue

            if mkt == "US" and TrendCalculator._is_leveraged_like(f"{display_symbol} {item.get('name', '')}"):
                continue

            cap_value = 0.0
            market_cap = 0.0
            try:
                if mkt == "CN":
                    cap_value = float(item.get("total_mv_100m", 0) or 0)
                    if cap_value <= 0:
                        metrics = get_cn_market_metrics(display_symbol or symbol)
                        cap_value = float((metrics or {}).get("total_mv_100m", 0) or 0)
                    market_cap = cap_value
                elif mkt == "HK":
                    cap_value = float(item.get("market_cap_100m_hkd", 0) or 0)
                    if cap_value <= 0:
                        metrics = get_hk_market_metrics(display_symbol or symbol)
                        cap_value = float((metrics or {}).get("market_cap_100m_hkd", 0) or 0)
                    market_cap = cap_value
                else:
                    cap_value = float(item.get("market_cap_musd", 0) or 0)
                    if cap_value <= 0:
                        metrics = get_us_market_metrics(display_symbol.split(".")[-1].strip())
                        cap_value = float((metrics or {}).get("market_cap_musd", 0) or 0)
                    market_cap = cap_value / 100.0
            except Exception as e:
                logger.debug("Slow trend market cap failed: market=%s symbol=%s err=%s", mkt, symbol, e)
                cap_value = 0.0
                market_cap = 0.0
            if cap_value <= 0 or cap_value < cap_floor.get(mkt, 0):
                continue

            amount = max(_latest_amount(symbol, mkt), float(series[-1].get("amount", 0) or 0))
            if amount < amount_floor.get(mkt, 0):
                continue

            amount_score = math.log10(max(1.0, amount / max(1.0, amount_floor[mkt]))) * 6.0
            cap_score = math.log10(max(1.0, cap_value / max(1.0, cap_floor[mkt]))) * 5.0
            structure_score = ma_gap_pct * 1.35 + max(0.0, 12.0 + high_gap_pct) * 0.7
            score = round(ret60 * 0.46 + ret20 * 0.30 + structure_score + amount_score + cap_score, 2)
            reason = f"慢趋势机构票 | 20日{ret20:+.1f}% | 60日{ret60:+.1f}% | MA20/60{ma_gap_pct:+.1f}% | 距60日高点{high_gap_pct:+.1f}%"
            item = dict(item)
            item.update({
                "symbol": symbol,
                "display_symbol": display_symbol,
                "current_price": current_price or item.get("current_price", 0),
                "price_date": price_date or item.get("price_date", ""),
                "return_pct": ret60,
                "return_20d": ret20,
                "return_60d": ret60,
                "trend_score": score,
                "slow_score": score,
                "amount": amount,
                "market_cap": market_cap,
                "ma_gap_pct": round(ma_gap_pct, 2),
                "high_gap_pct": round(high_gap_pct, 2),
                "slow_reason": reason,
                "aggregated_reason": reason,
                "catalyst_tags": reason,
                "signal_strength": max(1.0, float(item.get("signal_strength", 0) or 0)),
            })
            rows.append(item)
        rows.sort(key=lambda x: x.get("slow_score", x.get("trend_score", 0)), reverse=True)
        filtered[mkt] = rows[:limit]

    result = {}
    for mkt, items in filtered.items():
        inst_symbols = [str((it or {}).get("symbol", "") or "").strip() for it in items if str((it or {}).get("symbol", "") or "").strip()]
        inst_map = {}
        try:
            inst_map = get_institutional_change_map(mkt, inst_symbols)
        except Exception as e:
            logger.debug(f"Slow trend inst factor failed: market={mkt} err={e}")

        transformed = []
        for i, item in enumerate(items, 1):
            symbol = str(item.get("symbol", "") or "").strip()
            inst_payload = inst_map.get(symbol, {}) or {}
            capital_signal = inst_payload.get("capital_signal")
            if not isinstance(capital_signal, dict):
                capital_signal = {"score": 0.0, "items": []}
            transformed.append({
                "symbol": item.get("display_symbol") or symbol,
                "raw_symbol": symbol,
                "name": item.get("name", ""),
                "price": item.get("current_price", 0),
                "return_pct": item.get("return_60d", item.get("return_pct", 0)),
                "return_20d": item.get("return_20d", 0),
                "return_60d": item.get("return_60d", item.get("return_pct", 0)),
                "trend_score": item.get("slow_score", item.get("trend_score", 0)),
                "catalyst_tags": item.get("slow_reason") or item.get("aggregated_reason", ""),
                "days_on_list": len(set(str(d) for d, _ in item.get("reason_records", []))) or 1,
                "signal_strength": item.get("signal_strength", 0),
                "mcap_mult": item.get("mcap_mult", 1.0),
                "price_date": item.get("price_date", ""),
                "market_cap": item.get("market_cap", 0),
                "amount": item.get("amount", 0),
                "ma_gap_pct": item.get("ma_gap_pct", 0),
                "high_gap_pct": item.get("high_gap_pct", 0),
                "inst_factor": float(inst_payload.get("inst_factor", 0) or 0),
                "inst_label": str(inst_payload.get("inst_label", "") or ""),
                "inst_change_pp": float(inst_payload.get("inst_change_pp", 0) or 0),
                "inst_delta_abs": float(inst_payload.get("inst_delta_abs", inst_payload.get("inst_change_pp", 0)) or 0),
                "inst_delta_pct": float(inst_payload.get("inst_delta_pct", 0) or 0),
                "inst_start_value": float(inst_payload.get("inst_start_value", 0) or 0),
                "inst_end_value": float(inst_payload.get("inst_end_value", 0) or 0),
                "inst_start_date": str(inst_payload.get("inst_start_date", "") or ""),
                "inst_end_date": str(inst_payload.get("inst_end_date", inst_payload.get("inst_date", "")) or ""),
                "inst_metric_unit": str(inst_payload.get("inst_metric_unit", "percentage_point") or "percentage_point"),
                "inst_date": str(inst_payload.get("inst_date", "") or ""),
                "inst_source": str(inst_payload.get("inst_source", "") or ""),
                "inst_text": str(inst_payload.get("inst_text", "") or ""),
                "inst_direction": str(inst_payload.get("inst_direction", "") or ""),
                "inst_holding": inst_payload.get("inst_holding", {}) or {},
                "capital_signal": capital_signal,
                "capital_signal_items": capital_signal.get("items", []) or [],
                "capital_signal_score": float(capital_signal.get("score", 0) or 0),
                "capital_signal_coverage": inst_payload.get("capital_signal_coverage", {}) or {},
            })
        result[mkt] = transformed

    payload = {
        "mode": "slow",
        "days": 60,
        "limit": limit,
        "market": market_filter,
        "candidate_source": "baseline/watchlist/positions/seed_pool/daily_rank/trend_daily_bar",
        "thresholds": {
            "return_20d_min": min20,
            "return_60d_min": min60,
            "amount_floor": amount_floor,
            "cap_floor": cap_floor,
            "ma_gap_min": ma_gap_min,
            "high_gap_min": high_gap_min,
        },
        "markets": result,
    }
    set_cache(cache_key, payload, ttl=900)
    return payload


@app.get("/api/trend/daily")
def get_daily_hot_trend(
    date: str = Query(default=""),
    limit: int = Query(default=100, ge=10, le=200),
    market: str = Query(default=""),
    user: str = Depends(get_current_user),
):
    """当日强势榜：只使用本地 DailyRank/TrendDailyBar 截面，避免 Web 打开时触发全市场 API。"""
    market_filter = _normalize_trend_market(market, {"CN", "HK", "US"})
    cache_key = f"api_trend_daily_hot_v5_{market_filter or 'ALL'}_{date or 'latest'}_{limit}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    from core.db import get_ledger_session
    from domain.ledger.analytics import DailyRank, TrendDailyBar
    from modules.ingestion.market_cap import get_cn_market_metrics, get_hk_market_metrics
    from modules.ingestion.us_market_cap import get_us_market_metrics, get_us_market_metrics_light
    from modules.monitor.trend_service import TrendCalculator
    from sqlmodel import select, func
    import math

    markets = [market_filter] if market_filter else ["CN", "HK", "US"]

    def _env_float(name: str, default: float) -> float:
        try:
            raw = os.getenv(name, "")
            return float(raw) if str(raw).strip() else float(default)
        except Exception:
            return float(default)

    def _env_int(name: str, default: int) -> int:
        try:
            raw = os.getenv(name, "")
            return int(raw) if str(raw).strip() else int(default)
        except Exception:
            return int(default)

    requested_date = None
    if str(date or "").strip():
        try:
            requested_date = dt.date.fromisoformat(str(date).strip()[:10])
        except Exception:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    thresholds = {
        "CN": {
            "amount_min": _env_float("DAILY_HOT_CN_AMOUNT_MIN", _env_float("CN_HARD_AMOUNT_MIN", 200_000_000.0)),
            "mcap_min_100m": _env_float("DAILY_HOT_CN_MCAP_MIN_100M", _env_float("CN_HARD_TOTAL_MV_100M_MIN", 50.0)),
            "price_min": _env_float("DAILY_HOT_CN_PRICE_MIN", 1.0),
        },
        "HK": {
            "amount_min": _env_float("DAILY_HOT_HK_AMOUNT_MIN", 500_000_000.0),
            "price_min": _env_float("DAILY_HOT_HK_PRICE_MIN", 2.0),
        },
        "US": {
            "amount_min": _env_float("DAILY_HOT_US_AMOUNT_MIN", 20_000_000.0),
            "mcap_min_musd": _env_float("DAILY_HOT_US_MCAP_MIN_MUSD", _env_float("US_HARD_MCAP_MUSD_MIN", 1000.0)),
            "price_min": _env_float("DAILY_HOT_US_PRICE_MIN", 2.0),
        },
    }
    cap_lookup_limits = {
        "CN": max(0, min(160, _env_int("DAILY_HOT_CN_MCAP_LOOKUP_LIMIT", _env_int("DAILY_HOT_MCAP_LOOKUP_LIMIT", 80)))),
        "HK": max(0, min(160, _env_int("DAILY_HOT_HK_MCAP_LOOKUP_LIMIT", _env_int("DAILY_HOT_MCAP_LOOKUP_LIMIT", 80)))),
        "US": max(0, min(80, _env_int("DAILY_HOT_US_MCAP_LOOKUP_LIMIT", _env_int("DAILY_HOT_MCAP_LOOKUP_LIMIT", 30)))),
    }

    def _to_float(value) -> float:
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    def _display_symbol(symbol: str, market: str) -> str:
        sym = str(symbol or "").strip()
        if market == "US" and "." in sym:
            return sym.split(".")[-1].strip().upper()
        return sym

    def _is_hk_product(symbol: str, name: str) -> bool:
        text = f"{symbol} {name}".lower()
        bad_words = [
            "牛", "熊", "认购", "认沽", "窝轮", "涡轮", "权证", "界内证",
            "杠杆", "反向", "做空", "两倍", "二倍", "三倍", "xl二", "xl三", "etf",
            "daily inverse", "daily leveraged", "leveraged", "inverse",
        ]
        return any(k in text for k in bad_words)

    def _is_us_product(symbol: str, name: str) -> bool:
        text = f"{symbol} {name}"
        if TrendCalculator._is_leveraged_like(text):
            return True
        low = text.lower()
        bad_words = [
            " warrant", " right", " unit", " etf", "proshares", "direxion",
            "graniteshares", "tradr", "t-rex", "yieldmax", "rex shares",
            "做空", "做多", "二倍", "两倍", "三倍", "杠杆", "反向", "etf",
        ]
        return any(k in low for k in bad_words)

    def _latest_market_date(session, market: str):
        def _max_date(model, upper=None):
            stmt = select(func.max(model.date)).where(model.market == market)
            if upper is not None:
                stmt = stmt.where(model.date <= upper)
            return session.exec(stmt).first()

        if requested_date is not None:
            rank_hit = session.exec(
                select(func.count()).select_from(DailyRank)
                .where(DailyRank.market == market)
                .where(DailyRank.date == requested_date)
            ).first()
            bar_hit = session.exec(
                select(func.count()).select_from(TrendDailyBar)
                .where(TrendDailyBar.market == market)
                .where(TrendDailyBar.date == requested_date)
            ).first()
            if (rank_hit or 0) > 0 or (bar_hit or 0) > 0:
                return requested_date
            dates = [d for d in (_max_date(DailyRank, requested_date), _max_date(TrendDailyBar, requested_date)) if d]
            return max(dates) if dates else None

        dates = [d for d in (_max_date(DailyRank), _max_date(TrendDailyBar)) if d]
        return max(dates) if dates else None

    def _merge_candidate(store: dict, market: str, symbol: str, name: str, price: float, pct: float, amount: float, turnover: float, source: str, catalyst: str, d):
        display = _display_symbol(symbol, market)
        if not display:
            return
        rec = store.get(display)
        if not rec:
            rec = {
                "symbol": display,
                "raw_symbol": str(symbol or "").strip(),
                "name": str(name or ""),
                "price": round(_to_float(price), 4),
                "return_pct": round(_to_float(pct), 2),
                "amount": _to_float(amount),
                "turnover_rate": _to_float(turnover),
                "source_set": set(),
                "catalyst_tags": str(catalyst or "").strip(),
                "price_date": str(d or ""),
            }
            store[display] = rec
        else:
            if name and not rec.get("name"):
                rec["name"] = str(name)
            if _to_float(amount) > _to_float(rec.get("amount")):
                rec["amount"] = _to_float(amount)
            if _to_float(turnover) > _to_float(rec.get("turnover_rate")):
                rec["turnover_rate"] = _to_float(turnover)
            if _to_float(pct) > _to_float(rec.get("return_pct")):
                rec["return_pct"] = round(_to_float(pct), 2)
                rec["price"] = round(_to_float(price), 4)
            if catalyst and catalyst not in str(rec.get("catalyst_tags", "")):
                rec["catalyst_tags"] = f"{rec.get('catalyst_tags', '')} | {catalyst}".strip(" |")
            if d and (not rec.get("price_date") or str(d) > str(rec.get("price_date"))):
                rec["price_date"] = str(d)
        if source:
            rec.setdefault("source_set", set()).add(str(source))

    def _load_candidates_for_market(session, market: str, market_date) -> list:
        store = {}
        if not market_date:
            return []

        rank_rows = session.exec(
            select(DailyRank)
            .where(DailyRank.market == market)
            .where(DailyRank.date == market_date)
            .order_by(DailyRank.change_pct.desc(), DailyRank.amount.desc())
            .limit(500)
        ).all()
        for r in rank_rows:
            _merge_candidate(
                store, market, r.symbol, r.name, r.price, r.change_pct, r.amount, r.turnover_rate,
                f"daily_rank:{r.rank_type}", "当日日榜", r.date,
            )

        bar_rows = session.exec(
            select(TrendDailyBar)
            .where(TrendDailyBar.market == market)
            .where(TrendDailyBar.date == market_date)
            .order_by(TrendDailyBar.amount.desc(), TrendDailyBar.close.desc())
            .limit(700)
        ).all()
        for b in bar_rows:
            pct = 0.0
            if _to_float(b.open) > 0:
                pct = (_to_float(b.close) - _to_float(b.open)) / _to_float(b.open) * 100.0
            _merge_candidate(
                store, market, b.symbol, b.name, b.close, pct, b.amount, b.turnover_rate,
                f"bar:{b.source}", b.catalyst_tags or "", b.date,
            )
        return list(store.values())

    def _passes_pre_filter(market: str, item: dict) -> bool:
        pct = _to_float(item.get("return_pct"))
        price = _to_float(item.get("price"))
        amount = _to_float(item.get("amount"))
        if pct <= 0 or price <= 0:
            return False
        if market == "CN":
            return price >= thresholds["CN"]["price_min"] and amount >= thresholds["CN"]["amount_min"]
        if market == "HK":
            if _is_hk_product(item.get("symbol", ""), item.get("name", "")):
                return False
            return price >= thresholds["HK"]["price_min"] and amount >= thresholds["HK"]["amount_min"]
        if market == "US":
            if _is_us_product(item.get("symbol", ""), item.get("name", "")):
                return False
            return price >= thresholds["US"]["price_min"] and amount >= thresholds["US"]["amount_min"]
        return False

    def _enrich_and_filter_market_cap(market: str, rows: list) -> list:
        if not rows:
            return []
        sorted_for_lookup = sorted(rows, key=lambda x: (_to_float(x.get("return_pct")), _to_float(x.get("amount"))), reverse=True)
        lookup_limit = int(cap_lookup_limits.get(market, 30) or 0)
        lookup_symbols = {str(x.get("symbol", "")) for x in sorted_for_lookup[:lookup_limit]}
        out = []
        for item in rows:
            sym = str(item.get("symbol", "")).strip()
            cap_value = 0.0
            market_cap_display = 0.0
            if sym in lookup_symbols:
                try:
                    if market == "CN":
                        metrics = get_cn_market_metrics(sym)
                        cap_value = _to_float((metrics or {}).get("total_mv_100m"))
                        market_cap_display = cap_value
                    elif market == "HK":
                        metrics = get_hk_market_metrics(sym)
                        cap_value = _to_float((metrics or {}).get("market_cap_100m_hkd")) or _to_float((metrics or {}).get("market_cap_100m_usd"))
                        market_cap_display = cap_value
                    elif market == "US":
                        # UI daily list uses Sina first to avoid Finnhub/Yahoo rate limits.
                        metrics = get_us_market_metrics_light(sym)
                        cap_value = _to_float((metrics or {}).get("market_cap_musd"))
                        if 0 < cap_value < 100.0:
                            cap_value = 0.0
                        market_cap_display = cap_value / 100.0 if cap_value > 0 else 0.0
                except Exception as e:
                    logger.debug("Daily hot mcap failed: market=%s symbol=%s err=%s", market, sym, e)
            cap_known = cap_value > 0
            item["market_cap"] = round(market_cap_display, 4)
            item["mcap_known"] = cap_known
            item["mcap_value"] = round(cap_value, 4)

            # 与 heatmap 一致：市值拿到才执行硬市值过滤；拿不到不丢票，避免限流造成漏票。
            if market == "CN" and cap_known and cap_value < thresholds["CN"]["mcap_min_100m"]:
                continue
            if market == "US" and cap_known and cap_value < thresholds["US"]["mcap_min_musd"]:
                continue
            out.append(item)
        return out

    def _score_rows(market: str, rows: list) -> list:
        if not rows:
            return []

        def _rank_map(values: dict) -> dict:
            if not values:
                return {}
            ordered = sorted(values.items(), key=lambda kv: kv[1])
            n = len(ordered)
            if n <= 1:
                return {ordered[0][0]: 1.0} if ordered else {}
            return {key: (idx + 1) / n for idx, (key, _v) in enumerate(ordered)}

        pct_rank = _rank_map({i: _to_float(r.get("return_pct")) for i, r in enumerate(rows)})
        amount_rank = _rank_map({i: math.log10(max(1.0, _to_float(r.get("amount")))) for i, r in enumerate(rows)})
        turnover_values = {i: _to_float(r.get("turnover_rate")) for i, r in enumerate(rows) if _to_float(r.get("turnover_rate")) > 0}
        turnover_rank = _rank_map(turnover_values) if turnover_values else {}
        cap_values = {i: math.log10(max(1.0, _to_float(r.get("mcap_value")))) for i, r in enumerate(rows) if _to_float(r.get("mcap_value")) > 0}
        cap_rank = _rank_map(cap_values) if cap_values else {}

        for i, r in enumerate(rows):
            score = (
                pct_rank.get(i, 0.5) * 0.55
                + amount_rank.get(i, 0.5) * 0.25
                + cap_rank.get(i, 0.5) * 0.15
                + turnover_rank.get(i, 0.5) * 0.05
            ) * 100.0
            if market == "US":
                cap = _to_float(r.get("mcap_value"))
                if cap >= 300_000:
                    score *= 1.35
                elif cap >= 100_000:
                    score *= 1.22
                elif cap >= 50_000:
                    score *= 1.12
            sources = sorted(r.get("source_set") or [])
            base_tag = str(r.get("catalyst_tags", "") or "").strip()
            if not base_tag or base_tag == "当日日榜":
                base_tag = "当日强势 | 成交额达标"
            r["trend_score"] = round(score, 2)
            r["daily_score"] = round(score, 2)
            r["source"] = ",".join(sources)
            r["catalyst_tags"] = base_tag
            r["days_on_list"] = 1
            r["signal_strength"] = 1
            r["mcap_mult"] = 1.0
        rows.sort(key=lambda x: (_to_float(x.get("return_pct")), _to_float(x.get("trend_score")), _to_float(x.get("amount"))), reverse=True)
        return rows[:limit]

    result = {}
    market_dates = {}
    with get_ledger_session() as session:
        for mkt in markets:
            market_date = _latest_market_date(session, mkt)
            market_dates[mkt] = str(market_date or "")
            candidates = _load_candidates_for_market(session, mkt, market_date)
            pre_filtered = [x for x in candidates if _passes_pre_filter(mkt, x)]
            cap_filtered = _enrich_and_filter_market_cap(mkt, pre_filtered)
            final_rows = _score_rows(mkt, cap_filtered)
            result[mkt] = final_rows

    # 默认不拉机构/筹码，避免打开 Web 日榜时产生额外外部数据调用；需要时可开 ENABLE_DAILY_HOT_INST=1。
    if str(os.getenv("ENABLE_DAILY_HOT_INST", "0")).strip().lower() in {"1", "true", "yes", "on"}:
        try:
            from modules.ingestion.institutional_factor import get_institutional_change_map
            for mkt, rows in result.items():
                syms = [str(r.get("symbol", "") or "").strip() for r in rows if str(r.get("symbol", "") or "").strip()]
                inst_map = get_institutional_change_map(mkt, syms)
                for r in rows:
                    inst_payload = inst_map.get(str(r.get("symbol", "")), {}) or {}
                    capital_signal = inst_payload.get("capital_signal")
                    if not isinstance(capital_signal, dict):
                        capital_signal = {"score": 0.0, "items": []}
                    r.update({
                        "inst_factor": float(inst_payload.get("inst_factor", 0) or 0),
                        "inst_label": str(inst_payload.get("inst_label", "") or ""),
                        "inst_change_pp": float(inst_payload.get("inst_change_pp", 0) or 0),
                        "inst_delta_abs": float(inst_payload.get("inst_delta_abs", inst_payload.get("inst_change_pp", 0)) or 0),
                        "inst_delta_pct": float(inst_payload.get("inst_delta_pct", 0) or 0),
                        "inst_start_value": float(inst_payload.get("inst_start_value", 0) or 0),
                        "inst_end_value": float(inst_payload.get("inst_end_value", 0) or 0),
                        "inst_start_date": str(inst_payload.get("inst_start_date", "") or ""),
                        "inst_end_date": str(inst_payload.get("inst_end_date", inst_payload.get("inst_date", "")) or ""),
                        "inst_metric_unit": str(inst_payload.get("inst_metric_unit", "percentage_point") or "percentage_point"),
                        "inst_date": str(inst_payload.get("inst_date", "") or ""),
                        "inst_source": str(inst_payload.get("inst_source", "") or ""),
                        "inst_text": str(inst_payload.get("inst_text", "") or ""),
                        "inst_direction": str(inst_payload.get("inst_direction", "") or ""),
                        "capital_signal": capital_signal,
                        "capital_signal_items": capital_signal.get("items", []) or [],
                        "capital_signal_score": float(capital_signal.get("score", 0) or 0),
                        "capital_signal_coverage": inst_payload.get("capital_signal_coverage", {}) or {},
                    })
        except Exception as e:
            logger.debug("Daily hot inst factor skipped: %s", e)

    for rows in result.values():
        for r in rows:
            r.pop("source_set", None)
            r.pop("mcap_value", None)
            r.setdefault("inst_factor", 0.0)
            r.setdefault("inst_label", "")
            r.setdefault("inst_change_pp", 0.0)
            r.setdefault("inst_delta_abs", 0.0)
            r.setdefault("inst_delta_pct", 0.0)
            r.setdefault("inst_start_value", 0.0)
            r.setdefault("inst_end_value", 0.0)
            r.setdefault("inst_start_date", "")
            r.setdefault("inst_end_date", "")
            r.setdefault("inst_metric_unit", "percentage_point")
            r.setdefault("inst_date", "")
            r.setdefault("inst_text", "")
            r.setdefault("inst_source", "")
            r.setdefault("inst_direction", "")
            r.setdefault("capital_signal_items", [])
            r.setdefault("capital_signal_score", 0.0)
            r.setdefault("capital_signal_coverage", {"status": "unknown", "text": "日榜", "reason": "当日强势榜默认不拉低频筹码数据"})

    payload = {
        "mode": "daily",
        "date": str(requested_date or ""),
        "market_dates": market_dates,
        "limit": limit,
        "market": market_filter,
        "candidate_source": "local DailyRank + TrendDailyBar",
        "thresholds": thresholds,
        "cap_lookup_limits": cap_lookup_limits,
        "markets": result,
    }
    set_cache(cache_key, payload, ttl=600)
    return payload

# --------------- Routes: Heatmap ---------------
@app.get("/api/heatmap")
def get_heatmap(
    date: str = Query(default=""),
    market: str = Query(default="CN"),
    user: str = Depends(get_current_user)
):
    from core.db import db_manager
    from domain.ledger.analytics import TrendDailyBar
    from sqlmodel import Session, select

    if not date:
        date = dt.date.today().strftime("%Y-%m-%d")

    target_date = dt.date.fromisoformat(date)

    with Session(db_manager.ledger_engine) as session:
        stmt = (
            select(TrendDailyBar)
            .where(TrendDailyBar.date == target_date)
            .where(TrendDailyBar.market == market)
            .where(TrendDailyBar.source == "heatmap")
            .order_by(TrendDailyBar.id)
        )
        bars = session.exec(stmt).all()

    items = []
    for i, b in enumerate(bars, 1):
        pct = 0.0
        if b.open and b.open > 0:
            pct = (b.close - b.open) / b.open * 100
        items.append({
            "rank": i,
            "symbol": b.symbol,
            "name": b.name,
            "close": b.close,
            "change_pct": round(pct, 2),
            "amount": b.amount,
            "market_cap": 0.0,
            "catalyst_tags": b.catalyst_tags or "",
        })

    # Enrich with market cap (lightweight - only 5~10 items)
    _enrich_heatmap_market_cap(items, market)

    return {"date": date, "market": market, "items": items}


def _enrich_heatmap_market_cap(items: list, market: str):
    """Fetch market cap for heatmap items. Unit: 亿 (100m CNY/HKD/USD)."""
    if not items:
        return
    try:
        from modules.ingestion.market_cap import get_cn_market_metrics, get_hk_market_metrics
        from modules.ingestion.us_market_cap import get_us_market_metrics, get_us_market_metrics_light

        for item in items:
            raw_sym = item.get("symbol", "")
            # Strip exchange prefix for US (e.g., "105.INTC" -> "INTC")
            sym = raw_sym.split(".")[-1] if "." in raw_sym else raw_sym
            try:
                if market == "CN":
                    mc = get_cn_market_metrics(sym)
                    item["market_cap"] = mc.get("total_mv_100m", 0.0) or 0.0
                elif market == "US":
                    mc = get_us_market_metrics(sym)
                    item["market_cap"] = mc.get("market_cap_100m_usd", 0.0) or 0.0
                elif market == "HK":
                    mc = get_hk_market_metrics(sym)
                    item["market_cap"] = mc.get("market_cap_100m_hkd", 0.0) or mc.get("market_cap_100m_usd", 0.0) or 0.0
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Heatmap mcap enrich failed: {e}")

# --------------- Routes: Trading Days ---------------
@app.get("/api/trading-days")
def get_trading_days(month: str = Query(default=""), user: str = Depends(get_current_user)):
    """Return list of dates that have heatmap data for the given month."""
    from core.db import db_manager
    from domain.ledger.analytics import TrendDailyBar
    from sqlmodel import Session, select, col

    if not month:
        month = dt.date.today().strftime("%Y-%m")

    # Parse month range
    year, mon = int(month[:4]), int(month[5:7])
    start = dt.date(year, mon, 1)
    if mon == 12:
        end = dt.date(year + 1, 1, 1)
    else:
        end = dt.date(year, mon + 1, 1)

    with Session(db_manager.ledger_engine) as session:
        stmt = (
            select(TrendDailyBar.date)
            .where(TrendDailyBar.date >= start)
            .where(TrendDailyBar.date < end)
            .where(TrendDailyBar.source == "heatmap")
            .distinct()
        )
        dates = session.exec(stmt).all()

    return {"month": month, "days": sorted(set(str(d) for d in dates))}

# --------------- SPA Fallback ---------------
_STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web", "dist")

@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    """Serve Vue SPA - static files or index.html fallback."""
    file_path = os.path.join(_STATIC_DIR, full_path)
    if full_path and os.path.isfile(file_path):
        return FileResponse(file_path)
    index_path = os.path.join(_STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return JSONResponse({"detail": "Frontend not built yet. Run: cd web && npm run build"}, status_code=404)
