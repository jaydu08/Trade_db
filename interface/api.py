"""
FastAPI Web Interface for Trade_db
Port 9800, JWT auth, serves Vue3 SPA static files.
"""
import os
import datetime as dt
import logging
import time
from typing import Optional, List, Dict

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from jose import jwt, JWTError

logger = logging.getLogger(__name__)

# --------------- Config ---------------
JWT_SECRET = os.getenv("WEB_JWT_SECRET", "trade_db_secret_key_2026")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 365
WEB_USERNAME = os.getenv("WEB_USERNAME", "game2du")
WEB_PORT = int(os.getenv("WEB_PORT", "9800"))

# News cache: {symbol: {"summary": str, "ts": float}}
_news_cache: Dict[str, Dict] = {}

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
    from modules.ingestion.us_market_cap import get_us_market_metrics
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
    """Get cached news summary for a stock. Cache for 4 hours.
    Returns immediately from cache; fetches in background if stale."""
    import threading
    cache_key = f"{market}:{symbol}"
    now = time.time()
    cached = _news_cache.get(cache_key)
    if cached and (now - cached["ts"]) < 43200:  # 12 hours
        return cached["summary"]

    # Return old cache immediately, refresh in background
    def _refresh():
        try:
            from modules.ingestion.data_factory import DataManager
            factory = DataManager()
            query = name or symbol
            raw = factory.search(query=f"{query} 最新", limit_per_source=2, timeout=8)
            if raw and len(raw) > 10:
                summary = _llm_summarize_news(raw, name or symbol)
            else:
                summary = ""
            _news_cache[cache_key] = {"summary": summary, "ts": time.time()}
        except Exception as e:
            logger.debug(f"News refresh failed for {cache_key}: {e}")

    threading.Thread(target=_refresh, daemon=True).start()
    return cached["summary"] if cached else ""


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
@app.get("/api/trend")
def get_trend(days: int = Query(default=7, ge=3, le=30), user: str = Depends(get_current_user)):
    from modules.monitor.trend_service import TrendCalculator
    calc = TrendCalculator()
    raw = calc.calculate_trend(days=days)
    # Transform to frontend-friendly format with proper field names
    result = {}
    for mkt, items in raw.items():
        transformed = []
        for item in items:
            # Count days on list from reason_records
            reason_recs = item.get("reason_records", [])
            days_on_list = len(set(d for d, _ in reason_recs)) if reason_recs else 1
            transformed.append({
                "symbol": item.get("symbol", ""),
                "name": item.get("name", ""),
                "price": item.get("current_price", 0),
                "return_pct": item.get("return_pct", 0),
                "trend_score": item.get("trend_score", 0),
                "catalyst_tags": item.get("aggregated_reason", "") or item.get("catalyst_tags", ""),
                "days_on_list": days_on_list,
                "signal_strength": item.get("signal_strength", 0),
                "mcap_mult": item.get("mcap_mult", 1.0),
            })
        result[mkt] = transformed
    return {"days": days, "markets": result}

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
        from modules.ingestion.us_market_cap import get_us_market_metrics

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
