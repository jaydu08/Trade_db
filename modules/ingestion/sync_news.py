"""
Sync News - 定向新闻采集（低频）
仅采集两类标的新闻：
1) trend/heatmap 相关标的
2) 自选观察 + 模拟持仓（ACTIVE）标的
"""
import datetime as dt
import hashlib
import logging
import os
from typing import Dict, List, Tuple

from sqlmodel import select

from core.db import get_collection, get_ledger_session
from domain.ledger.analytics import TrendSeedPool, DailyRank
from domain.ledger.paper_trade import PaperTrade
from modules.monitor.repository import WatchlistRepository
from modules.ingestion.data_factory import data_manager

logger = logging.getLogger(__name__)


class NewsSyncer:
    """定向新闻采集器（不使用 LLM，降低成本）"""

    def __init__(self):
        self.collection = get_collection("market_events")
        self.max_symbols = int(os.getenv("NEWS_TARGET_MAX_SYMBOLS", "24") or 24)
        self.limit_per_source = int(os.getenv("NEWS_LIMIT_PER_SOURCE", "2") or 2)
        self.search_timeout = int(os.getenv("NEWS_SEARCH_TIMEOUT", "12") or 12)
        self.trend_lookback_days = int(os.getenv("NEWS_TREND_LOOKBACK_DAYS", "2") or 2)
        self.rank_lookback_days = int(os.getenv("NEWS_RANK_LOOKBACK_DAYS", "1") or 1)

    @staticmethod
    def _norm_market(market: str) -> str:
        m = str(market or "").upper().strip()
        return m if m in {"CN", "HK", "US", "CF"} else "CN"

    @staticmethod
    def _market_text(market: str) -> str:
        return {
            "CN": "A股",
            "HK": "港股",
            "US": "美股",
            "CF": "商品期货",
        }.get(market, market)

    def _collect_trend_heatmap_symbols(self) -> List[Dict[str, str]]:
        """场景1：trend池 + heatmap(日榜 DailyRank)"""
        out: List[Dict[str, str]] = []
        today = dt.date.today()
        trend_cutoff = today - dt.timedelta(days=max(0, self.trend_lookback_days))
        rank_cutoff = today - dt.timedelta(days=max(0, self.rank_lookback_days))

        with get_ledger_session() as session:
            trend_rows = session.exec(
                select(TrendSeedPool)
                .where(TrendSeedPool.date >= trend_cutoff)
                .order_by(TrendSeedPool.date.desc())
            ).all()

            for r in trend_rows:
                out.append(
                    {
                        "symbol": str(r.symbol or "").strip(),
                        "name": str(r.name or "").strip(),
                        "market": self._norm_market(r.market),
                        "scene": "trend_heatmap",
                    }
                )

            rank_rows = session.exec(
                select(DailyRank)
                .where(DailyRank.date >= rank_cutoff)
                .order_by(DailyRank.date.desc())
            ).all()

            for r in rank_rows:
                out.append(
                    {
                        "symbol": str(r.symbol or "").strip(),
                        "name": str(r.name or "").strip(),
                        "market": self._norm_market(r.market),
                        "scene": "trend_heatmap",
                    }
                )

        return out

    def _collect_watch_hold_symbols(self) -> List[Dict[str, str]]:
        """场景2：自选观察 + 模拟持仓 ACTIVE"""
        out: List[Dict[str, str]] = []

        watch_data = WatchlistRepository().load_all()
        for _, item in (watch_data or {}).items():
            if not item or not item.get("is_active", True):
                continue
            out.append(
                {
                    "symbol": str(item.get("symbol", "")).strip(),
                    "name": str(item.get("name", "")).strip(),
                    "market": self._norm_market(item.get("market", "CN")),
                    "scene": "watch_hold",
                }
            )

        with get_ledger_session() as session:
            active_trades = session.exec(
                select(PaperTrade).where(PaperTrade.status == "ACTIVE")
            ).all()
            for t in active_trades:
                out.append(
                    {
                        "symbol": str(t.symbol or "").strip(),
                        "name": str(t.name or "").strip(),
                        "market": self._norm_market(t.market),
                        "scene": "watch_hold",
                    }
                )

        return out

    def _build_targets(self, limit: int) -> List[Dict[str, str]]:
        raw = self._collect_watch_hold_symbols() + self._collect_trend_heatmap_symbols()
        merged: Dict[Tuple[str, str], Dict[str, str]] = {}

        for item in raw:
            symbol = str(item.get("symbol", "")).strip()
            market = self._norm_market(item.get("market"))
            if not symbol:
                continue
            key = (market, symbol)
            if key not in merged:
                merged[key] = {
                    "symbol": symbol,
                    "name": str(item.get("name", "")).strip(),
                    "market": market,
                    "scene": str(item.get("scene", "")).strip() or "unknown",
                }
            else:
                # 优先保留带名称条目，场景合并
                if not merged[key].get("name") and item.get("name"):
                    merged[key]["name"] = str(item.get("name", "")).strip()
                prev_scene = merged[key].get("scene", "")
                scene = str(item.get("scene", "")).strip()
                if scene and scene not in prev_scene:
                    merged[key]["scene"] = f"{prev_scene},{scene}" if prev_scene else scene

        # 优先自选/持仓，再 trend/heatmap
        items = list(merged.values())
        items.sort(key=lambda x: (0 if "watch_hold" in x.get("scene", "") else 1, x["market"], x["symbol"]))

        hard_limit = max(1, min(int(limit or self.max_symbols), self.max_symbols))
        return items[:hard_limit]

    def _build_query(self, item: Dict[str, str]) -> str:
        symbol = item["symbol"]
        name = item.get("name", "")
        market = item.get("market", "CN")
        mtxt = self._market_text(market)
        base = f"{symbol} {name} {mtxt}"
        if market == "US":
            return f"{base} stock news earnings guidance SEC filing"
        if market == "HK":
            return f"{base} 股票 新闻 公告 盈利预告 资金流向"
        return f"{base} 股票 新闻 公告 业绩 资金流向"

    def _store_event(self, item: Dict[str, str], news_text: str) -> bool:
        if not news_text:
            return False
        if "所有搜索引擎均未返回有效结果" in news_text or "本地未配置任何有效的搜索引擎" in news_text:
            return False

        symbol = item["symbol"]
        market = item["market"]
        scene = item.get("scene", "unknown")
        event_date = dt.date.today().isoformat()

        digest = hashlib.md5(f"{event_date}|{market}|{symbol}|{news_text[:500]}".encode("utf-8", errors="ignore")).hexdigest()[:12]
        doc_id = f"evt_news_{event_date}_{market}_{symbol}_{digest}"

        doc_text = (
            f"【定向新闻】{item.get('name', '')}({symbol}-{market})\n"
            f"场景: {scene}\n"
            f"{news_text[:5000]}"
        )

        metadata = {
            "event_type": "news",
            "event_date": event_date,
            "impact": "neutral",
            "impact_score": 0.5,
            "source": f"targeted_news:{scene}",
            "related_symbols": symbol,
            "market": market,
            "doc_version": 1,
            "created_at": str(dt.datetime.utcnow()),
        }

        try:
            self.collection.add(ids=[doc_id], documents=[doc_text], metadatas=[metadata])
            return True
        except Exception as e:
            # 重复 id / 临时写入失败时跳过，不影响主流程
            logger.warning("Store targeted news failed for %s-%s: %s", market, symbol, e)
            return False

    def sync_news_stream(self, limit: int = 20) -> Dict[str, int]:
        """
        定向新闻同步（低频）
        - 不做 LLM 分析
        - 仅定向标的搜索并入 market_events
        """
        result = {
            "targets": 0,
            "searched": 0,
            "stored": 0,
            "skipped": 0,
            "errors": 0,
        }

        targets = self._build_targets(limit)
        result["targets"] = len(targets)
        if not targets:
            logger.info("No target symbols for targeted news sync.")
            return result

        logger.info("Targeted news sync started: targets=%s", len(targets))

        for item in targets:
            query = self._build_query(item)
            try:
                news_text = data_manager.search(
                    query,
                    limit_per_source=self.limit_per_source,
                    timeout=self.search_timeout,
                )
                result["searched"] += 1
                if self._store_event(item, str(news_text or "")):
                    result["stored"] += 1
                else:
                    result["skipped"] += 1
            except Exception as e:
                result["errors"] += 1
                logger.warning("Targeted news search failed for %s-%s: %s", item.get("market"), item.get("symbol"), e)

        logger.info("Targeted news sync done: %s", result)
        return result


news_syncer = NewsSyncer()
