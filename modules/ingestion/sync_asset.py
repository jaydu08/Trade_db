"""
Sync Asset - 同步 A 股资产列表
"""
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlmodel import select

from core.db import db_manager
from domain.meta import Asset, DataSyncLog
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class AssetSyncer:
    """
    资产同步器 - 从 AkShare 同步股票列表到 meta.db
    """
    
    def __init__(self):
        self.sync_log_id: Optional[int] = None
    
    def _create_sync_log(self, sync_type: str = "FULL") -> int:
        """创建同步日志"""
        log = DataSyncLog(
            table_name="asset",
            sync_type=sync_type,
            data_source="akshare",
            started_at=datetime.utcnow(),
            status="RUNNING",
        )
        with db_manager.meta_session() as session:
            session.add(log)
            session.commit()
            session.refresh(log)
            return log.id
    
    def _update_sync_log(
        self,
        log_id: int,
        status: str,
        record_count: int = 0,
        error_msg: Optional[str] = None,
    ) -> None:
        """更新同步日志"""
        with db_manager.meta_session() as session:
            log = session.get(DataSyncLog, log_id)
            if log:
                log.status = status
                log.record_count = record_count
                log.ended_at = datetime.utcnow()
                log.error_msg = error_msg
                session.add(log)
    
    def _parse_listing_date(self, date_str: str) -> Optional[datetime]:
        """解析上市日期"""
        if pd.isna(date_str) or not date_str:
            return None
        try:
            # 尝试多种日期格式
            for fmt in ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(str(date_str), fmt).date()
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def _extract_symbol_name(self, row: pd.Series) -> tuple[str, str]:
        symbol_candidates = [
            "code", "代码", "symbol", "Symbol", "股票代码", "证券代码", "代码.1"
        ]
        name_candidates = [
            "name", "名称", "Name", "股票名称", "证券名称"
        ]
        symbol = ""
        name = ""
        for key in symbol_candidates:
            value = row.get(key, "")
            if value:
                symbol = str(value).strip()
                break
        for key in name_candidates:
            value = row.get(key, "")
            if value:
                name = str(value).strip()
                break
        return symbol, name

    def _get_market_df(self, market: str) -> pd.DataFrame:
        if market == "CN":
            return akshare_client.get_stock_info_a()
        if market == "HK":
            return akshare_client.get_stock_info_hk()
        if market == "US":
            return akshare_client.get_stock_info_us()
        raise ValueError(f"Unsupported market: {market}")

    def sync_market(self, market: str, full_sync: bool = True) -> dict:
        sync_type = "FULL" if full_sync else "INCREMENTAL"
        try:
            self.sync_log_id = self._create_sync_log(sync_type)
        except Exception as e:
            logger.error(f"Failed to create sync log: {e}")
            raise

        result = {
            "total": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0,
        }
        
        try:
            df = self._get_market_df(market)
            result["total"] = len(df)
            
            logger.info(f"Processing {len(df)} stocks for {market}...")
            
            with db_manager.meta_session() as session:
                for _, row in df.iterrows():
                    try:
                        symbol, name = self._extract_symbol_name(row)
                        
                        if not symbol or not name:
                            continue
                        
                        # 检查是否存在
                        existing = session.get(Asset, symbol)
                        
                        if existing:
                            # 更新
                            existing.name = name
                            existing.updated_at = datetime.utcnow()
                            session.add(existing)
                            result["updated"] += 1
                        else:
                            # 新增
                            asset = Asset(
                                symbol=symbol,
                                name=name,
                                market=market,
                                asset_type="STOCK",
                                listing_status="ACTIVE",
                            )
                            session.add(asset)
                            result["inserted"] += 1
                    
                    except Exception as e:
                        logger.error(f"Error processing stock {row}: {e}")
                        result["errors"] += 1
            
            # 更新同步日志
            self._update_sync_log(
                self.sync_log_id,
                status="SUCCESS",
                record_count=result["inserted"] + result["updated"],
            )
            
            logger.info(f"Asset sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Asset sync failed: {e}")
            if self.sync_log_id:
                self._update_sync_log(
                    self.sync_log_id,
                    status="FAILED",
                    error_msg=str(e),
                )
            raise

    def sync_markets(self, markets: list[str], full_sync: bool = True) -> dict:
        summary = {
            "markets": {},
            "total": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0,
        }
        for market in markets:
            result = self.sync_market(market, full_sync=full_sync)
            summary["markets"][market] = result
            summary["total"] += result.get("total", 0)
            summary["inserted"] += result.get("inserted", 0)
            summary["updated"] += result.get("updated", 0)
            summary["errors"] += result.get("errors", 0)
        return summary
    
    def get_all_assets(self, market: str = "CN") -> list[Asset]:
        """获取所有资产"""
        with db_manager.meta_session() as session:
            statement = select(Asset).where(Asset.market == market)
            return list(session.exec(statement).all())
    
    def get_asset(self, symbol: str) -> Optional[Asset]:
        """获取单个资产"""
        with db_manager.meta_session() as session:
            return session.get(Asset, symbol)
    
    def count_assets(self, market: str = "CN") -> int:
        """统计资产数量"""
        with db_manager.meta_session() as session:
            statement = select(Asset).where(Asset.market == market)
            return len(list(session.exec(statement).all()))


# 全局实例
asset_syncer = AssetSyncer()


def sync_assets(full_sync: bool = True) -> dict:
    """同步资产的便捷函数"""
    return asset_syncer.sync_market("CN", full_sync)


def sync_assets_by_markets(markets: list[str], full_sync: bool = True) -> dict:
    return asset_syncer.sync_markets(markets, full_sync)
