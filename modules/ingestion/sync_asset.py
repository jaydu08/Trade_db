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
    资产同步器 - 从 AkShare 同步 A 股股票列表到 meta.db
    """
    
    def __init__(self):
        self.sync_log: Optional[DataSyncLog] = None
    
    def _create_sync_log(self, sync_type: str = "FULL") -> DataSyncLog:
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
            return log
    
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
    
    def sync_a_shares(self, full_sync: bool = True) -> dict:
        """
        同步 A 股股票列表
        
        Args:
            full_sync: 是否全量同步
        
        Returns:
            同步结果统计
        """
        sync_type = "FULL" if full_sync else "INCREMENTAL"
        self.sync_log = self._create_sync_log(sync_type)
        
        result = {
            "total": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0,
        }
        
        try:
            # 获取股票列表
            df = akshare_client.get_stock_info_a()
            result["total"] = len(df)
            
            logger.info(f"Processing {len(df)} stocks...")
            
            with db_manager.meta_session() as session:
                for _, row in df.iterrows():
                    try:
                        symbol = str(row.get("code", row.get("代码", ""))).strip()
                        name = str(row.get("name", row.get("名称", ""))).strip()
                        
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
                                market="CN",
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
                self.sync_log.id,
                status="SUCCESS",
                record_count=result["inserted"] + result["updated"],
            )
            
            logger.info(f"Asset sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Asset sync failed: {e}")
            self._update_sync_log(
                self.sync_log.id,
                status="FAILED",
                error_msg=str(e),
            )
            raise
    
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
    return asset_syncer.sync_a_shares(full_sync)
