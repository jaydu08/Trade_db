"""
Sync Industry - 同步行业分类
"""
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlmodel import select

from core.db import db_manager
from domain.meta import Industry, AssetIndustryLink, Asset, DataSyncLog
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class IndustrySyncer:
    """
    行业分类同步器 - 从 AkShare 同步东方财富行业分类
    """
    
    def __init__(self):
        self.sync_log: Optional[DataSyncLog] = None
    
    def _create_sync_log(self, table_name: str, sync_type: str = "FULL") -> int:
        """创建同步日志"""
        log = DataSyncLog(
            table_name=table_name,
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
    
    def sync_industry_boards(self) -> dict:
        """
        同步行业板块列表
        
        Returns:
            同步结果统计
        """
        sync_log_id = self._create_sync_log("industry", "FULL")
        
        result = {
            "total": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0,
        }
        
        try:
            # 获取行业板块列表
            df = akshare_client.get_industry_board_list()
            result["total"] = len(df)
            
            logger.info(f"Processing {len(df)} industry boards...")
            
            with db_manager.meta_session() as session:
                for _, row in df.iterrows():
                    try:
                        # 板块代码和名称
                        code = str(row.get("板块代码", "")).strip()
                        name = str(row.get("板块名称", "")).strip()
                        
                        if not code or not name:
                            continue
                        
                        # 检查是否存在
                        existing = session.get(Industry, code)
                        
                        if existing:
                            existing.name = name
                            existing.updated_at = datetime.utcnow()
                            session.add(existing)
                            result["updated"] += 1
                        else:
                            industry = Industry(
                                code=code,
                                name=name,
                                level=1,  # 东财行业板块默认为一级
                                classification="eastmoney",
                            )
                            session.add(industry)
                            result["inserted"] += 1
                    
                    except Exception as e:
                        logger.error(f"Error processing industry {row}: {e}")
                        result["errors"] += 1
            
            self._update_sync_log(
                sync_log_id,
                status="SUCCESS",
                record_count=result["inserted"] + result["updated"],
            )
            
            logger.info(f"Industry sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Industry sync failed: {e}")
            self._update_sync_log(sync_log_id, status="FAILED", error_msg=str(e))
            raise
    
    def sync_industry_constituents(self, limit: Optional[int] = None) -> dict:
        """
        同步行业分类成分股关联
        
        Args:
            limit: 限制同步的行业数量 (用于测试)
        
        Returns:
            同步结果统计
        """
        sync_log_id = self._create_sync_log("asset_industry_link", "FULL")
        
        result = {
            "industries_processed": 0,
            "links_inserted": 0,
            "links_updated": 0,
            "errors": 0,
        }
        
        try:
            # 获取所有行业
            with db_manager.meta_session() as session:
                statement = select(Industry)
                industries_data = [(i.code, i.name) for i in session.exec(statement).all()]
            
            if limit:
                industries_data = industries_data[:limit]
            
            logger.info(f"Processing constituents for {len(industries_data)} industries...")
            
            for industry_code, industry_name in industries_data:
                try:
                    # 获取成分股
                    df = akshare_client.get_industry_constituents(industry_name)
                    
                    if df is None or df.empty:
                        continue
                    
                    with db_manager.meta_session() as session:
                        for _, row in df.iterrows():
                            try:
                                symbol = str(row.get("代码", "")).strip()
                                
                                if not symbol:
                                    continue
                                
                                # 检查资产是否存在
                                asset = session.get(Asset, symbol)
                                if not asset:
                                    continue
                                
                                # 检查关联是否存在
                                statement = select(AssetIndustryLink).where(
                                    AssetIndustryLink.symbol == symbol,
                                    AssetIndustryLink.industry_code == industry_code,
                                )
                                existing = session.exec(statement).first()
                                
                                if existing:
                                    existing.updated_at = datetime.utcnow()
                                    session.add(existing)
                                    result["links_updated"] += 1
                                else:
                                    link = AssetIndustryLink(
                                        symbol=symbol,
                                        industry_code=industry_code,
                                        is_primary=True,
                                    )
                                    session.add(link)
                                    result["links_inserted"] += 1
                            
                            except Exception as e:
                                logger.error(f"Error processing link: {e}")
                                result["errors"] += 1
                    
                    result["industries_processed"] += 1
                    
                    if result["industries_processed"] % 10 == 0:
                        logger.info(f"Processed {result['industries_processed']} industries...")
                
                except Exception as e:
                    logger.error(f"Error processing industry {industry_name}: {e}")
                    result["errors"] += 1
            
            self._update_sync_log(
                sync_log_id,
                status="SUCCESS",
                record_count=result["links_inserted"] + result["links_updated"],
            )
            
            logger.info(f"Industry constituents sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Industry constituents sync failed: {e}")
            self._update_sync_log(sync_log_id, status="FAILED", error_msg=str(e))
            raise
    
    def get_all_industries(self) -> list[Industry]:
        """获取所有行业"""
        with db_manager.meta_session() as session:
            statement = select(Industry)
            return list(session.exec(statement).all())
    
    def get_industry_stocks(self, industry_code: str) -> list[str]:
        """获取行业下的所有股票代码"""
        with db_manager.meta_session() as session:
            statement = select(AssetIndustryLink.symbol).where(
                AssetIndustryLink.industry_code == industry_code
            )
            return list(session.exec(statement).all())
    
    def get_stock_industries(self, symbol: str) -> list[str]:
        """获取股票所属的所有行业"""
        with db_manager.meta_session() as session:
            statement = select(AssetIndustryLink.industry_code).where(
                AssetIndustryLink.symbol == symbol
            )
            return list(session.exec(statement).all())


# 全局实例
industry_syncer = IndustrySyncer()


def sync_industries() -> dict:
    """同步行业的便捷函数"""
    return industry_syncer.sync_industry_boards()


def sync_industry_constituents(limit: Optional[int] = None) -> dict:
    """同步行业成分股的便捷函数"""
    return industry_syncer.sync_industry_constituents(limit)
