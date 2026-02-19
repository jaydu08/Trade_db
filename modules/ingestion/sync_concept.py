"""
Sync Concept - 同步概念板块
"""
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlmodel import select

from core.db import db_manager
from domain.meta import Concept, AssetConceptLink, Asset, DataSyncLog
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class ConceptSyncer:
    """
    概念板块同步器 - 从 AkShare 同步东方财富概念板块
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
    
    def sync_concept_boards(self) -> dict:
        """
        同步概念板块列表
        
        Returns:
            同步结果统计
        """
        sync_log_id = self._create_sync_log("concept", "FULL")
        
        result = {
            "total": 0,
            "inserted": 0,
            "updated": 0,
            "errors": 0,
        }
        
        try:
            # 获取概念板块列表
            df = akshare_client.get_concept_board_list()
            result["total"] = len(df)
            
            logger.info(f"Processing {len(df)} concept boards...")
            
            with db_manager.meta_session() as session:
                for _, row in df.iterrows():
                    try:
                        # 板块代码和名称
                        code = str(row.get("板块代码", "")).strip()
                        name = str(row.get("板块名称", "")).strip()
                        
                        if not code or not name:
                            continue
                        
                        # 检查是否存在
                        existing = session.get(Concept, code)
                        
                        if existing:
                            existing.name = name
                            existing.updated_at = datetime.utcnow()
                            session.add(existing)
                            result["updated"] += 1
                        else:
                            concept = Concept(
                                code=code,
                                name=name,
                                source="eastmoney",
                            )
                            session.add(concept)
                            result["inserted"] += 1
                    
                    except Exception as e:
                        logger.error(f"Error processing concept {row}: {e}")
                        result["errors"] += 1
            
            self._update_sync_log(
                sync_log_id,
                status="SUCCESS",
                record_count=result["inserted"] + result["updated"],
            )
            
            logger.info(f"Concept sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Concept sync failed: {e}")
            self._update_sync_log(sync_log_id, status="FAILED", error_msg=str(e))
            raise
    
    def sync_concept_constituents(self, limit: Optional[int] = None) -> dict:
        """
        同步概念板块成分股关联
        
        Args:
            limit: 限制同步的概念数量 (用于测试)
        
        Returns:
            同步结果统计
        """
        sync_log_id = self._create_sync_log("asset_concept_link", "FULL")
        
        result = {
            "concepts_processed": 0,
            "links_inserted": 0,
            "links_updated": 0,
            "errors": 0,
        }
        
        try:
            # 获取所有概念
            with db_manager.meta_session() as session:
                statement = select(Concept)
                concepts = list(session.exec(statement).all())
            
            if limit:
                concepts = concepts[:limit]
            
            logger.info(f"Processing constituents for {len(concepts)} concepts...")
            
            for concept in concepts:
                try:
                    # 获取成分股
                    df = akshare_client.get_concept_constituents(concept.name)
                    
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
                                statement = select(AssetConceptLink).where(
                                    AssetConceptLink.symbol == symbol,
                                    AssetConceptLink.concept_code == concept.code,
                                )
                                existing = session.exec(statement).first()
                                
                                if existing:
                                    existing.updated_at = datetime.utcnow()
                                    session.add(existing)
                                    result["links_updated"] += 1
                                else:
                                    link = AssetConceptLink(
                                        symbol=symbol,
                                        concept_code=concept.code,
                                        weight=1.0,
                                    )
                                    session.add(link)
                                    result["links_inserted"] += 1
                            
                            except Exception as e:
                                logger.error(f"Error processing link: {e}")
                                result["errors"] += 1
                    
                    result["concepts_processed"] += 1
                    
                    if result["concepts_processed"] % 10 == 0:
                        logger.info(f"Processed {result['concepts_processed']} concepts...")
                
                except Exception as e:
                    logger.error(f"Error processing concept {concept.name}: {e}")
                    result["errors"] += 1
            
            self._update_sync_log(
                sync_log_id,
                status="SUCCESS",
                record_count=result["links_inserted"] + result["links_updated"],
            )
            
            logger.info(f"Concept constituents sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Concept constituents sync failed: {e}")
            self._update_sync_log(sync_log_id, status="FAILED", error_msg=str(e))
            raise
    
    def get_all_concepts(self) -> list[Concept]:
        """获取所有概念"""
        with db_manager.meta_session() as session:
            statement = select(Concept)
            return list(session.exec(statement).all())
    
    def get_concept_stocks(self, concept_code: str) -> list[str]:
        """获取概念下的所有股票代码"""
        with db_manager.meta_session() as session:
            statement = select(AssetConceptLink.symbol).where(
                AssetConceptLink.concept_code == concept_code
            )
            return list(session.exec(statement).all())
    
    def get_stock_concepts(self, symbol: str) -> list[str]:
        """获取股票关联的所有概念"""
        with db_manager.meta_session() as session:
            statement = select(AssetConceptLink.concept_code).where(
                AssetConceptLink.symbol == symbol
            )
            return list(session.exec(statement).all())


# 全局实例
concept_syncer = ConceptSyncer()


def sync_concepts() -> dict:
    """同步概念的便捷函数"""
    return concept_syncer.sync_concept_boards()


def sync_concept_constituents(limit: Optional[int] = None) -> dict:
    """同步概念成分股的便捷函数"""
    return concept_syncer.sync_concept_constituents(limit)
