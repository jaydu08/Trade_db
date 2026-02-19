"""
Sync Profile - 同步公司简介并向量化存储到 ChromaDB
"""
import logging
from datetime import datetime
from typing import Optional
import hashlib

import pandas as pd
from sqlmodel import select

from core.db import db_manager, get_collection
from domain.meta import Asset, AssetProfile, DataSyncLog
from domain.vector import CompanyChunkDocument, CompanyChunkMetadata
from modules.ingestion.akshare_client import akshare_client

logger = logging.getLogger(__name__)


class ProfileSyncer:
    """
    公司简介同步器
    
    1. 从 AkShare 获取公司信息
    2. 存储到 AssetProfile 表
    3. 向量化后存储到 ChromaDB company_chunks collection
    """
    
    def __init__(self):
        self.collection = None
    
    def _get_collection(self):
        """获取向量库 collection"""
        if self.collection is None:
            self.collection = get_collection("company_chunks")
        return self.collection
    
    def _create_sync_log(self, table_name: str, sync_type: str = "FULL") -> DataSyncLog:
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
    
    def _generate_doc_id(self, symbol: str, chunk_type: str, version: int = 1) -> str:
        """生成文档 ID"""
        return f"{symbol}_{chunk_type}_v{version}"
    
    def _parse_stock_info(self, df: pd.DataFrame) -> dict:
        """解析股票信息 DataFrame 为字典"""
        if df is None or df.empty:
            return {}
        
        result = {}
        for _, row in df.iterrows():
            item = row.get("item", row.get("项目", ""))
            value = row.get("value", row.get("值", ""))
            if item and value:
                result[str(item).strip()] = str(value).strip()
        
        return result
    
    def sync_profile(self, symbol: str) -> Optional[dict]:
        """
        同步单个股票的公司简介
        
        Args:
            symbol: 股票代码
        
        Returns:
            同步结果
        """
        try:
            # 获取公司信息
            df = akshare_client.get_stock_profile(symbol)
            info = self._parse_stock_info(df)
            
            if not info:
                logger.warning(f"No profile info for {symbol}")
                return None
            
            # 提取关键字段
            main_business = info.get("主营业务", info.get("经营范围", ""))
            company_profile = info.get("公司简介", info.get("公司介绍", ""))
            
            # 保存到数据库
            with db_manager.meta_session() as session:
                # 检查资产是否存在
                asset = session.get(Asset, symbol)
                if not asset:
                    logger.warning(f"Asset {symbol} not found")
                    return None
                
                # 检查 profile 是否存在
                profile = session.get(AssetProfile, symbol)
                
                if profile:
                    profile.main_business = main_business
                    profile.company_profile = company_profile
                    profile.updated_at = datetime.utcnow()
                else:
                    profile = AssetProfile(
                        symbol=symbol,
                        main_business=main_business,
                        company_profile=company_profile,
                    )
                
                session.add(profile)
            
            # 向量化存储到 ChromaDB
            self._vectorize_profile(symbol, asset.name, main_business, company_profile)
            
            return {
                "symbol": symbol,
                "main_business": main_business[:100] if main_business else "",
                "profile_length": len(company_profile) if company_profile else 0,
            }
        
        except Exception as e:
            logger.error(f"Error syncing profile for {symbol}: {e}")
            return None
    
    def _vectorize_profile(
        self,
        symbol: str,
        name: str,
        main_business: str,
        company_profile: str,
    ) -> None:
        """
        将公司简介向量化存储到 ChromaDB
        
        创建多个 chunk:
        - overview: 公司简介
        - business: 主营业务
        """
        collection = self._get_collection()
        now = datetime.utcnow().isoformat()
        
        chunks_to_add = []
        
        # 公司简介 chunk
        if company_profile and len(company_profile) > 10:
            chunks_to_add.append({
                "id": self._generate_doc_id(symbol, "overview"),
                "document": f"{name}: {company_profile}",
                "metadata": {
                    "symbol": symbol,
                    "name": name,
                    "market": "CN",
                    "chunk_type": "overview",
                    "source": "akshare",
                    "confidence": 1.0,
                    "doc_version": 1,
                    "updated_at": now,
                }
            })
        
        # 主营业务 chunk
        if main_business and len(main_business) > 10:
            chunks_to_add.append({
                "id": self._generate_doc_id(symbol, "business"),
                "document": f"{name} 主营业务: {main_business}",
                "metadata": {
                    "symbol": symbol,
                    "name": name,
                    "market": "CN",
                    "chunk_type": "business",
                    "source": "akshare",
                    "confidence": 1.0,
                    "doc_version": 1,
                    "updated_at": now,
                }
            })
        
        # 批量添加到 ChromaDB
        if chunks_to_add:
            ids = [c["id"] for c in chunks_to_add]
            documents = [c["document"] for c in chunks_to_add]
            metadatas = [c["metadata"] for c in chunks_to_add]
            
            # 先删除旧的
            try:
                collection.delete(ids=ids)
            except Exception:
                pass
            
            # 添加新的
            collection.add(
                ids=ids,
                documents=documents,
                metadatas=metadatas,
            )
            
            logger.debug(f"Added {len(chunks_to_add)} chunks for {symbol}")
    
    def sync_profiles_batch(
        self,
        limit: Optional[int] = None,
        skip_existing: bool = True,
    ) -> dict:
        """
        批量同步公司简介
        
        Args:
            limit: 限制同步数量
            skip_existing: 是否跳过已有简介的股票
        
        Returns:
            同步结果统计
        """
        sync_log = self._create_sync_log("asset_profile", "FULL")
        
        result = {
            "total": 0,
            "synced": 0,
            "skipped": 0,
            "errors": 0,
        }
        
        try:
            # 获取所有资产
            with db_manager.meta_session() as session:
                statement = select(Asset).where(Asset.market == "CN")
                assets = list(session.exec(statement).all())
            
            if limit:
                assets = assets[:limit]
            
            result["total"] = len(assets)
            logger.info(f"Syncing profiles for {len(assets)} stocks...")
            
            for i, asset in enumerate(assets):
                try:
                    # 检查是否已有简介
                    if skip_existing:
                        with db_manager.meta_session() as session:
                            existing = session.get(AssetProfile, asset.symbol)
                            if existing and existing.main_business:
                                result["skipped"] += 1
                                continue
                    
                    # 同步简介
                    sync_result = self.sync_profile(asset.symbol)
                    
                    if sync_result:
                        result["synced"] += 1
                    else:
                        result["errors"] += 1
                    
                    # 进度日志
                    if (i + 1) % 50 == 0:
                        logger.info(f"Progress: {i + 1}/{len(assets)}")
                
                except Exception as e:
                    logger.error(f"Error processing {asset.symbol}: {e}")
                    result["errors"] += 1
            
            self._update_sync_log(
                sync_log.id,
                status="SUCCESS",
                record_count=result["synced"],
            )
            
            logger.info(f"Profile sync completed: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Profile sync failed: {e}")
            self._update_sync_log(sync_log.id, status="FAILED", error_msg=str(e))
            raise
    
    def search_companies(
        self,
        query: str,
        n_results: int = 10,
        chunk_type: Optional[str] = None,
    ) -> list[dict]:
        """
        语义搜索公司
        
        Args:
            query: 搜索查询
            n_results: 返回结果数量
            chunk_type: 限制 chunk 类型
        
        Returns:
            搜索结果列表
        """
        collection = self._get_collection()
        
        where_filter = None
        if chunk_type:
            where_filter = {"chunk_type": chunk_type}
        
        results = collection.query(
            query_texts=[query],
            n_results=n_results,
            where=where_filter,
        )
        
        # 格式化结果
        formatted = []
        if results and results["ids"]:
            for i, doc_id in enumerate(results["ids"][0]):
                formatted.append({
                    "id": doc_id,
                    "document": results["documents"][0][i] if results["documents"] else "",
                    "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                    "distance": results["distances"][0][i] if results["distances"] else None,
                })
        
        return formatted


# 全局实例
profile_syncer = ProfileSyncer()


def sync_profile(symbol: str) -> Optional[dict]:
    """同步单个股票简介"""
    return profile_syncer.sync_profile(symbol)


def sync_profiles(limit: Optional[int] = None, skip_existing: bool = True) -> dict:
    """批量同步公司简介"""
    return profile_syncer.sync_profiles_batch(limit, skip_existing)


def search_companies(query: str, n_results: int = 10) -> list[dict]:
    """语义搜索公司"""
    return profile_syncer.search_companies(query, n_results)
