"""
Meta Domain Models - 元数据库表定义
"""
from domain.meta.asset import Asset, AssetExt
from domain.meta.concept import Concept, AssetConceptLink
from domain.meta.industry import Industry, AssetIndustryLink
from domain.meta.profile import AssetProfile
from domain.meta.field_mapping import FieldMapping
from domain.meta.sync_log import DataSyncLog
from domain.meta.peer_group import PeerGroup, PeerGroupMember
from domain.meta.financial import AssetFinancial

__all__ = [
    # 资产
    "Asset",
    "AssetExt",
    "AssetProfile",
    "AssetFinancial",
    # 概念
    "Concept",
    "AssetConceptLink",
    # 行业
    "Industry",
    "AssetIndustryLink",
    # 配置
    "FieldMapping",
    "DataSyncLog",
    # 同行
    "PeerGroup",
    "PeerGroupMember",
]

# 解决循环依赖: 集中更新 Forward Refs
Asset.model_rebuild()
AssetExt.model_rebuild()
AssetProfile.model_rebuild()
Concept.model_rebuild()
AssetConceptLink.model_rebuild()
Industry.model_rebuild()
AssetIndustryLink.model_rebuild()
PeerGroup.model_rebuild()
PeerGroupMember.model_rebuild()
