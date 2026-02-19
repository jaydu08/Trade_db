"""
AssetProfile 公司简介表
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Text

from domain.base import AuditMixin, ExtraFieldMixin, LiteReservedFieldsMixin

if TYPE_CHECKING:
    from domain.meta.asset import Asset


class AssetProfile(SQLModel, AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin, table=True):
    """
    公司简介表 - 存储公司详细信息(向量化前的原文)
    
    设计说明:
    - 与 Asset 一对一关系
    - 存储原文用于向量化
    - vector_id: 关联 ChromaDB 向量 ID
    """
    __tablename__ = "asset_profile"
    
    # 主键 (同时也是外键)
    symbol: str = Field(
        primary_key=True,
        foreign_key="asset.symbol",
        max_length=20,
        description="资产代码"
    )
    
    # 核心字段
    main_business: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="主营业务描述"
    )
    business_scope: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="经营范围"
    )
    products: Optional[str] = Field(
        default=None,
        max_length=1000,
        description="核心产品(逗号分隔)"
    )
    company_profile: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="公司简介"
    )
    
    # 向量关联
    vector_id: Optional[str] = Field(
        default=None,
        max_length=100,
        description="ChromaDB向量ID"
    )
    
    # 关系
    asset: Optional["Asset"] = Relationship(back_populates="profile")


# 更新 forward references
AssetProfile.model_rebuild()
