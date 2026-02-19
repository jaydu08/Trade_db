"""
Concept 概念板块表
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Index

from domain.base import AuditMixin, ExtraFieldMixin, LiteReservedFieldsMixin

if TYPE_CHECKING:
    from domain.meta.asset import Asset


class Concept(SQLModel, AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin, table=True):
    """
    概念板块表 - 存储东方财富等来源的概念板块
    
    设计说明:
    - 支持层级概念 (parent_code)
    - 预留字段: str_1~2, int_1, float_1
    """
    __tablename__ = "concept"
    __table_args__ = (
        Index("ix_concept_source", "source"),
        Index("ix_concept_parent", "parent_code"),
    )
    
    # 主键
    code: str = Field(
        primary_key=True,
        max_length=20,
        description="概念代码,如BK0001"
    )
    
    # 核心字段
    name: str = Field(
        nullable=False,
        max_length=100,
        description="概念名称"
    )
    source: str = Field(
        default="eastmoney",
        max_length=50,
        description="数据来源: eastmoney/tonghuashun/custom"
    )
    parent_code: Optional[str] = Field(
        default=None,
        max_length=20,
        description="父级概念代码(支持层级)"
    )
    description: Optional[str] = Field(
        default=None,
        max_length=1000,
        description="概念描述"
    )
    
    # 关系
    asset_links: list["AssetConceptLink"] = Relationship(back_populates="concept")


class AssetConceptLink(SQLModel, AuditMixin, ExtraFieldMixin, table=True):
    """
    资产-概念关联表 - 多对多关系
    
    设计说明:
    - weight: 关联权重 0-1
    - is_primary: 是否为主要概念
    """
    __tablename__ = "asset_concept_link"
    __table_args__ = (
        Index("ix_acl_symbol_concept", "symbol", "concept_code", unique=True),
        Index("ix_acl_concept", "concept_code"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    symbol: str = Field(
        foreign_key="asset.symbol",
        nullable=False,
        max_length=20,
        description="资产代码"
    )
    concept_code: str = Field(
        foreign_key="concept.code",
        nullable=False,
        max_length=20,
        description="概念代码"
    )
    weight: float = Field(
        default=1.0,
        description="关联权重 0-1"
    )
    is_primary: bool = Field(
        default=False,
        description="是否为主要概念"
    )
    
    # 关系
    asset: Optional["Asset"] = Relationship(back_populates="concept_links")
    concept: Optional[Concept] = Relationship(back_populates="asset_links")


# 更新 forward references
Concept.model_rebuild()
AssetConceptLink.model_rebuild()
