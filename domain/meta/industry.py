"""
Industry 行业分类表
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Index

from domain.base import AuditMixin, ExtraFieldMixin, LiteReservedFieldsMixin

if TYPE_CHECKING:
    from domain.meta.asset import Asset


class Industry(SQLModel, AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin, table=True):
    """
    行业分类表 - 存储申万、中信等行业分类体系
    
    设计说明:
    - 支持多级行业 (level + parent_code)
    - classification: 分类体系 (shenwan/citic/custom)
    """
    __tablename__ = "industry"
    __table_args__ = (
        Index("ix_industry_level", "level"),
        Index("ix_industry_parent", "parent_code"),
        Index("ix_industry_class", "classification"),
    )
    
    # 主键
    code: str = Field(
        primary_key=True,
        max_length=20,
        description="行业代码,如801010"
    )
    
    # 核心字段
    name: str = Field(
        nullable=False,
        max_length=100,
        description="行业名称"
    )
    level: int = Field(
        nullable=False,
        description="层级: 1/2/3"
    )
    parent_code: Optional[str] = Field(
        default=None,
        max_length=20,
        description="父级行业代码"
    )
    classification: str = Field(
        default="shenwan",
        max_length=50,
        description="分类体系: shenwan/citic/custom"
    )
    
    # 关系
    # asset_links: list["AssetIndustryLink"] = Relationship(back_populates="industry")


class AssetIndustryLink(SQLModel, AuditMixin, ExtraFieldMixin, table=True):
    """
    资产-行业关联表 - 多对多关系
    
    设计说明:
    - is_primary: 是否为主行业
    """
    __tablename__ = "asset_industry_link"
    __table_args__ = (
        Index("ix_ail_symbol_industry", "symbol", "industry_code", unique=True),
        Index("ix_ail_industry", "industry_code"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    symbol: str = Field(
        foreign_key="asset.symbol",
        nullable=False,
        max_length=20,
        description="资产代码"
    )
    industry_code: str = Field(
        foreign_key="industry.code",
        nullable=False,
        max_length=20,
        description="行业代码"
    )
    is_primary: bool = Field(
        default=True,
        description="是否为主行业"
    )
    
    # 关系
    # asset: "Asset" = Relationship(back_populates="industry_links")
    # industry: "Industry" = Relationship(back_populates="asset_links")


# 更新 forward references
from domain.meta.asset import Asset
Industry.model_rebuild()
AssetIndustryLink.model_rebuild()
