"""
Asset 资产表 - 元数据主库核心表
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Index, Text

from domain.base import (
    CoreTableMixin, ExtTableMixin, ExtFieldValueMixin,
    AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin
)


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
    # asset: "Asset" = Relationship(back_populates="profile")


class Asset(SQLModel, CoreTableMixin, table=True):
    """
    资产主表 - 存储股票、ETF、可转债等资产基本信息
    
    设计说明:
    - 核心字段: symbol, name, market, asset_type, listing_status
    - 预留字段: str_1~5, int_1~3, float_1~3, bool_1~2, date_1~2
    - JSON扩展: extra
    """
    __tablename__ = "asset"
    __table_args__ = (
        Index("ix_asset_market", "market"),
        Index("ix_asset_type", "asset_type"),
        Index("ix_asset_status", "listing_status"),
    )
    
    # 主键
    symbol: str = Field(
        primary_key=True,
        max_length=20,
        description="证券代码,如000001/AAPL"
    )
    
    # 核心字段
    name: str = Field(
        nullable=False,
        max_length=100,
        description="证券名称"
    )
    market: str = Field(
        default="CN",
        max_length=10,
        description="市场: CN/US/HK"
    )
    asset_type: str = Field(
        default="STOCK",
        max_length=20,
        description="资产类型: STOCK/ETF/BOND/INDEX"
    )
    listing_date: Optional[date] = Field(
        default=None,
        description="上市日期"
    )
    listing_status: str = Field(
        default="ACTIVE",
        max_length=20,
        description="上市状态: ACTIVE/SUSPENDED/DELISTED"
    )
    
    # 关系
    # profile: Optional[AssetProfile] = Relationship(back_populates="asset")
    # concept_links: list["domain.meta.concept.AssetConceptLink"] = Relationship(back_populates="asset")
    # industry_links: list["domain.meta.industry.AssetIndustryLink"] = Relationship(back_populates="asset")
    # extensions: list["AssetExt"] = Relationship(back_populates="asset")


class AssetExt(SQLModel, ExtTableMixin, ExtFieldValueMixin, table=True):
    """
    资产扩展表 - 存储需要索引的动态扩展字段
    
    设计说明:
    - 用于存储需要强约束或索引的新增字段
    - 通过 field_name + field_type 定义字段
    - 通过 value_xxx 存储对应类型的值
    """
    __tablename__ = "asset_ext"
    __table_args__ = (
        Index("ix_asset_ext_symbol_field", "symbol", "field_name", unique=True),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    symbol: str = Field(
        foreign_key="asset.symbol",
        nullable=False,
        max_length=20,
        description="关联资产代码"
    )
    
    # 关系
    # asset: Asset = Relationship(back_populates="extensions")


# 导入关联模型以避免循环导入
try:
    from domain.meta.concept import AssetConceptLink
    from domain.meta.industry import AssetIndustryLink
except ImportError:
    pass

# 更新 forward references
Asset.model_rebuild()
AssetProfile.model_rebuild()
AssetExt.model_rebuild()
