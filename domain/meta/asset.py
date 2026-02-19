"""
Asset 资产表 - 元数据主库核心表
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Index

from domain.base import CoreTableMixin, ExtTableMixin, ExtFieldValueMixin


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
    profile: Optional["AssetProfile"] = Relationship(back_populates="asset")
    concept_links: list["AssetConceptLink"] = Relationship(back_populates="asset")
    industry_links: list["AssetIndustryLink"] = Relationship(back_populates="asset")
    extensions: list["AssetExt"] = Relationship(back_populates="asset")


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
    asset: Optional[Asset] = Relationship(back_populates="extensions")


# 导入关联模型以避免循环导入
from domain.meta.profile import AssetProfile
from domain.meta.concept import AssetConceptLink
from domain.meta.industry import AssetIndustryLink

# 更新 forward references
Asset.model_rebuild()
AssetExt.model_rebuild()
