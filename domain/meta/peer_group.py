"""
PeerGroup 跨市场同行映射表
"""
from __future__ import annotations

from typing import Optional

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, JSON, Index

from domain.base import AuditMixin, ExtraFieldMixin


class PeerGroup(SQLModel, AuditMixin, ExtraFieldMixin, table=True):
    """
    跨市场同行组表 - 存储跨市场同行业公司分组
    
    设计说明:
    - 用于跨市场联动分析 (如: 国产CDN vs 美股NET)
    - 一个 PeerGroup 包含多个 PeerGroupMember
    """
    __tablename__ = "peer_group"
    __table_args__ = (
        Index("ix_pg_industry", "industry"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 基本信息
    group_name: str = Field(
        nullable=False,
        max_length=100,
        description="分组名称,如'全球CDN'"
    )
    industry: str = Field(
        nullable=False,
        max_length=100,
        description="所属行业"
    )
    description: Optional[str] = Field(
        default=None,
        max_length=500,
        description="分组说明"
    )
    
    # 关系
    members: list["PeerGroupMember"] = Relationship(back_populates="group")


class PeerGroupMember(SQLModel, AuditMixin, ExtraFieldMixin, table=True):
    """
    跨市场同行成员表 - PeerGroup 的成员
    """
    __tablename__ = "peer_group_member"
    __table_args__ = (
        Index("ix_pgm_group_symbol", "group_id", "symbol", unique=True),
        Index("ix_pgm_market", "market"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    group_id: int = Field(
        foreign_key="peer_group.id",
        nullable=False,
        description="所属分组ID"
    )
    
    # 成员信息
    symbol: str = Field(
        nullable=False,
        max_length=20,
        description="证券代码"
    )
    market: str = Field(
        nullable=False,
        max_length=10,
        description="市场: CN/US/HK"
    )
    weight: float = Field(
        default=1.0,
        description="权重 0-1"
    )
    is_leader: bool = Field(
        default=False,
        description="是否为行业龙头"
    )
    
    # 关系
    group: Optional[PeerGroup] = Relationship(back_populates="members")


# 更新 forward references
PeerGroup.model_rebuild()
PeerGroupMember.model_rebuild()
