"""
DataSyncLog 数据同步日志表
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import SQLModel, Field
from sqlalchemy import Column, JSON, Index

from domain.base import AuditMixin


class DataSyncLog(SQLModel, AuditMixin, table=True):
    """
    数据同步日志表 - 追踪 AkShare 等数据源的同步状态
    
    设计说明:
    - 记录每次同步的开始/结束时间、状态、记录数
    - error_msg: 存储错误信息
    - extra: 存储额外同步参数
    """
    __tablename__ = "data_sync_log"
    __table_args__ = (
        Index("ix_dsl_table_status", "table_name", "status"),
        Index("ix_dsl_started", "started_at"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 同步目标
    table_name: str = Field(
        nullable=False,
        max_length=100,
        description="同步目标表名"
    )
    collection_name: Optional[str] = Field(
        default=None,
        max_length=100,
        description="向量库Collection名(如适用)"
    )
    
    # 同步类型
    sync_type: str = Field(
        nullable=False,
        max_length=20,
        description="同步类型: FULL/INCREMENTAL"
    )
    data_source: str = Field(
        nullable=False,
        max_length=50,
        description="数据来源: akshare/manual/llm"
    )
    
    # 时间追踪
    started_at: datetime = Field(
        nullable=False,
        default_factory=datetime.utcnow,
        description="开始时间"
    )
    ended_at: Optional[datetime] = Field(
        default=None,
        description="结束时间"
    )
    
    # 结果
    record_count: int = Field(
        default=0,
        description="同步记录数"
    )
    status: str = Field(
        nullable=False,
        default="RUNNING",
        max_length=20,
        description="状态: RUNNING/SUCCESS/FAILED"
    )
    error_msg: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="错误信息"
    )
    
    # 扩展
    extra: Optional[dict] = Field(
        default=None,
        sa_column=Column(JSON),
        description="额外同步参数"
    )
