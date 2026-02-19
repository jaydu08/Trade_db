"""
FieldMapping 字段映射表 - 管理预留字段的业务含义
"""
from __future__ import annotations

from typing import Optional

from sqlmodel import SQLModel, Field
from sqlalchemy import Index

from domain.base import AuditMixin


class FieldMapping(SQLModel, AuditMixin, table=True):
    """
    字段映射表 - 记录预留字段的业务含义
    
    设计说明:
    - 将 str_1, int_1 等预留字段映射到 short_name, employee_count 等逻辑字段
    - 支持动态属性访问
    """
    __tablename__ = "field_mapping"
    __table_args__ = (
        Index("ix_fm_table_physical", "table_name", "physical_name", unique=True),
        Index("ix_fm_table_logical", "table_name", "logical_name", unique=True),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 映射定义
    table_name: str = Field(
        nullable=False,
        max_length=100,
        description="表名: asset/signal/..."
    )
    physical_name: str = Field(
        nullable=False,
        max_length=50,
        description="物理字段名: str_1/int_2/..."
    )
    logical_name: str = Field(
        nullable=False,
        max_length=100,
        description="逻辑字段名: short_name/employee_count/..."
    )
    display_name: Optional[str] = Field(
        default=None,
        max_length=100,
        description="显示名称: 简称/员工数/..."
    )
    field_type: str = Field(
        nullable=False,
        max_length=20,
        description="字段类型: STR/INT/FLOAT/BOOL/DATE"
    )
    description: Optional[str] = Field(
        default=None,
        max_length=500,
        description="字段说明"
    )
    is_active: bool = Field(
        default=True,
        description="是否启用"
    )
