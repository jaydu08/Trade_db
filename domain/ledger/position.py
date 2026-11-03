"""
Position 持仓表
"""
from __future__ import annotations

from typing import Optional

from sqlmodel import SQLModel, Field
from sqlalchemy import Index

from domain.base import AuditMixin, ExtraFieldMixin, LiteReservedFieldsMixin


class Position(SQLModel, AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin, table=True):
    """
    持仓记录表 - 当前持仓快照
    
    设计说明:
    - 每个标的只有一条记录
    - 实时更新市值和盈亏
    """
    __tablename__ = "position"
    __table_args__ = (
        Index("ix_position_symbol", "symbol", unique=True),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 持仓标的
    symbol: str = Field(
        nullable=False,
        max_length=20,
        description="标的代码"
    )
    
    # 持仓信息
    quantity: float = Field(
        nullable=False,
        description="持仓数量"
    )
    avg_cost: float = Field(
        nullable=False,
        description="持仓成本"
    )
    
    # 市值 & 盈亏 (快照)
    market_value: Optional[float] = Field(
        default=None,
        description="当前市值"
    )
    unrealized_pnl: Optional[float] = Field(
        default=None,
        description="浮动盈亏"
    )
    unrealized_pnl_pct: Optional[float] = Field(
        default=None,
        description="浮动盈亏比例"
    )
    realized_pnl: float = Field(
        default=0,
        description="已实现盈亏"
    )
