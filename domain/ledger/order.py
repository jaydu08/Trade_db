"""
Order 订单表
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, TYPE_CHECKING

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Index

from domain.base import AuditMixin, ExtraFieldMixin, LiteReservedFieldsMixin

if TYPE_CHECKING:
    from domain.ledger.signal import Signal


class Order(SQLModel, AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin, table=True):
    """
    订单记录表 - 存储交易订单
    
    设计说明:
    - 可关联 Signal，也可独立(手动下单)
    - 支持部分成交
    """
    __tablename__ = "order"
    __table_args__ = (
        Index("ix_order_signal", "signal_id"),
        Index("ix_order_symbol", "symbol"),
        Index("ix_order_status", "status"),
        Index("ix_order_submitted", "submitted_at"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 关联 (可选)
    signal_id: Optional[int] = Field(
        default=None,
        foreign_key="signal.id",
        description="关联信号ID(可空)"
    )
    
    # 订单基本信息
    symbol: str = Field(
        nullable=False,
        max_length=20,
        description="标的代码"
    )
    side: str = Field(
        nullable=False,
        max_length=10,
        description="方向: BUY/SELL"
    )
    order_type: str = Field(
        default="MARKET",
        max_length=20,
        description="订单类型: MARKET/LIMIT"
    )
    
    # 数量价格
    quantity: float = Field(
        nullable=False,
        description="委托数量"
    )
    price: Optional[float] = Field(
        default=None,
        description="委托价格(限价单)"
    )
    
    # 成交信息
    filled_qty: float = Field(
        default=0,
        description="已成交数量"
    )
    avg_price: Optional[float] = Field(
        default=None,
        description="成交均价"
    )
    
    # 状态
    status: str = Field(
        default="PENDING",
        max_length=20,
        description="状态: PENDING/SUBMITTED/PARTIAL/FILLED/CANCELLED/REJECTED"
    )
    
    # 时间
    submitted_at: Optional[datetime] = Field(
        default=None,
        description="提交时间"
    )
    filled_at: Optional[datetime] = Field(
        default=None,
        description="完成时间"
    )
    
    # 关系
    signal: Optional["Signal"] = Relationship(back_populates="orders")


# 更新 forward references
Order.model_rebuild()
