"""
Signal 交易信号表
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, TYPE_CHECKING

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, Text, Index

from domain.base import CoreTableMixin, ExtTableMixin, ExtFieldValueMixin

if TYPE_CHECKING:
    from domain.ledger.strategy import StrategyRun
    from domain.ledger.order import Order


class Signal(SQLModel, CoreTableMixin, table=True):
    """
    交易信号表 - 策略生成的交易信号
    
    设计说明:
    - 核心字段: symbol, direction, strength, reasoning
    - 预留字段: str_1~5, int_1~3, float_1~3, bool_1~2, date_1~2
    - reasoning: 存储 LLM 推理过程 (Chain of Thought)
    """
    __tablename__ = "signal"
    __table_args__ = (
        Index("ix_signal_run", "strategy_run_id"),
        Index("ix_signal_symbol", "symbol"),
        Index("ix_signal_status", "status"),
        Index("ix_signal_created", "created_at"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 关联
    strategy_run_id: Optional[int] = Field(
        default=None,
        foreign_key="strategy_run.id",
        description="关联策略运行ID"
    )
    
    # 核心信号字段
    symbol: str = Field(
        nullable=False,
        max_length=20,
        description="标的代码"
    )
    direction: str = Field(
        nullable=False,
        max_length=10,
        description="方向: LONG/SHORT/CLOSE"
    )
    strength: float = Field(
        nullable=False,
        description="信号强度 0.0-1.0"
    )
    
    # 推理过程
    reasoning: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="LLM推理过程(CoT)"
    )
    
    # 状态管理
    status: str = Field(
        default="PENDING",
        max_length=20,
        description="状态: PENDING/EXECUTED/EXPIRED/CANCELLED"
    )
    expired_at: Optional[datetime] = Field(
        default=None,
        description="信号过期时间"
    )
    
    # 关系
    strategy_run: Optional["StrategyRun"] = Relationship(back_populates="signals")
    orders: list["Order"] = Relationship(back_populates="signal")
    extensions: list["SignalExt"] = Relationship(back_populates="signal")


class SignalExt(SQLModel, ExtTableMixin, ExtFieldValueMixin, table=True):
    """
    信号扩展表 - 存储需要索引的动态扩展字段
    """
    __tablename__ = "signal_ext"
    __table_args__ = (
        Index("ix_signal_ext_signal_field", "signal_id", "field_name", unique=True),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    signal_id: int = Field(
        foreign_key="signal.id",
        nullable=False,
        description="关联信号ID"
    )
    
    # 关系
    signal: Optional[Signal] = Relationship(back_populates="extensions")


# 更新 forward references
Signal.model_rebuild()
SignalExt.model_rebuild()
