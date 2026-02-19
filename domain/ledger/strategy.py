"""
Strategy 策略表
"""
from __future__ import annotations

from typing import Optional

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, JSON, Index

from domain.base import AuditMixin, ExtraFieldMixin, LiteReservedFieldsMixin


class Strategy(SQLModel, AuditMixin, LiteReservedFieldsMixin, ExtraFieldMixin, table=True):
    """
    策略注册表 - 存储策略元信息
    
    设计说明:
    - 每个策略有唯一名称和版本
    - params_schema: JSON Schema 定义策略参数格式
    """
    __tablename__ = "strategy"
    __table_args__ = (
        Index("ix_strategy_name", "name", unique=True),
        Index("ix_strategy_active", "is_active"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 基本信息
    name: str = Field(
        nullable=False,
        max_length=100,
        description="策略名称(唯一)"
    )
    display_name: Optional[str] = Field(
        default=None,
        max_length=100,
        description="显示名称"
    )
    strategy_version: str = Field(
        nullable=False,
        default="v1.0.0",
        max_length=20,
        description="策略版本"
    )
    description: Optional[str] = Field(
        default=None,
        max_length=1000,
        description="策略描述"
    )
    
    # 配置
    params_schema: Optional[dict] = Field(
        default=None,
        sa_column=Column(JSON),
        description="参数JSON Schema"
    )
    is_active: bool = Field(
        default=True,
        description="是否启用"
    )
    
    # 关系
    runs: list["StrategyRun"] = Relationship(back_populates="strategy")


class StrategyRun(SQLModel, AuditMixin, ExtraFieldMixin, table=True):
    """
    策略运行记录表 - 记录每次策略执行
    """
    __tablename__ = "strategy_run"
    __table_args__ = (
        Index("ix_sr_strategy", "strategy_id"),
        Index("ix_sr_status", "status"),
        Index("ix_sr_started", "started_at"),
    )
    
    id: Optional[int] = Field(default=None, primary_key=True)
    strategy_id: int = Field(
        foreign_key="strategy.id",
        nullable=False,
        description="关联策略ID"
    )
    
    # 运行模式
    run_mode: str = Field(
        nullable=False,
        default="PAPER",
        max_length=20,
        description="运行模式: BACKTEST/LIVE/PAPER"
    )
    
    # 参数
    params: Optional[dict] = Field(
        default=None,
        sa_column=Column(JSON),
        description="本次运行参数"
    )
    
    # 时间
    started_at: Optional[str] = Field(
        default=None,
        description="开始时间"
    )
    ended_at: Optional[str] = Field(
        default=None,
        description="结束时间"
    )
    
    # 结果
    status: str = Field(
        nullable=False,
        default="RUNNING",
        max_length=20,
        description="状态: RUNNING/SUCCESS/FAILED"
    )
    result_summary: Optional[dict] = Field(
        default=None,
        sa_column=Column(JSON),
        description="运行结果摘要"
    )
    error_msg: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="错误信息"
    )
    
    # 关系
    strategy: Optional[Strategy] = Relationship(back_populates="runs")
    signals: list["Signal"] = Relationship(back_populates="strategy_run")


# 避免循环导入
from domain.ledger.signal import Signal

# 更新 forward references
Strategy.model_rebuild()
StrategyRun.model_rebuild()
