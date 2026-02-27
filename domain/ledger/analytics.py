"""
Analytics Data Models - 分析与监控数据晚模型
"""
import datetime as dt
from typing import Optional
from sqlmodel import Field, SQLModel


class DailyRank(SQLModel, table=True):
    """
    每日排行榜数据
    记录全市场排名的切片数据，避免实时拉取全市场数据。
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    date: dt.date = Field(index=True, description="排行榜日期")
    market: str = Field(index=True, description="市场 (CN, HK, US)")
    rank_type: str = Field(index=True, description="榜单类型 (如: 涨幅榜, 成交额榜)")
    symbol: str = Field(index=True)
    name: str = Field(default="")
    price: float = Field(default=0.0)
    change_pct: float = Field(default=0.0, description="涨跌幅 (%)")
    amount: float = Field(default=0.0, description="成交额")
    turnover_rate: float = Field(default=0.0, description="换手率 (%)")
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class WatchlistAlert(SQLModel, table=True):
    """
    自选股异动告警记录
    记录监控器发现的个股异动事件。
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)
    symbol: str = Field(index=True)
    name: str = Field(default="")
    market: str = Field(index=True)
    alert_reason: str = Field(description="异动原因 (如: 涨幅超阈值, 跌幅超阈值)")
    price: float = Field(default=0.0)
    change_pct: float = Field(default=0.0)
    status: str = Field(default="PENDING", description="处理状态")
