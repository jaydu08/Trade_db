from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field

class Watchlist(SQLModel, table=True):
    """
    自选股监控表
    """
    __tablename__ = "watchlist"

    symbol: str = Field(primary_key=True, description="股票代码 (e.g. 00700)")
    market: str = Field(primary_key=True, description="市场 (CN/HK/US)")
    name: str = Field(description="股票名称")
    
    added_at: datetime = Field(default_factory=datetime.now)
    last_alert_at: Optional[datetime] = Field(default=None, description="上次触发异动报警的时间")
    alert_threshold_pct: float = Field(default=5.0, description="涨跌幅触发阈值(%)")
    
    is_active: bool = Field(default=True, description="是否开启监控")