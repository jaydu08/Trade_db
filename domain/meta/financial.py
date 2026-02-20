
from datetime import date
from typing import Optional
from sqlmodel import SQLModel, Field

class AssetFinancial(SQLModel, table=True):
    """
    资产财务数据 (基本面指标)
    """
    __tablename__ = "asset_financial"

    symbol: str = Field(primary_key=True, description="股票代码")
    report_date: date = Field(description="报告期")
    
    # 市值数据 (实时/最近)
    market_cap: Optional[float] = Field(default=None, description="总市值")
    pe_ttm: Optional[float] = Field(default=None, description="市盈率TTM")
    pb: Optional[float] = Field(default=None, description="市净率")
    ps_ttm: Optional[float] = Field(default=None, description="市销率TTM")
    dv_ratio: Optional[float] = Field(default=None, description="股息率")
    
    # 核心财务指标 (最近财报)
    total_revenue: Optional[float] = Field(default=None, description="营业总收入")
    net_profit: Optional[float] = Field(default=None, description="净利润")
    gross_profit_margin: Optional[float] = Field(default=None, description="毛利率(%)")
    net_profit_margin: Optional[float] = Field(default=None, description="净利率(%)")
    roe: Optional[float] = Field(default=None, description="净资产收益率(%)")
    
    # 成长性
    revenue_yoy: Optional[float] = Field(default=None, description="营收同比增长(%)")
    net_profit_yoy: Optional[float] = Field(default=None, description="净利润同比增长(%)")
    
    updated_at: str = Field(description="更新时间")
