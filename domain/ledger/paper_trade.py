"""
PaperTrade 模拟持仓表
记录用户手动添加的模拟交易标的，支持无限期持仓跟踪与 AI 复盘分析。
"""
import datetime as dt
from typing import Optional
from sqlmodel import Field, SQLModel


class PaperTrade(SQLModel, table=True):
    """
    模拟交易持仓记录
    
    用于记录用户在分析日报后主动添加的模拟交易标的，持续跟踪浮动盈亏，
    支持设置目标持仓天数（自动到期平仓）或无期限跟踪（手动平仓），
    平仓后自动触发 AI 六维度回测复盘研报。
    """
    __tablename__ = "papertrade"

    id: Optional[int] = Field(default=None, primary_key=True)

    # ── 标的信息 ───────────────────────────────────────────────────────
    symbol: str = Field(index=True, max_length=20, description="标的代码")
    name: str = Field(default="", max_length=50, description="标的名称")
    market: str = Field(index=True, max_length=5, description="市场 (CN, HK, US, CF)")

    # ── 建仓信息 ───────────────────────────────────────────────────────
    entry_date: dt.date = Field(index=True, description="建仓日期")
    entry_price: float = Field(description="建仓价格")
    entry_reason: str = Field(default="", description="建仓逻辑/假设 (自由文本)")

    # ── 目标持仓天数 (选填，None 表示无限期直至手动平仓) ──────────────
    target_days: Optional[int] = Field(default=None, description="目标持仓天数，None 表示不设置")

    # ── 状态 ──────────────────────────────────────────────────────────
    status: str = Field(default="ACTIVE", index=True, description="持仓状态: ACTIVE / CLOSED")

    # ── 平仓信息 (CLOSED 后填入) ──────────────────────────────────────
    exit_date: Optional[dt.date] = Field(default=None, description="平仓日期")
    exit_price: Optional[float] = Field(default=None, description="平仓价格")
    pnl_pct: Optional[float] = Field(default=None, description="盈亏百分比 (exit-entry)/entry*100")

    # ── 复盘研报 (平仓后 AI 生成) ─────────────────────────────────────
    review_text: Optional[str] = Field(default=None, description="AI 回测复盘研报全文")

    # ── 元数据 ────────────────────────────────────────────────────────
    chat_id: Optional[int] = Field(default=None, description="触发建仓的 Telegram chat_id")
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, description="记录创建时间")
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, description="最后更新时间")
