"""
Domain Base Models - Mixin 定义

遵循设计规范:
1. 所有表必须包含 created_at, updated_at, version 字段
2. 高频变更字段采用 JSON 类型 (extra)
3. 预留 typed 字段支持动态扩展
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, Optional

from sqlmodel import SQLModel, Field
from sqlalchemy import Column, JSON


# ============================================================
# 审计字段 Mixin
# ============================================================
class TimestampMixin:
    """时间戳 Mixin - 所有表必须包含"""
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        description="创建时间"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        description="更新时间",
        sa_column_kwargs={"onupdate": datetime.utcnow}
    )


class VersionMixin:
    """版本控制 Mixin - 乐观锁"""
    version: int = Field(
        default=1,
        nullable=False,
        description="版本号(乐观锁)"
    )


class AuditMixin(TimestampMixin, VersionMixin):
    """审计 Mixin - 组合时间戳和版本控制"""
    pass


# ============================================================
# JSON 扩展 Mixin
# ============================================================
class ExtraFieldMixin:
    """JSON 扩展字段 Mixin"""
    extra: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_type=JSON,
        description="JSON扩展字段-存储非标准属性"
    )


# ============================================================
# 预留字段 Mixin (完整版 - 用于核心业务表)
# ============================================================
class FullReservedFieldsMixin:
    """完整预留字段 Mixin - 用于 Asset, Signal 等核心表"""
    
    # 字符串类型预留 (5个)
    str_1: str | None = Field(default=None, max_length=255, description="预留字符串1")
    str_2: str | None = Field(default=None, max_length=255, description="预留字符串2")
    str_3: str | None = Field(default=None, max_length=255, description="预留字符串3")
    str_4: str | None = Field(default=None, max_length=500, description="预留字符串4(长文本)")
    str_5: str | None = Field(default=None, max_length=500, description="预留字符串5(长文本)")
    
    # 整数类型预留 (3个)
    int_1: int | None = Field(default=None, description="预留整数1")
    int_2: int | None = Field(default=None, description="预留整数2")
    int_3: int | None = Field(default=None, description="预留整数3")
    
    # 浮点类型预留 (3个)
    float_1: float | None = Field(default=None, description="预留浮点1")
    float_2: float | None = Field(default=None, description="预留浮点2")
    float_3: float | None = Field(default=None, description="预留浮点3")
    
    # 布尔类型预留 (2个)
    bool_1: bool | None = Field(default=None, description="预留布尔1")
    bool_2: bool | None = Field(default=None, description="预留布尔2")
    
    # 日期类型预留 (2个)
    date_1: date | None = Field(default=None, description="预留日期1")
    date_2: date | None = Field(default=None, description="预留日期2")


# ============================================================
# 预留字段 Mixin (简化版 - 用于关联表)
# ============================================================
class LiteReservedFieldsMixin:
    """简化预留字段 Mixin - 用于关联表、扩展表"""
    
    str_1: str | None = Field(default=None, max_length=255, description="预留字符串1")
    str_2: str | None = Field(default=None, max_length=255, description="预留字符串2")
    int_1: int | None = Field(default=None, description="预留整数1")
    float_1: float | None = Field(default=None, description="预留浮点1")


# ============================================================
# 完整基类 (组合常用 Mixin)
# ============================================================
class BaseModel(SQLModel):
    """基础模型 - 不带表定义"""
    pass


class CoreTableMixin(AuditMixin, FullReservedFieldsMixin, ExtraFieldMixin):
    """核心业务表 Mixin - 包含完整预留字段"""
    pass


class LinkTableMixin(AuditMixin, ExtraFieldMixin):
    """关联表 Mixin - 不包含预留字段"""
    pass


class ExtTableMixin(AuditMixin):
    """扩展表 Mixin - 最小化"""
    pass


# ============================================================
# 扩展表通用字段定义
# ============================================================
class ExtFieldValueMixin:
    """扩展表值字段 Mixin - 用于 xxx_ext 表"""
    field_name: str = Field(nullable=False, max_length=100, description="字段名")
    field_type: str = Field(nullable=False, max_length=20, description="字段类型: STR/INT/FLOAT/BOOL/DATE")
    value_str: str | None = Field(default=None, max_length=1000, description="字符串值")
    value_int: int | None = Field(default=None, description="整数值")
    value_float: float | None = Field(default=None, description="浮点值")
    value_bool: bool | None = Field(default=None, description="布尔值")
    value_date: date | None = Field(default=None, description="日期值")
    
    def get_value(self) -> Any:
        """根据 field_type 获取对应的值"""
        type_map = {
            "STR": self.value_str,
            "INT": self.value_int,
            "FLOAT": self.value_float,
            "BOOL": self.value_bool,
            "DATE": self.value_date,
        }
        return type_map.get(self.field_type)
    
    def set_value(self, value: Any) -> None:
        """根据 field_type 设置对应的值"""
        if self.field_type == "STR":
            self.value_str = str(value) if value is not None else None
        elif self.field_type == "INT":
            self.value_int = int(value) if value is not None else None
        elif self.field_type == "FLOAT":
            self.value_float = float(value) if value is not None else None
        elif self.field_type == "BOOL":
            self.value_bool = bool(value) if value is not None else None
        elif self.field_type == "DATE":
            self.value_date = value
