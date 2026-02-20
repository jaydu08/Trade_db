"""
AlphaBase 全局配置
"""
from __future__ import annotations

from pathlib import Path
from typing import Final, Dict

import os

# ============================================================
# Telegram 配置
# ============================================================
TELEGRAM_BOT_TOKEN: Final[str] = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_ID: Final[int] = int(os.getenv("TELEGRAM_ADMIN_ID", "0") or 0)

# ============================================================
# 路径配置
# ============================================================
PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"

# 数据库路径
META_DB_PATH: Final[Path] = DATA_DIR / "meta.db"
LEDGER_DB_PATH: Final[Path] = DATA_DIR / "ledger.db"
VECTOR_STORE_PATH: Final[Path] = DATA_DIR / "vector_store"
CACHE_DIR: Final[Path] = DATA_DIR / "cache"

# ============================================================
# 数据库配置
# ============================================================
META_DB_URL: Final[str] = f"sqlite:///{META_DB_PATH}"
LEDGER_DB_URL: Final[str] = f"sqlite:///{LEDGER_DB_PATH}"

# SQLite 连接参数
SQLITE_CONNECT_ARGS: Final[Dict] = {
    "check_same_thread": False,
    "timeout": 30.0,
}

# ============================================================
# 向量数据库配置 (ChromaDB)
# ============================================================
VECTOR_COLLECTIONS: Final[Dict[str, str]] = {
    "company_chunks": "公司画像库 - 多粒度存储公司信息",
    "industry_knowledge": "产业链知识库 - 产业链结构与传导逻辑",
    "market_events": "市场事件库 - 新闻/公告/政策摘要",
    "entity_relation": "实体关系库 - 知识图谱事实关系",
}

# Embedding 模型配置
EMBEDDING_MODEL: Final[str] = "BAAI/bge-small-zh-v1.5"
EMBEDDING_DEVICE: Final[str] = "cpu"  # CPU-First

# ============================================================
# 缓存配置 (DiskCache)
# ============================================================
CACHE_TTL_SECONDS: Final[int] = 60  # 行情缓存 TTL
CACHE_SIZE_LIMIT: Final[int] = 1024 * 1024 * 500  # 500MB

# ============================================================
# AkShare 配置
# ============================================================
AKSHARE_RETRY_TIMES: Final[int] = 3
AKSHARE_RETRY_DELAY: Final[float] = 1.0  # 秒
AKSHARE_REQUEST_TIMEOUT: Final[int] = 30  # 秒

# ============================================================
# LLM 配置
# ============================================================
LLM_API_BASE: Final[str] = "https://api.openai.com/v1"
LLM_MODEL: Final[str] = "gpt-4o-mini"
LLM_TEMPERATURE: Final[float] = 0.1

# ============================================================
# 市场常量
# ============================================================
MARKETS: Final[Dict[str, str]] = {
    "CN": "中国A股",
    "US": "美股",
    "HK": "港股",
}

ASSET_TYPES: Final[Dict[str, str]] = {
    "STOCK": "股票",
    "ETF": "ETF基金",
    "BOND": "可转债",
    "INDEX": "指数",
}

LISTING_STATUS: Final[Dict[str, str]] = {
    "ACTIVE": "正常交易",
    "SUSPENDED": "停牌",
    "DELISTED": "退市",
}

# ============================================================
# 信号/订单状态
# ============================================================
SIGNAL_STATUS: Final[Dict[str, str]] = {
    "PENDING": "待处理",
    "EXECUTED": "已执行",
    "EXPIRED": "已过期",
    "CANCELLED": "已取消",
}

ORDER_STATUS: Final[Dict[str, str]] = {
    "PENDING": "待提交",
    "SUBMITTED": "已提交",
    "PARTIAL": "部分成交",
    "FILLED": "全部成交",
    "CANCELLED": "已取消",
    "REJECTED": "已拒绝",
}

DIRECTION: Final[Dict[str, str]] = {
    "LONG": "做多",
    "SHORT": "做空",
    "CLOSE": "平仓",
}

SIDE: Final[Dict[str, str]] = {
    "BUY": "买入",
    "SELL": "卖出",
}
