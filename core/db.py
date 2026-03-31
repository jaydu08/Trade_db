"""
Database 数据库连接管理 - 单例模式

管理三种数据库连接:
1. MetaDB - SQLite 元数据库
2. LedgerDB - SQLite 交易账本库
3. VectorDB - ChromaDB 向量数据库
"""
import logging
from pathlib import Path
from typing import Generator, Optional
from contextlib import contextmanager

from sqlmodel import SQLModel, Session, create_engine
from sqlalchemy import Engine, text
import chromadb
from chromadb.config import Settings as ChromaSettings

from config.settings import (
    META_DB_URL,
    LEDGER_DB_URL,
    SQLITE_CONNECT_ARGS,
    VECTOR_STORE_PATH,
    VECTOR_COLLECTIONS,
    DATA_DIR,
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    数据库管理器 - 单例模式

    管理 SQLite 和 ChromaDB 的连接
    """

    _instance: Optional["DatabaseManager"] = None
    _initialized: bool = False

    def __new__(cls) -> "DatabaseManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        VECTOR_STORE_PATH.mkdir(parents=True, exist_ok=True)

        self._meta_engine: Optional[Engine] = None
        self._ledger_engine: Optional[Engine] = None
        self._chroma_client: Optional[chromadb.PersistentClient] = None

        self._initialized = True
        logger.info("DatabaseManager initialized")

    # ================================================================
    # SQLite - Meta Database
    # ================================================================
    @property
    def meta_engine(self) -> Engine:
        """获取 Meta 数据库引擎"""
        if self._meta_engine is None:
            self._meta_engine = create_engine(
                META_DB_URL,
                connect_args=SQLITE_CONNECT_ARGS,
                echo=False,
            )
            logger.info(f"Meta engine created: {META_DB_URL}")
        return self._meta_engine

    @contextmanager
    def meta_session(self) -> Generator[Session, None, None]:
        """获取 Meta 数据库会话"""
        session = Session(self.meta_engine, expire_on_commit=False)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def init_meta_db(self) -> None:
        """初始化 Meta 数据库表"""
        from domain.meta import (
            Asset,
            AssetExt,
            Concept,
            AssetConceptLink,
            Industry,
            AssetIndustryLink,
            AssetProfile,
            FieldMapping,
            DataSyncLog,
            PeerGroup,
            PeerGroupMember,
            AssetFinancial,
        )
        from domain.monitor import Watchlist

        SQLModel.metadata.create_all(self.meta_engine)
        logger.info("Meta database tables created")

    # ================================================================
    # SQLite - Ledger Database
    # ================================================================
    @property
    def ledger_engine(self) -> Engine:
        """获取 Ledger 数据库引擎"""
        if self._ledger_engine is None:
            self._ledger_engine = create_engine(
                LEDGER_DB_URL,
                connect_args=SQLITE_CONNECT_ARGS,
                echo=False,
            )
            logger.info(f"Ledger engine created: {LEDGER_DB_URL}")
        return self._ledger_engine

    @contextmanager
    def ledger_session(self) -> Generator[Session, None, None]:
        """获取 Ledger 数据库会话"""
        session = Session(self.ledger_engine, expire_on_commit=False)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _get_table_columns(self, table: str) -> set[str]:
        with self.ledger_engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(r[1]) for r in rows}

    def _ensure_column(self, table: str, col_name: str, col_def: str) -> bool:
        cols = self._get_table_columns(table)
        if col_name in cols:
            return False
        ddl = f"ALTER TABLE {table} ADD COLUMN {col_def}"
        with self.ledger_engine.begin() as conn:
            conn.execute(text(ddl))
        logger.info("Ledger schema migrated: %s.%s added", table, col_name)
        return True

    def _migrate_ledger_schema(self) -> None:
        """对已有 SQLite 账本做最小增量迁移，避免线上重建表。"""
        try:
            cols = self._get_table_columns("papertrade")
            if not cols:
                return

            self._ensure_column("papertrade", "review_status", "review_status TEXT DEFAULT 'PENDING'")
            self._ensure_column("papertrade", "review_attempts", "review_attempts INTEGER DEFAULT 0")
            self._ensure_column("papertrade", "review_error", "review_error TEXT")
            self._ensure_column("papertrade", "review_source", "review_source TEXT")
            self._ensure_column("papertrade", "last_reviewed_at", "last_reviewed_at DATETIME")

            # 历史数据兜底修复
            with self.ledger_engine.begin() as conn:
                conn.execute(text("UPDATE papertrade SET review_status = 'PENDING' WHERE review_status IS NULL OR trim(review_status) = ''"))
                conn.execute(
                    text(
                        "UPDATE papertrade "
                        "SET review_status='DONE', last_reviewed_at=COALESCE(last_reviewed_at, updated_at) "
                        "WHERE review_text IS NOT NULL AND length(trim(review_text)) > 0"
                    )
                )
        except Exception as e:
            logger.warning("Ledger schema migration skipped due to error: %s", e)

    def init_ledger_db(self) -> None:
        """初始化 Ledger 数据库表"""
        from domain.ledger import (
            Strategy,
            StrategyRun,
            Signal,
            SignalExt,
            Order,
            Position,
            DailyRank,
            WatchlistAlert,
            TrendSeedPool,
            TrendDailyBar,
            PaperTrade,
        )

        ledger_tables = [
            Strategy.__table__,
            StrategyRun.__table__,
            Signal.__table__,
            SignalExt.__table__,
            Order.__table__,
            Position.__table__,
            DailyRank.__table__,
            WatchlistAlert.__table__,
            TrendSeedPool.__table__,
            TrendDailyBar.__table__,
            PaperTrade.__table__,
        ]
        for table in ledger_tables:
            table.create(self.ledger_engine, checkfirst=True)

        self._migrate_ledger_schema()
        logger.info("Ledger database tables created")

    # ================================================================
    # ChromaDB - Vector Database
    # ================================================================
    @property
    def chroma_client(self) -> chromadb.PersistentClient:
        """获取 ChromaDB 客户端"""
        if self._chroma_client is None:
            import os

            os.environ["ANONYMIZED_TELEMETRY"] = "False"

            self._chroma_client = chromadb.PersistentClient(
                path=str(VECTOR_STORE_PATH),
                settings=ChromaSettings(
                    anonymized_telemetry=False,
                    allow_reset=True,
                ),
            )
            logger.info(f"ChromaDB client created: {VECTOR_STORE_PATH}")
        return self._chroma_client

    def get_collection(self, name: str) -> chromadb.Collection:
        """获取或创建 ChromaDB Collection"""
        if name not in VECTOR_COLLECTIONS:
            raise ValueError(f"Unknown collection: {name}. Valid: {list(VECTOR_COLLECTIONS.keys())}")

        return self.chroma_client.get_or_create_collection(
            name=name,
            metadata={"description": VECTOR_COLLECTIONS[name]},
        )

    def init_vector_db(self) -> None:
        """初始化所有向量库 Collections"""
        for name, desc in VECTOR_COLLECTIONS.items():
            self.chroma_client.get_or_create_collection(
                name=name,
                metadata={"description": desc},
            )
            logger.info(f"Vector collection created: {name}")

    # ================================================================
    # 初始化所有数据库
    # ================================================================
    def init_all(self) -> None:
        """初始化所有数据库"""
        self.init_meta_db()
        self.init_ledger_db()
        self.init_vector_db()
        logger.info("All databases initialized")

    def close(self) -> None:
        """关闭所有连接"""
        if self._meta_engine:
            self._meta_engine.dispose()
            self._meta_engine = None
        if self._ledger_engine:
            self._ledger_engine.dispose()
            self._ledger_engine = None
        self._chroma_client = None
        logger.info("All database connections closed")


# 全局单例
db_manager = DatabaseManager()


# 便捷函数
def get_meta_session() -> Generator[Session, None, None]:
    """获取 Meta 数据库会话的便捷函数"""
    return db_manager.meta_session()


def get_ledger_session() -> Generator[Session, None, None]:
    """获取 Ledger 数据库会话的便捷函数"""
    return db_manager.ledger_session()


def get_collection(name: str) -> chromadb.Collection:
    """获取向量库 Collection 的便捷函数"""
    return db_manager.get_collection(name)
