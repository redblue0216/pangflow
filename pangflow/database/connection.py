# -*- coding: utf-8 -*-
"""
Database connection and session management.
"""

import logging
from pathlib import Path
from typing import Generator, Optional

from contextlib import contextmanager
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

from pangflow.database.models import Base

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages the SQLite engine and sessions."""

    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
            # Default to ~/.pangflow/pangflow.db
            home = Path.home()
            db_dir = home / ".pangflow"
            db_dir.mkdir(parents=True, exist_ok=True)
            database_url = f"sqlite:///{db_dir / 'pangflow.db'}"
        self.database_url = database_url
        self._engine = create_engine(
            database_url,
            echo=False,
            connect_args={"check_same_thread": False} if database_url.startswith("sqlite") else {},
        )
        self._session_factory = sessionmaker(bind=self._engine)
        self._setup_pragmas()

    def _setup_pragmas(self) -> None:
        """Enable WAL mode for SQLite to improve concurrency."""
        if self.database_url.startswith("sqlite"):
            @event.listens_for(self._engine, "connect")
            def _set_sqlite_pragma(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.close()

    def create_tables(self) -> None:
        Base.metadata.create_all(self._engine)
        self._migrate_columns()
        logger.debug("Database tables created/verified.")

    def _migrate_columns(self) -> None:
        """Auto-migrate: add missing columns to existing SQLite tables."""
        if not self.database_url.startswith("sqlite"):
            return
        from sqlalchemy import inspect, text

        inspector = inspect(self._engine)
        with self._engine.connect() as conn:
            for table_name, table in Base.metadata.tables.items():
                if not inspector.has_table(table_name):
                    continue
                existing_cols = {c["name"] for c in inspector.get_columns(table_name)}
                for col in table.columns:
                    if col.name not in existing_cols:
                        col_type = col.type.compile(dialect=self._engine.dialect)
                        nullable = "NULL" if col.nullable else "NOT NULL"
                        default = ""
                        if col.default is not None and hasattr(col.default, "arg"):
                            default_val = col.default.arg
                            if isinstance(default_val, str):
                                default = f" DEFAULT '{default_val}'"
                            else:
                                default = f" DEFAULT {default_val}"
                        sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col.name}" {col_type} {nullable}{default}'
                        try:
                            conn.execute(text(sql))
                            conn.commit()
                            logger.info(f"Added missing column '{col.name}' to table '{table_name}'")
                        except Exception as exc:
                            logger.warning(f"Failed to add column '{col.name}' to '{table_name}': {exc}")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @property
    def engine(self):
        return self._engine


# Singleton instance per process
_db_manager: Optional[DatabaseManager] = None


def initialize_database(database_url: Optional[str] = None) -> DatabaseManager:
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(database_url)
        _db_manager.create_tables()
    elif database_url is not None and _db_manager.database_url != database_url:
        _db_manager = DatabaseManager(database_url)
        _db_manager.create_tables()
    return _db_manager


def get_db_manager() -> DatabaseManager:
    if _db_manager is None:
        raise RuntimeError("Database not initialized. Call initialize_database() first.")
    return _db_manager
