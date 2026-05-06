# -*- coding: utf-8 -*-
"""
Storage backends – abstract base + concrete implementations.
"""

import abc
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


class StorageBackend(abc.ABC):
    """Abstract base class for all storage backends."""

    @abc.abstractmethod
    def write(self, key: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Persist *data* under *key*. Returns the resolved key."""

    @abc.abstractmethod
    def read(self, key: str) -> bytes:
        """Read bytes previously stored under *key*."""

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if *key* exists."""

    @abc.abstractmethod
    def delete(self, key: str) -> bool:
        """Delete *key*. Returns True if something was deleted."""

    @abc.abstractmethod
    def list(self, prefix: str = "") -> List[Dict[str, Any]]:
        """List entries whose key starts with *prefix*."""

    def connect(self) -> None:
        """Optional lifecycle hook."""

    def close(self) -> None:
        """Optional lifecycle hook."""


class LocalFileBackend(StorageBackend):
    """Stores objects as files under a base directory."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path).expanduser().resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        path = (self.base_path / key).resolve()
        # Simple directory-traversal guard
        if not str(path).startswith(str(self.base_path)):
            raise ValueError(f"Invalid key '{key}'")
        return path

    def write(self, key: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        if metadata:
            meta_path = path.with_suffix(path.suffix + ".meta.json")
            meta_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
        return str(path)

    def read(self, key: str) -> bytes:
        return self._resolve(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def delete(self, key: str) -> bool:
        path = self._resolve(key)
        if not path.exists():
            return False
        path.unlink()
        meta_path = path.with_suffix(path.suffix + ".meta.json")
        if meta_path.exists():
            meta_path.unlink()
        return True

    def list(self, prefix: str = "") -> List[Dict[str, Any]]:
        search_dir = self.base_path / prefix
        if not search_dir.exists():
            return []
        results: List[Dict[str, Any]] = []
        for p in search_dir.rglob("*"):
            if p.is_file() and not p.name.endswith(".meta.json"):
                rel = str(p.relative_to(self.base_path))
                meta_path = p.with_suffix(p.suffix + ".meta.json")
                meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
                results.append({
                    "key": rel,
                    "path": str(p),
                    "size": p.stat().st_size,
                    "metadata": meta,
                })
        return results


class SQLiteBackend(StorageBackend):
    """Stores blobs in a dedicated SQLite table."""

    def __init__(self, db_path: Optional[str] = None, table_name: str = "storage_blobs"):
        if db_path is None:
            db_dir = Path.home() / ".pangflow"
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(db_dir / "storage_blobs.db")
        self.db_path = db_path
        self.table_name = table_name
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._ensure_table()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _ensure_table(self) -> None:
        if self._conn is None:
            raise RuntimeError("SQLiteBackend not connected")
        self._conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                key TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                metadata_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        self._conn.commit()

    def write(self, key: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        if self._conn is None:
            self.connect()
        meta_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
        self._conn.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (key, data, metadata_json) VALUES (?, ?, ?)",
            (key, data, meta_json),
        )
        self._conn.commit()
        return key

    def read(self, key: str) -> bytes:
        if self._conn is None:
            self.connect()
        row = self._conn.execute(
            f"SELECT data FROM {self.table_name} WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]

    def exists(self, key: str) -> bool:
        if self._conn is None:
            self.connect()
        row = self._conn.execute(
            f"SELECT 1 FROM {self.table_name} WHERE key = ?", (key,)
        ).fetchone()
        return row is not None

    def delete(self, key: str) -> bool:
        if self._conn is None:
            self.connect()
        cur = self._conn.execute(
            f"DELETE FROM {self.table_name} WHERE key = ?", (key,)
        )
        self._conn.commit()
        return cur.rowcount > 0

    def list(self, prefix: str = "") -> List[Dict[str, Any]]:
        if self._conn is None:
            self.connect()
        rows = self._conn.execute(
            f"SELECT key, metadata_json, created_at FROM {self.table_name} WHERE key LIKE ?",
            (prefix + "%",),
        ).fetchall()
        return [
            {
                "key": key,
                "metadata": json.loads(meta_json) if meta_json else {},
                "created_at": created_at,
            }
            for key, meta_json, created_at in rows
        ]


class S3Backend(StorageBackend):
    """Stub for AWS S3 storage."""

    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.region = region

    def write(self, key: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        raise NotImplementedError("S3Backend is not implemented yet")

    def read(self, key: str) -> bytes:
        raise NotImplementedError("S3Backend is not implemented yet")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("S3Backend is not implemented yet")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("S3Backend is not implemented yet")

    def list(self, prefix: str = "") -> List[Dict[str, Any]]:
        raise NotImplementedError("S3Backend is not implemented yet")


class PostgreSQLBackend(StorageBackend):
    """Stub for PostgreSQL large-object storage."""

    def __init__(self, host: str = "localhost", port: int = 5432, database: str = "pangflow"):
        self.host = host
        self.port = port
        self.database = database

    def write(self, key: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        raise NotImplementedError("PostgreSQLBackend is not implemented yet")

    def read(self, key: str) -> bytes:
        raise NotImplementedError("PostgreSQLBackend is not implemented yet")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("PostgreSQLBackend is not implemented yet")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("PostgreSQLBackend is not implemented yet")

    def list(self, prefix: str = "") -> List[Dict[str, Any]]:
        raise NotImplementedError("PostgreSQLBackend is not implemented yet")


class MongoBackend(StorageBackend):
    """Stub for MongoDB GridFS storage."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        database: str = "pangflow",
        collection: str = "blobs",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection

    def write(self, key: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        raise NotImplementedError("MongoBackend is not implemented yet")

    def read(self, key: str) -> bytes:
        raise NotImplementedError("MongoBackend is not implemented yet")

    def exists(self, key: str) -> bool:
        raise NotImplementedError("MongoBackend is not implemented yet")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("MongoBackend is not implemented yet")

    def list(self, prefix: str = "") -> List[Dict[str, Any]]:
        raise NotImplementedError("MongoBackend is not implemented yet")
