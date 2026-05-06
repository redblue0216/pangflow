# -*- coding: utf-8 -*-
"""
LogStore – structured log persistence backed by the ORM node_logs table.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from pangflow.database.connection import get_db_manager
from pangflow.database.models import NodeLogModel
from pangflow.storage.backend import StorageBackend


class LogStore:
    """Writes log records to the ORM and optionally to a raw backend."""

    def __init__(self, backend: StorageBackend):
        self.backend = backend

    def write(self, record: Dict[str, Any]) -> None:
        """Persist a log record."""
        db = get_db_manager()
        with db.get_session() as session:
            log = NodeLogModel(
                timestamp=record.get("timestamp", datetime.now()),
                workflow_id=record.get("workflow_id"),
                workflow_name=record.get("workflow_name"),
                node_id=record.get("node_id"),
                node_name=record.get("node_name"),
                log_type=record.get("log_type", "auto"),
                level=record.get("level", "INFO"),
                message=record.get("message"),
                extra_json=json.dumps(record.get("extra", {}), ensure_ascii=False),
                inputs_hash=record.get("inputs_hash"),
                outputs_hash=record.get("outputs_hash"),
                duration_ms=record.get("duration_ms"),
                exception=record.get("exception"),
                metric_name=record.get("metric_name"),
                metric_value=record.get("metric_value"),
                trace_id=record.get("trace_id"),
                storage_backend=record.get("storage_backend", "sqlite"),
            )
            session.add(log)

        # Also archive the raw record on the backend
        key = f"logs/{record.get('trace_id', 'unknown')}/{datetime.now().isoformat()}"
        self.backend.write(key, json.dumps(record, ensure_ascii=False, default=str).encode("utf-8"), record)

    def query(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Query node logs with optional filters."""
        db = get_db_manager()
        with db.get_session() as session:
            query = session.query(NodeLogModel)
            if filters:
                if "workflow_id" in filters:
                    query = query.filter_by(workflow_id=filters["workflow_id"])
                if "node_id" in filters:
                    query = query.filter_by(node_id=filters["node_id"])
                if "level" in filters:
                    query = query.filter_by(level=filters["level"])
                if "log_type" in filters:
                    query = query.filter_by(log_type=filters["log_type"])
                if "since" in filters:
                    query = query.filter(NodeLogModel.timestamp >= filters["since"])
                if "until" in filters:
                    query = query.filter(NodeLogModel.timestamp <= filters["until"])
                if "trace_id" in filters:
                    query = query.filter_by(trace_id=filters["trace_id"])
            limit = filters.get("limit", 1000) if filters else 1000
            logs = query.order_by(NodeLogModel.timestamp.desc()).limit(limit).all()

            return [
                {
                    "id": log.id,
                    "timestamp": log.timestamp.isoformat() if log.timestamp else None,
                    "workflow_id": log.workflow_id,
                    "workflow_name": log.workflow_name,
                    "node_id": log.node_id,
                    "node_name": log.node_name,
                    "log_type": log.log_type,
                    "level": log.level,
                    "message": log.message,
                    "extra": json.loads(log.extra_json) if log.extra_json else {},
                    "inputs_hash": log.inputs_hash,
                    "outputs_hash": log.outputs_hash,
                    "duration_ms": log.duration_ms,
                    "exception": log.exception,
                    "metric_name": log.metric_name,
                    "metric_value": log.metric_value,
                    "trace_id": log.trace_id,
                    "storage_backend": log.storage_backend,
                }
                for log in logs
            ]
