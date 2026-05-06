# -*- coding: utf-8 -*-
"""
MetricObserver – captures METRIC_RECORD events.

Stores metrics in an in-memory list.  Optionally mirrors them to the SQLite
node_logs table (log_type='metric') when *store_in_db=True*.

Usage
-----
    from pangflow.observer.metric_observer import MetricObserver
    obs = MetricObserver(store_in_db=True)
    obs.start()
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from pangflow.database.connection import get_db_manager
from pangflow.database.models import NodeLogModel
from pangflow.observer.subject import get_subject

logger = logging.getLogger(__name__)


class MetricObserver:
    """Subscriber that captures metrics from METRIC_RECORD and NODE_COMPLETE."""

    def __init__(self, store_in_db: bool = False):
        self._metrics: List[Dict[str, Any]] = []
        self.store_in_db = store_in_db

    # -- callbacks ---------------------------------------------------------- #

    def on_metric(self, payload: Dict[str, Any]) -> None:
        record = {
            "metric_name": payload.get("metric_name"),
            "metric_value": payload.get("metric_value"),
            "tags": payload.get("tags", {}),
            "workflow_id": payload.get("workflow_id"),
            "node_id": payload.get("node_id"),
            "run_id": payload.get("run_id"),
            "timestamp": payload.get("timestamp", datetime.now().isoformat()),
        }
        self._metrics.append(record)

        if self.store_in_db:
            self._persist(record)

    def on_node_complete(self, payload: Dict[str, Any]) -> None:
        """Auto-capture duration_ms as a metric when present."""
        duration = payload.get("duration_ms")
        if duration is None:
            return
        self.on_metric(
            {
                "metric_name": "node_duration_ms",
                "metric_value": float(duration),
                "tags": {"node_name": payload.get("node_name"), "event": "NODE_COMPLETE"},
                "workflow_id": payload.get("workflow_id"),
                "node_id": payload.get("node_id"),
                "run_id": payload.get("run_id"),
                "timestamp": payload.get("timestamp", datetime.now().isoformat()),
            }
        )

    # -- persistence -------------------------------------------------------- #

    def _persist(self, record: Dict[str, Any]) -> None:
        try:
            db = get_db_manager()
            with db.get_session() as session:
                ts_str = record.get("timestamp")
                ts = datetime.fromisoformat(ts_str) if ts_str else datetime.now()
                log = NodeLogModel(
                    timestamp=ts,
                    workflow_id=record.get("workflow_id"),
                    node_id=record.get("node_id"),
                    log_type="metric",
                    level="INFO",
                    metric_name=record.get("metric_name"),
                    metric_value=record.get("metric_value"),
                    extra_json=json.dumps(record["tags"]) if record.get("tags") else None,
                    trace_id=record.get("run_id"),
                )
                session.add(log)
        except Exception:
            logger.exception("MetricObserver failed to persist metric record")

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        get_subject().attach("METRIC_RECORD", self.on_metric)
        get_subject().attach("NODE_COMPLETE", self.on_node_complete)

    def stop(self) -> None:
        get_subject().detach("METRIC_RECORD", self.on_metric)
        get_subject().detach("NODE_COMPLETE", self.on_node_complete)

    # -- query -------------------------------------------------------------- #

    def get_metrics(self) -> List[Dict[str, Any]]:
        """Return a shallow copy of the in-memory metric list."""
        return list(self._metrics)
