# -*- coding: utf-8 -*-
"""
LogObserver – persists structured log events to the SQLite node_logs table.

Filter chain architecture:
    LogFilter (abstract)
    ├── LevelFilter
    ├── NodeFilter
    ├── WorkflowFilter
    ├── TimeRangeFilter
    └── RegexFilter

Usage
-----
    from pangflow.observer.log_observer import LogObserver, LogFilterChain, LevelFilter
    chain = LogFilterChain([LevelFilter("INFO")])
    observer = LogObserver(filter_chain=chain)
    observer.start()
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from pangflow.database.connection import get_db_manager
from pangflow.database.models import NodeLogModel
from pangflow.observer.subject import get_subject

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Filters
# --------------------------------------------------------------------------- #

class LogFilter(ABC):
    """Abstract base for a single log-record filter."""

    @abstractmethod
    def accept(self, payload: Dict[str, Any]) -> bool:
        """Return True if the payload passes the filter."""
        ...


class LevelFilter(LogFilter):
    """Filter by minimum log level."""

    _ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

    def __init__(self, min_level: str = "DEBUG"):
        self.min_level = self._ORDER.get(min_level.upper(), 0)

    def accept(self, payload: Dict[str, Any]) -> bool:
        level = payload.get("level", "INFO").upper()
        return self._ORDER.get(level, 0) >= self.min_level


class NodeFilter(LogFilter):
    """Filter by node_id allow-list."""

    def __init__(self, node_ids: Optional[List[str]] = None):
        self.node_ids = set(node_ids or [])

    def accept(self, payload: Dict[str, Any]) -> bool:
        if not self.node_ids:
            return True
        return payload.get("node_id") in self.node_ids


class WorkflowFilter(LogFilter):
    """Filter by workflow_id allow-list."""

    def __init__(self, workflow_ids: Optional[List[str]] = None):
        self.workflow_ids = set(workflow_ids or [])

    def accept(self, payload: Dict[str, Any]) -> bool:
        if not self.workflow_ids:
            return True
        return payload.get("workflow_id") in self.workflow_ids


class TimeRangeFilter(LogFilter):
    """Filter by timestamp range."""

    def __init__(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ):
        self.start = start
        self.end = end

    def accept(self, payload: Dict[str, Any]) -> bool:
        ts_str = payload.get("timestamp")
        if not ts_str:
            return True
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            return True
        if self.start is not None and ts < self.start:
            return False
        if self.end is not None and ts > self.end:
            return False
        return True


class RegexFilter(LogFilter):
    """Filter by regex match on the log message."""

    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern)

    def accept(self, payload: Dict[str, Any]) -> bool:
        message = payload.get("message", "")
        return bool(self.pattern.search(message))


# --------------------------------------------------------------------------- #
# Filter chain
# --------------------------------------------------------------------------- #

class LogFilterChain:
    """Chains multiple LogFilters with AND semantics."""

    def __init__(self, filters: Optional[List[LogFilter]] = None):
        self.filters: List[LogFilter] = filters or []

    def add(self, filter_obj: LogFilter) -> None:
        self.filters.append(filter_obj)

    def accept(self, payload: Dict[str, Any]) -> bool:
        return all(f.accept(payload) for f in self.filters)


# --------------------------------------------------------------------------- #
# Observer
# --------------------------------------------------------------------------- #

class LogObserver:
    """Subscriber that writes LOG_RECORD events to the node_logs table."""

    def __init__(self, filter_chain: Optional[LogFilterChain] = None):
        self.filter_chain = filter_chain or LogFilterChain()

    # -- callbacks ---------------------------------------------------------- #

    def on_log(self, payload: Dict[str, Any]) -> None:
        if not self.filter_chain.accept(payload):
            return
        try:
            db = get_db_manager()
            with db.get_session() as session:
                ts_str = payload.get("timestamp")
                ts = datetime.fromisoformat(ts_str) if ts_str else datetime.now()
                log = NodeLogModel(
                    timestamp=ts,
                    workflow_id=payload.get("workflow_id"),
                    workflow_name=payload.get("workflow_name"),
                    node_id=payload.get("node_id"),
                    node_name=payload.get("node_name"),
                    log_type=payload.get("log_type", "auto"),
                    level=payload.get("level"),
                    message=payload.get("message"),
                    extra_json=json.dumps(payload.get("extra")) if payload.get("extra") else None,
                    trace_id=payload.get("trace_id"),
                )
                session.add(log)
        except Exception:
            logger.exception("LogObserver failed to persist log record")

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        get_subject().attach("LOG_RECORD", self.on_log)

    def stop(self) -> None:
        get_subject().detach("LOG_RECORD", self.on_log)
