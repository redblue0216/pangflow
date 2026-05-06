# -*- coding: utf-8 -*-
"""
Task Layer – Base task and execution context.
"""

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Callable


@dataclass
class ExecutionContext:
    """Runtime context passed through the execution layer."""
    workflow_id: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    node_id: Optional[str] = None
    node_name: Optional[str] = None
    env: Optional[Any] = None  # CondaEnv or None
    runtime_params: Dict[str, Any] = field(default_factory=dict)
    log_context: Optional[Any] = None
    artifact_context: Optional[Any] = None
    trace_id: Optional[str] = None

    def get_param(self, key: str, default: Any = None) -> Any:
        return self.runtime_params.get(key, default)

    def log(self, level: str, message: str, **extra) -> None:
        from pangflow.observer.subject import get_subject
        get_subject().publish(
            "LOG_RECORD",
            {
                "level": level,
                "message": message,
                "extra": extra,
                "workflow_id": self.workflow_id,
                "node_id": self.node_id,
                "run_id": self.run_id,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def log_metric(self, name: str, value: float, **tags) -> None:
        from pangflow.observer.subject import get_subject
        get_subject().publish(
            "METRIC_RECORD",
            {
                "metric_name": name,
                "metric_value": value,
                "tags": tags,
                "workflow_id": self.workflow_id,
                "node_id": self.node_id,
                "run_id": self.run_id,
                "timestamp": datetime.now().isoformat(),
            },
        )


@dataclass
class Result:
    status: str  # "success", "failed", "cancelled"
    data: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseTask(ABC):
    """Abstract base task with lifecycle hooks."""

    def __init__(
        self,
        task_id: Optional[str] = None,
        name: Optional[str] = None,
        func: Optional[Callable] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.task_id = task_id or str(uuid.uuid4())
        self.name = name or self.task_id
        self.func = func
        self.config = config or {}
        self.result: Optional[Result] = None

    def execute(self, context: ExecutionContext) -> Result:
        self.on_start(context)
        try:
            data = self._do_execute(context)
            self.result = Result(status="success", data=data, started_at=datetime.now(), completed_at=datetime.now())
            self.on_success(context, self.result)
        except Exception as exc:
            self.result = Result(status="failed", error=str(exc), started_at=datetime.now(), completed_at=datetime.now())
            self.on_failure(context, exc)
        return self.result

    @abstractmethod
    def _do_execute(self, context: ExecutionContext) -> Any:
        ...

    def on_start(self, context: ExecutionContext) -> None:
        from pangflow.observer.subject import get_subject
        get_subject().publish(
            "NODE_START",
            {
                "node_id": self.task_id,
                "node_name": self.name,
                "workflow_id": context.workflow_id,
                "run_id": context.run_id,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def on_success(self, context: ExecutionContext, result: Result) -> None:
        from pangflow.observer.subject import get_subject
        get_subject().publish(
            "NODE_COMPLETE",
            {
                "node_id": self.task_id,
                "node_name": self.name,
                "workflow_id": context.workflow_id,
                "run_id": context.run_id,
                "duration_ms": None,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def on_failure(self, context: ExecutionContext, exc: Exception) -> None:
        from pangflow.observer.subject import get_subject
        get_subject().publish(
            "NODE_FAILURE",
            {
                "node_id": self.task_id,
                "node_name": self.name,
                "workflow_id": context.workflow_id,
                "run_id": context.run_id,
                "exception": str(exc),
                "timestamp": datetime.now().isoformat(),
            },
        )
