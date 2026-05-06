# -*- coding: utf-8 -*-
"""
FlowRunner – sync/async workflow execution with a simple in-memory queue.
"""

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from pangflow.core.task import TaskResult, TaskStatus

logger = logging.getLogger(__name__)


@dataclass
class FlowResult:
    """Result of a flow execution."""

    task_id: str
    status: str
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class _AsyncTask:
    task_id: str
    flow: Callable[..., Any]
    parameters: Dict[str, Any]
    status: str = "pending"
    result: Optional[FlowResult] = None
    thread: Optional[threading.Thread] = None


class FlowRunner:
    """Runs flows synchronously or asynchronously via an in-memory queue."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: Dict[str, _AsyncTask] = {}

    def run_sync(self, flow: Callable[..., Any], parameters: Optional[Dict[str, Any]] = None) -> FlowResult:
        """Execute *flow* directly in the current thread."""
        parameters = parameters or {}
        task_id = str(uuid.uuid4())
        started_at = datetime.now()
        try:
            logger.info(f"[FlowRunner] Starting sync flow {task_id}")
            output = flow(**parameters)
            result = FlowResult(
                task_id=task_id,
                status="success",
                output=output,
                started_at=started_at,
                completed_at=datetime.now(),
            )
            logger.info(f"[FlowRunner] Sync flow {task_id} succeeded")
            return result
        except Exception as exc:
            logger.exception(f"[FlowRunner] Sync flow {task_id} failed")
            return FlowResult(
                task_id=task_id,
                status="failed",
                error=str(exc),
                started_at=started_at,
                completed_at=datetime.now(),
            )

    def run_async(self, flow: Callable[..., Any], parameters: Optional[Dict[str, Any]] = None) -> str:
        """Submit *flow* to the in-memory queue and return a task_id."""
        parameters = parameters or {}
        task_id = str(uuid.uuid4())
        task = _AsyncTask(task_id=task_id, flow=flow, parameters=parameters)

        with self._lock:
            self._tasks[task_id] = task

        def _run() -> None:
            task.status = "running"
            started_at = datetime.now()
            try:
                output = flow(**task.parameters)
                task.result = FlowResult(
                    task_id=task_id,
                    status="success",
                    output=output,
                    started_at=started_at,
                    completed_at=datetime.now(),
                )
                task.status = "success"
            except Exception as exc:
                task.result = FlowResult(
                    task_id=task_id,
                    status="failed",
                    error=str(exc),
                    started_at=started_at,
                    completed_at=datetime.now(),
                )
                task.status = "failed"
            finally:
                logger.info(f"[FlowRunner] Async flow {task_id} finished with status {task.status}")

        thread = threading.Thread(target=_run, daemon=True)
        task.thread = thread
        thread.start()
        logger.info(f"[FlowRunner] Submitted async flow {task_id}")
        return task_id

    def get_status(self, task_id: str) -> Optional[str]:
        """Return the status of an async task, or None if unknown."""
        with self._lock:
            task = self._tasks.get(task_id)
        return task.status if task else None

    def get_result(self, task_id: str) -> Optional[FlowResult]:
        """Return the result of an async task, or None if unknown/not finished."""
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            return None
        return task.result
