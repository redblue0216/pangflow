# -*- coding: utf-8 -*-
"""
Task Layer – NodeTask with lifecycle hooks, context injection, and auto-persistence.
"""

import contextvars
import inspect
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from pangflow.task.base import BaseTask, ExecutionContext, Result

logger = logging.getLogger(__name__)

# Thread-safe context variable for log context injection.
_log_context_var: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar(
    "pangflow_node_log_context", default=None
)


@dataclass
class LogContext:
    """Structured log context injected at node runtime."""

    workflow_id: str
    node_id: str
    run_id: str
    node_name: Optional[str] = None
    trace_id: Optional[str] = None


@dataclass
class ArtifactContext:
    """Artifact persistence configuration."""

    artifact_name: Optional[str] = None
    feature_name: Optional[str] = None
    version: str = "1.0.0"
    tags: Dict[str, Any] = field(default_factory=dict)
    lifecycle: str = "hot"
    ttl_days: Optional[int] = None


@dataclass
class NodeMetadata:
    """Metadata describing a node task."""

    name: Optional[str] = None
    description: Optional[str] = None
    log_context: Optional[LogContext] = None
    artifact_context: Optional[ArtifactContext] = None
    retries: int = 0
    timeout: Optional[int] = None


def _get_default_model_store() -> Any:
    """Lazily initialise a default ModelStore with a LocalFileBackend."""
    from pangflow.storage.backend import LocalFileBackend
    from pangflow.storage.meta_store import MetaStore
    from pangflow.storage.model_store import ModelStore

    backend = LocalFileBackend(os.environ.get("PANGFLOW_STORAGE_PATH", "./pangflow_storage"))
    return ModelStore(file_backend=backend, meta_store=MetaStore())


def _get_default_feature_store() -> Any:
    """Lazily initialise a default FeatureStore with a LocalFileBackend."""
    from pangflow.storage.backend import LocalFileBackend
    from pangflow.storage.meta_store import MetaStore
    from pangflow.storage.feature_store import FeatureStore

    backend = LocalFileBackend(os.environ.get("PANGFLOW_STORAGE_PATH", "./pangflow_storage"))
    return FeatureStore(file_backend=backend, meta_store=MetaStore())


class NodeTask(BaseTask):
    """Concrete task implementation for workflow nodes."""

    def __init__(
        self,
        task_id: Optional[str] = None,
        name: Optional[str] = None,
        func: Optional[Callable] = None,
        config: Optional[Dict[str, Any]] = None,
        metadata: Optional[NodeMetadata] = None,
    ):
        super().__init__(task_id=task_id, name=name, func=func, config=config)
        self.metadata = metadata or NodeMetadata(name=self.name)

    # --------------------------------------------------------------------- #
    # Lifecycle hooks
    # --------------------------------------------------------------------- #

    def pre_execute(self, context: ExecutionContext) -> None:
        """Inject log context into a thread-local contextvar."""
        log_ctx = LogContext(
            workflow_id=context.workflow_id,
            node_id=self.task_id,
            run_id=context.run_id,
            node_name=self.name,
            trace_id=context.trace_id,
        )
        self.metadata.log_context = log_ctx
        _log_context_var.set(log_ctx.__dict__)

    def post_execute(self, context: ExecutionContext, result: Result) -> None:
        """Auto-save artifact or feature if configured via decorator."""
        if result.status != "success" or result.data is None:
            return

        artifact_cfg = self.config.get("artifact")
        feature_cfg = self.config.get("feature")

        if artifact_cfg:
            self._persist_artifact(context, result.data, artifact_cfg)

        if feature_cfg:
            self._persist_feature(context, result.data, feature_cfg)

    def on_failure(self, context: ExecutionContext, exc: Exception) -> None:
        """Publish NODE_FAILURE and log the exception."""
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
        logger.exception(
            "Node %s failed in workflow %s run %s",
            self.task_id,
            context.workflow_id,
            context.run_id,
        )

    # --------------------------------------------------------------------- #
    # Execution
    # --------------------------------------------------------------------- #

    def execute(self, context: ExecutionContext) -> Result:
        self.pre_execute(context)
        try:
            data = self._do_execute(context)
            self.result = Result(
                status="success",
                data=data,
                started_at=datetime.now(),
                completed_at=datetime.now(),
            )
            self.on_success(context, self.result)
            self.post_execute(context, self.result)
        except Exception as exc:
            self.result = Result(
                status="failed",
                error=str(exc),
                started_at=datetime.now(),
                completed_at=datetime.now(),
            )
            self.on_failure(context, exc)
        finally:
            _log_context_var.set(None)
        return self.result

    def _do_execute(self, context: ExecutionContext) -> Any:
        if self.func is None:
            raise RuntimeError("NodeTask.func is not set")

        sig = inspect.signature(self.func)
        bound: Dict[str, Any] = {}

        for param_name, param in sig.parameters.items():
            if param_name == "context":
                bound["context"] = context
                continue
            if param_name in context.runtime_params:
                bound[param_name] = context.runtime_params[param_name]
            elif param.default is not inspect.Parameter.empty:
                bound[param_name] = param.default
            else:
                raise TypeError(
                    f"Missing required argument '{param_name}' for node '{self.name}'"
                )

        return self.func(**bound)

    # --------------------------------------------------------------------- #
    # Persistence helpers
    # --------------------------------------------------------------------- #

    def _persist_artifact(
        self, context: ExecutionContext, data: Any, artifact_cfg: Any
    ) -> None:
        name = artifact_cfg if isinstance(artifact_cfg, str) else artifact_cfg.get("name", "model")
        version = (
            artifact_cfg.get("version", "1.0.0")
            if isinstance(artifact_cfg, dict)
            else "1.0.0"
        )
        tags = (
            artifact_cfg.get("tags", {})
            if isinstance(artifact_cfg, dict)
            else {}
        )
        lifecycle = (
            artifact_cfg.get("lifecycle", "hot")
            if isinstance(artifact_cfg, dict)
            else "hot"
        )

        store = _get_default_model_store()
        store.save_model(
            data,
            metadata={
                "name": name,
                "version": version,
                "workflow_id": context.workflow_id,
                "node_id": self.task_id,
                "tags": tags,
                "lifecycle": lifecycle,
            },
        )

    def _persist_feature(
        self, context: ExecutionContext, data: Any, feature_cfg: Any
    ) -> None:
        name = feature_cfg if isinstance(feature_cfg, str) else feature_cfg.get("name", "feature")
        version = (
            feature_cfg.get("version", "1.0.0")
            if isinstance(feature_cfg, dict)
            else "1.0.0"
        )
        tags = (
            feature_cfg.get("tags", {})
            if isinstance(feature_cfg, dict)
            else {}
        )
        ttl_days = (
            feature_cfg.get("ttl_days")
            if isinstance(feature_cfg, dict)
            else None
        )
        lifecycle = (
            feature_cfg.get("lifecycle", "hot")
            if isinstance(feature_cfg, dict)
            else "hot"
        )
        partition = (
            feature_cfg.get("partition_key", "default")
            if isinstance(feature_cfg, dict)
            else "default"
        )

        store = _get_default_feature_store()
        store.save_feature(
            data,
            metadata={
                "name": name,
                "version": version,
                "partition_key": partition,
                "workflow_id": context.workflow_id,
                "node_id": self.task_id,
                "tags": tags,
                "ttl_days": ttl_days,
                "lifecycle": lifecycle,
            },
        )
