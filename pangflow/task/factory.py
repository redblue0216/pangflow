# -*- coding: utf-8 -*-
"""
Task Layer – Factory for building concrete task instances from decorator configs.
"""

from typing import Any, Callable, Dict, Optional

from pangflow.task.base import BaseTask
from pangflow.task.node_task import NodeTask, NodeMetadata, LogContext, ArtifactContext
from pangflow.task.serve_task import ServeTask, ServeMetadata, HTTPEndpoint


class TaskFactory:
    """Creates ``BaseTask`` subclasses from metadata or decorator configurations."""

    @classmethod
    def create_task(cls, metadata: Dict[str, Any]) -> BaseTask:
        """Dispatch to the correct builder based on *metadata* type.

        Supported keys:
        - ``type`` == "node"  -> NodeTask
        - ``type`` == "serve" -> ServeTask
        """
        task_type = metadata.get("type", "node")
        if task_type == "serve":
            return cls._build_serve_task(metadata)
        return cls._build_node_task(metadata)

    @classmethod
    def from_node_decorator(cls, func: Callable, config: Optional[Dict[str, Any]] = None) -> NodeTask:
        """Build a ``NodeTask`` from a function decorated with ``@node``."""
        cfg = config or {}
        name = cfg.get("name") or getattr(func, "__name__", None)

        artifact_ctx = None
        artifact_cfg = cfg.get("artifact")
        feature_cfg = cfg.get("feature")
        if artifact_cfg:
            artifact_ctx = ArtifactContext(
                artifact_name=artifact_cfg if isinstance(artifact_cfg, str) else artifact_cfg.get("name"),
                version=(artifact_cfg.get("version", "1.0.0") if isinstance(artifact_cfg, dict) else "1.0.0"),
                tags=(artifact_cfg.get("tags", {}) if isinstance(artifact_cfg, dict) else {}),
                lifecycle=(artifact_cfg.get("lifecycle", "hot") if isinstance(artifact_cfg, dict) else "hot"),
            )
        if feature_cfg:
            artifact_ctx = ArtifactContext(
                feature_name=feature_cfg if isinstance(feature_cfg, str) else feature_cfg.get("name"),
                version=(feature_cfg.get("version", "1.0.0") if isinstance(feature_cfg, dict) else "1.0.0"),
                tags=(feature_cfg.get("tags", {}) if isinstance(feature_cfg, dict) else {}),
                lifecycle=(feature_cfg.get("lifecycle", "hot") if isinstance(feature_cfg, dict) else "hot"),
                ttl_days=(feature_cfg.get("ttl_days") if isinstance(feature_cfg, dict) else None),
            )

        node_meta = NodeMetadata(
            name=name,
            description=cfg.get("description"),
            artifact_context=artifact_ctx,
            retries=cfg.get("retries", 0),
            timeout=cfg.get("timeout"),
        )

        return NodeTask(
            name=name,
            func=func,
            config=cfg,
            metadata=node_meta,
        )

    @classmethod
    def from_serve_decorator(cls, func: Callable, config: Optional[Dict[str, Any]] = None) -> ServeTask:
        """Build a ``ServeTask`` from a function decorated with ``@serve``."""
        cfg = config or {}
        name = cfg.get("name") or getattr(func, "__name__", None)

        endpoint_cfg = cfg.get("endpoint", {})
        endpoint = HTTPEndpoint(
            path=endpoint_cfg.get("path", "/"),
            method=endpoint_cfg.get("method", "POST"),
            host=endpoint_cfg.get("host"),
            port=endpoint_cfg.get("port"),
        )

        serve_meta = ServeMetadata(
            name=name,
            description=cfg.get("description"),
            endpoint=endpoint,
            input_schema=cfg.get("input_schema"),
            output_schema=cfg.get("output_schema"),
        )

        return ServeTask(
            name=name,
            func=func,
            config=cfg,
            metadata=serve_meta,
        )

    @classmethod
    def _build_node_task(cls, metadata: Dict[str, Any]) -> NodeTask:
        func = metadata.get("func")
        if func is None:
            raise ValueError("NodeTask metadata missing 'func'")
        return cls.from_node_decorator(func, metadata.get("config", {}))

    @classmethod
    def _build_serve_task(cls, metadata: Dict[str, Any]) -> ServeTask:
        func = metadata.get("func")
        if func is None:
            raise ValueError("ServeTask metadata missing 'func'")
        return cls.from_serve_decorator(func, metadata.get("config", {}))
