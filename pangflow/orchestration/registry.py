# -*- coding: utf-8 -*-
"""
NodeRegistry singleton + @node / @workflow / @serve decorators.
"""

from __future__ import annotations

import functools
import inspect
import logging
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pangflow.orchestration.proxy import NodeProxy, clear_active_dag, set_active_dag
from pangflow.orchestration.dag import DAGBuilder

logger = logging.getLogger(__name__)


@dataclass
class NodeMetadata:
    """Runtime metadata for a single @node-decorated function."""

    node_id: str
    name: str
    func_ref: Callable
    signature: inspect.Signature
    config: Dict[str, Any] = field(default_factory=dict)
    input_types: Dict[str, type] = field(default_factory=dict)
    output_type: Any = None
    is_artifact: bool = False
    is_feature: bool = False
    log_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServeMetadata:
    """Runtime metadata for a single @serve-decorated function."""

    endpoint: str
    method: str
    func_ref: Callable
    name: str


class NodeRegistry:
    """Thread-safe singleton registry for nodes and serve endpoints."""

    _instance: Optional[NodeRegistry] = None
    _lock = threading.Lock()

    def __new__(cls) -> NodeRegistry:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._registry: Dict[str, NodeMetadata] = {}
                    cls._instance._by_name: Dict[str, str] = {}
                    cls._instance._serve_registry: Dict[str, ServeMetadata] = {}
        return cls._instance

    # ------------------------------------------------------------------ #
    # Node API
    # ------------------------------------------------------------------ #
    def register(
        self, func: Callable, config: Optional[Dict[str, Any]] = None
    ) -> NodeMetadata:
        config = config or {}
        name = config.get("name", getattr(func, "__name__", "anonymous"))
        node_id = config.get("node_id", f"{name}_{uuid.uuid4().hex[:8]}")

        sig = inspect.signature(func)
        input_types = {
            pname: param.annotation
            for pname, param in sig.parameters.items()
            if param.annotation is not inspect.Parameter.empty
        }
        output_type = (
            sig.return_annotation
            if sig.return_annotation is not inspect.Parameter.empty
            else None
        )

        meta = NodeMetadata(
            node_id=node_id,
            name=name,
            func_ref=func,
            signature=sig,
            config=config,
            input_types=input_types,
            output_type=output_type,
            is_artifact=bool(config.get("artifact", False)),
            is_feature=bool(config.get("feature", False)),
            log_config=config.get("log", {}),
        )

        if node_id in self._registry:
            logger.warning("Overwriting existing node registration: %s", node_id)

        self._registry[node_id] = meta
        self._by_name[name] = node_id
        return meta

    def get(self, node_id: str) -> Optional[NodeMetadata]:
        return self._registry.get(node_id)

    def get_by_name(self, name: str) -> Optional[NodeMetadata]:
        nid = self._by_name.get(name)
        return self._registry.get(nid) if nid else None

    def list(self) -> List[NodeMetadata]:
        return list(self._registry.values())

    def unregister(self, node_id: str) -> bool:
        if node_id not in self._registry:
            return False
        meta = self._registry.pop(node_id)
        if meta.name in self._by_name:
            del self._by_name[meta.name]
        return True

    # ------------------------------------------------------------------ #
    # Serve API
    # ------------------------------------------------------------------ #
    def register_serve(
        self,
        func: Callable,
        endpoint: str,
        method: str = "POST",
        name: Optional[str] = None,
    ) -> ServeMetadata:
        meta = ServeMetadata(
            endpoint=endpoint,
            method=method.upper(),
            func_ref=func,
            name=name or getattr(func, "__name__", "anonymous"),
        )
        self._serve_registry[endpoint] = meta
        return meta

    def list_serve(self) -> List[ServeMetadata]:
        return list(self._serve_registry.values())


# ---------------------------------------------------------------------- #
# Decorators
# ---------------------------------------------------------------------- #
def node(name=None, log=None, artifact=False, feature=False, stage=None, **kwargs):
    """Decorate an algorithm function as a workflow node.

    Returns a :class:`NodeProxy` that overloads ``>>`` and intercepts calls
    for DAG building when used inside a ``@workflow``.
    """

    def decorator(func: Callable) -> NodeProxy:
        config = {
            "name": name or getattr(func, "__name__", "anonymous"),
            "log": log,
            "artifact": artifact,
            "feature": feature,
            "stage": stage,
            **kwargs,
        }
        meta = NodeRegistry().register(func, config)
        return NodeProxy(meta)

    return decorator


def workflow(name=None, schedule=None, **kwargs):
    """Decorate a workflow orchestration function.

    When called, the wrapped function executes in a DAG-building context,
    validates the resulting graph, and returns a compiled Prefect flow.
    """

    def decorator(func: Callable) -> Callable:
        workflow_name = name or getattr(func, "__name__", "anonymous")

        @functools.wraps(func)
        def wrapper(*args, **func_kwargs):
            dag = DAGBuilder()
            set_active_dag(dag)
            try:
                result = func(*args, **func_kwargs)
            finally:
                clear_active_dag()

            # Pull in any nodes that were referenced but never used as edges.
            # (e.g. a bare ``load_data`` call with no downstream consumer)
            # The proxy __call__ already added them, but ensure they exist.
            dag.validate()

            import os
            from pangflow.env.manager import EnvManager
            from pangflow.orchestration.compiler import FlowCompiler

            env_manager = EnvManager()
            compiler = FlowCompiler()
            workflow_id = os.environ.get("PANGFLOW_WORKFLOW_ID")
            compiled_flow = compiler.compile(
                dag, env_manager, workflow_name=workflow_name, workflow_id=workflow_id
            )
            return compiled_flow

        wrapper._pangflow_workflow = True
        wrapper._pangflow_name = workflow_name
        wrapper._pangflow_schedule = schedule
        wrapper._pangflow_kwargs = kwargs
        return wrapper

    return decorator


def serve(endpoint, method="POST", **kwargs):
    """Decorate a function as an HTTP endpoint.

    The endpoint is registered in :class:`NodeRegistry` and compiled later
    by :class:`ServeCompiler`.
    """

    def decorator(func: Callable) -> Callable:
        NodeRegistry().register_serve(
            func,
            endpoint,
            method,
            name=kwargs.get("name", getattr(func, "__name__", "anonymous")),
        )

        @functools.wraps(func)
        def wrapper(*args, **func_kwargs):
            return func(*args, **func_kwargs)

        wrapper._pangflow_serve = True
        wrapper._pangflow_endpoint = endpoint
        wrapper._pangflow_method = method
        return wrapper

    return decorator
