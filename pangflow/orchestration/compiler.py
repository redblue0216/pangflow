# -*- coding: utf-8 -*-
"""
FlowCompiler – compiles a :class:`DAGBuilder` into a Prefect Flow.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Callable, Dict, List

try:
    from prefect import flow, task

    PREFECT_AVAILABLE = True
except ImportError:  # pragma: no cover
    PREFECT_AVAILABLE = False
    flow = None
    task = None

from pangflow.orchestration.dag import DAGBuilder
from pangflow.task.base import ExecutionContext

# Graceful fallback if node_task module does not yet exist.
try:
    from pangflow.task.node_task import NodeTask
except ImportError:  # pragma: no cover
    from pangflow.task.base import BaseTask as NodeTask

from pangflow.env.manager import EnvManager
from pangflow.observer.subject import get_subject

logger = logging.getLogger(__name__)


class FlowCompiler:
    """Compiles a pangflow DAG into an executable Prefect flow."""

    def compile(
        self,
        dag: DAGBuilder,
        env_manager: EnvManager,
        workflow_name: str = "workflow",
    ) -> Callable:
        if not PREFECT_AVAILABLE:
            raise RuntimeError(
                "Prefect is not installed. Install it with: pip install prefect"
            )

        layers = dag.topological_sort()

        # Build a Prefect @task wrapper for every node.
        task_map: Dict[str, Callable] = {}
        for meta in dag.nodes.values():
            task_map[meta.node_id] = self._wrap_task(
                meta, env_manager, workflow_name
            )

        @flow(name=workflow_name)
        def compiled_flow(**runtime_params: Any) -> Any:
            results: Dict[str, Any] = {}

            for layer in layers:
                for meta in layer:
                    upstream_edges = dag.get_upstream_edges(meta.node_id)

                    args: List[Any] = []
                    kwargs: Dict[str, Any] = {}
                    wait_for: List[Any] = []

                    for edge in upstream_edges:
                        upstream_result = results[edge.from_node_id]
                        wait_for.append(upstream_result)

                        if not edge.param_mapping:
                            # Default mapping → first positional arg.
                            args.append(upstream_result)
                        else:
                            for param_name, upstream_id in edge.param_mapping.items():
                                result = results[upstream_id]
                                if param_name.startswith("__pos_"):
                                    idx = int(param_name.split("_pos_")[1].strip("_"))
                                    while len(args) <= idx:
                                        args.append(None)
                                    args[idx] = result
                                else:
                                    kwargs[param_name] = result

                    # Compact positional args (remove None placeholders).
                    args = [a for a in args if a is not None]

                    t = task_map[meta.node_id]
                    if wait_for:
                        results[meta.node_id] = t(
                            *args, **kwargs, wait_for=wait_for
                        )
                    else:
                        results[meta.node_id] = t(*args, **kwargs)

            # Return the last layer's last node result (typical workflow tail).
            if layers:
                return results[layers[-1][-1].node_id]
            return None

        return compiled_flow

    def _wrap_task(
        self,
        meta: Any,  # NodeMetadata
        env_manager: EnvManager,
        workflow_id: str,
    ) -> Callable:
        try:
            env = env_manager.get_env(workflow_id)
        except RuntimeError:
            env = None

        @task(name=meta.name)
        def _task(*args: Any, **kwargs: Any) -> Any:
            # ``wait_for`` may leak through from Prefect; strip it.
            kwargs.pop("wait_for", None)

            ctx = ExecutionContext(
                workflow_id=workflow_id,
                node_id=meta.node_id,
                node_name=meta.name,
                env=env,
                runtime_params=kwargs.pop("runtime_params", {}),
            )

            run_id = os.environ.get("PANGFLOW_RUN_ID", str(uuid.uuid4()))

            get_subject().publish(
                "NODE_START",
                {
                    "node_id": meta.node_id,
                    "node_name": meta.name,
                    "workflow_id": workflow_id,
                    "run_id": run_id,
                },
            )
            self._write_node_log(run_id, workflow_id, meta, status="running")

            started_at = time.time()
            try:
                if env is not None:
                    result = self._run_in_conda(env, meta.func_ref, args, kwargs)
                else:
                    result = meta.func_ref(*args, **kwargs)

                duration_ms = (time.time() - started_at) * 1000

                get_subject().publish(
                    "NODE_COMPLETE",
                    {
                        "node_id": meta.node_id,
                        "node_name": meta.name,
                        "workflow_id": workflow_id,
                        "run_id": run_id,
                        "duration_ms": duration_ms,
                    },
                )
                self._write_node_log(
                    run_id, workflow_id, meta, status="success", duration_ms=duration_ms
                )
                return result
            except Exception as exc:
                duration_ms = (time.time() - started_at) * 1000
                get_subject().publish(
                    "NODE_FAILURE",
                    {
                        "node_id": meta.node_id,
                        "node_name": meta.name,
                        "workflow_id": workflow_id,
                        "run_id": run_id,
                        "exception": str(exc),
                    },
                )
                self._write_node_log(
                    run_id,
                    workflow_id,
                    meta,
                    status="failed",
                    duration_ms=duration_ms,
                    exception=str(exc),
                )
                raise

        return _task

    def _write_node_log(
        self,
        run_id: str,
        workflow_id: str,
        meta: Any,
        status: str,
        duration_ms: Optional[float] = None,
        exception: Optional[str] = None,
    ) -> None:
        """Write a node execution status record to node_logs table."""
        try:
            from pathlib import Path
            from pangflow.database.connection import get_db_manager, initialize_database
            from pangflow.database.models import NodeLogModel
            from pangflow.utils.workspace import find_workspace
            from datetime import datetime

            try:
                db_manager = get_db_manager()
            except RuntimeError:
                workspace_path = find_workspace()
                if workspace_path is not None:
                    db_url = f"sqlite:///{workspace_path / 'pangflow.db'}"
                else:
                    db_url = None
                db_manager = initialize_database(db_url)

            with db_manager.get_session() as session:
                log = NodeLogModel(
                    timestamp=datetime.now(),
                    workflow_id=workflow_id,
                    workflow_name=workflow_id,
                    node_id=meta.node_id,
                    node_name=meta.name,
                    log_type="auto",
                    level="INFO" if status in ("running", "success") else "ERROR",
                    message=f"Node {meta.name} {status}",
                    duration_ms=duration_ms,
                    exception=exception,
                    trace_id=run_id,
                    run_id=run_id,
                )
                session.add(log)
        except Exception:
            logger.debug("Failed to write node log (DB may not be initialized)", exc_info=True)

    def _run_in_conda(
        self, env: Any, func: Callable, args: tuple, kwargs: dict
    ) -> Any:
        """Pragmatic conda wrapper – logs and falls back to direct call."""
        env_name = getattr(env, "name", str(env))
        logger.info("Running node in conda env: %s", env_name)
        # Full ``conda run`` implementation would serialise args and invoke
        # a subprocess. For v0.2.7 we keep it simple and call in-process.
        return func(*args, **kwargs)
