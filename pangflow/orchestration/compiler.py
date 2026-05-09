# -*- coding: utf-8 -*-
"""
FlowCompiler – compiles a :class:`DAGBuilder` into a Prefect Flow.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path
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
        workflow_id: Optional[str] = None,
    ) -> Callable:
        if not PREFECT_AVAILABLE:
            raise RuntimeError(
                "Prefect is not installed. Install it with: pip install prefect"
            )

        layers = dag.topological_sort()
        effective_wf_id = self._resolve_workflow_id(workflow_id or workflow_name)

        # Build a Prefect @task wrapper for every node.
        task_map: Dict[str, Callable] = {}
        for meta in dag.nodes.values():
            task_map[meta.node_id] = self._wrap_task(
                meta, env_manager, effective_wf_id, workflow_name, dag
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
        workflow_name: str,
        dag: Any,
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

            # Inject log context so pf.log() / pf.log_metric() know current node
            import pangflow as _pf
            _pf._set_log_context(
                workflow_id=workflow_id,
                node_id=meta.node_id,
                node_name=meta.name,
                run_id=run_id,
            )

            get_subject().publish(
                "NODE_START",
                {
                    "node_id": meta.node_id,
                    "node_name": meta.name,
                    "workflow_id": workflow_id,
                    "run_id": run_id,
                },
            )
            self._write_node_log(run_id, workflow_id, workflow_name, meta, status="running")

            started_at = time.time()
            try:
                if env is not None:
                    result = self._run_in_conda(env, meta.func_ref, args, kwargs)
                else:
                    result = meta.func_ref(*args, **kwargs)

                duration_ms = (time.time() - started_at) * 1000

                # Track artifact produced by this node for lineage
                import pangflow as _pf
                output_artifact_id = getattr(_pf._log_context_local, '_last_artifact_id', None)
                get_subject().publish(
                    "NODE_COMPLETE",
                    {
                        "node_id": meta.node_id,
                        "node_name": meta.name,
                        "workflow_id": workflow_id,
                        "run_id": run_id,
                        "duration_ms": duration_ms,
                        "output_artifact_id": output_artifact_id,
                    },
                )
                self._write_node_log(
                    run_id, workflow_id, workflow_name, meta, status="success", duration_ms=duration_ms
                )
                # Write lineage edges if this node produced an artifact
                try:
                    self._write_lineage_edges(workflow_id, meta, dag)
                except Exception:
                    pass  # lineage is best-effort
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
                    workflow_name,
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
        workflow_name: str,
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
                    workflow_name=workflow_name,
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

    def _write_lineage_edges(self, workflow_id: str, meta: Any, dag: Any) -> None:
        """Write lineage edges from upstream artifacts to the current node's artifact.

        Recursively searches upstream nodes when direct predecessors have not
        produced an artifact themselves.
        """
        from pangflow.database.connection import get_db_manager
        from pangflow.database.models import ArtifactModel, LineageEdgeModel

        db = get_db_manager()
        with db.get_session() as session:
            artifact = (
                session.query(ArtifactModel)
                .filter_by(workflow_id=workflow_id, node_id=meta.node_id)
                .order_by(ArtifactModel.created_at.desc())
                .first()
            )
            if not artifact:
                return

            upstream_artifacts = self._find_upstream_artifacts(
                session, workflow_id, meta.node_id, dag, set()
            )
            for ua in upstream_artifacts:
                existing = (
                    session.query(LineageEdgeModel)
                    .filter_by(
                        from_artifact_id=ua.artifact_id,
                        to_artifact_id=artifact.artifact_id,
                    )
                    .first()
                )
                if not existing:
                    session.add(
                        LineageEdgeModel(
                            from_artifact_id=ua.artifact_id,
                            to_artifact_id=artifact.artifact_id,
                            edge_type="data_flow",
                        )
                    )

    def _find_upstream_artifacts(
        self,
        session: Any,
        workflow_id: str,
        node_id: str,
        dag: Any,
        visited: set,
    ) -> List[Any]:
        """Recursively find the nearest upstream artifacts for a node."""
        from pangflow.database.models import ArtifactModel

        if node_id in visited:
            return []
        visited.add(node_id)

        upstream_edges = dag.get_upstream_edges(node_id)
        upstream_node_ids = [e.from_node_id for e in upstream_edges]
        if not upstream_node_ids:
            return []

        direct = (
            session.query(ArtifactModel)
            .filter(
                ArtifactModel.workflow_id == workflow_id,
                ArtifactModel.node_id.in_(upstream_node_ids),
            )
            .all()
        )
        if direct:
            return direct

        # Recurse into upstream-of-upstream
        found: List[Any] = []
        for uid in upstream_node_ids:
            found.extend(
                self._find_upstream_artifacts(session, workflow_id, uid, dag, visited)
            )
        return found

    @staticmethod
    def _resolve_workflow_id(wf_id: Optional[str]) -> str:
        """If *wf_id* is a workflow name rather than a UUID, look up the UUID in DB.

        Falls back to the original *wf_id* when the DB is unavailable or the
        workflow has never been registered.
        """
        if not wf_id:
            return ""
        import re
        if re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            wf_id,
            re.I,
        ):
            return wf_id
        try:
            from pangflow.database.connection import get_db_manager, initialize_database
            from pangflow.database.models import WorkflowModel
            from pangflow.utils.workspace import find_workspace

            try:
                db = get_db_manager()
            except RuntimeError:
                # Database not initialised (common when calling a workflow
                # directly without going through the CLI).  Auto-discover
                # the workspace and initialise.
                ws_path = find_workspace()
                if ws_path is not None:
                    db_url = f"sqlite:///{ws_path / 'pangflow.db'}"
                else:
                    db_url = None
                db = initialize_database(db_url)

            with db.get_session() as session:
                wf = (
                    session.query(WorkflowModel)
                    .filter_by(workflow_name=wf_id)
                    .first()
                )
                if wf:
                    return wf.id
        except Exception:
            pass
        return wf_id

    def _run_in_conda(self, env, func, args, kwargs):
        import subprocess, sys, tempfile, cloudpickle, os
        env_name = getattr(env, "name", str(env))
        logger.info("Running node in conda env: %s", env_name)
        # Validate that the conda environment actually exists before attempting to run
        check = subprocess.run(
            ["conda", "run", "-n", env_name, "python", "--version"],
            capture_output=True, text=True, check=False
        )
        if check.returncode != 0:
            raise RuntimeError(
                f"Conda environment '{env_name}' is not available. "
                f"Please create it first with: pangflowctl env create --workflow <name>"
            )
        
        # Serialize function + args via cloudpickle
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pkl", delete=False) as fh:
            cloudpickle.dump((func, args, kwargs), fh)
            input_path = fh.name
        output_path = input_path + ".out"
        script_path = input_path + ".py"
        
        # Propagate log context into the subprocess so pf.save_model() knows workflow/node
        import pangflow as _pf
        ctx = _pf._get_log_context()
        
        # Resolve workspace path for subprocess cwd
        from pangflow.utils.workspace import find_workspace
        workspace_path = find_workspace()

        # Resolve pangflow package root so the conda subprocess can import it
        import importlib.util
        pangflow_spec = importlib.util.find_spec("pangflow")
        pangflow_pkg_root = (
            str(Path(pangflow_spec.origin).parent.parent)
            if (pangflow_spec and pangflow_spec.origin)
            else ""
        )

        script = (
            "import cloudpickle\n"
            "import os\n"
            "import sys\n"
            "from pangflow.utils.workspace import find_workspace\n"
            "_ws = find_workspace()\n"
            "if _ws:\n"
            "    os.chdir(str(_ws))\n"
        )
        if pangflow_pkg_root:
            script += (
                f"_PANGFLOW_ROOT = {pangflow_pkg_root!r}\n"
                "if _PANGFLOW_ROOT not in sys.path:\n"
                "    sys.path.insert(0, _PANGFLOW_ROOT)\n"
            )
        script += (
            "import pangflow as _pf\n"
            f"os.environ['PANGFLOW_RUN_ID'] = {ctx.get('run_id')!r}\n"
            f"_pf._set_log_context(\n"
            f"    workflow_id={ctx.get('workflow_id')!r},\n"
            f"    node_id={ctx.get('node_id')!r},\n"
            f"    node_name={ctx.get('node_name')!r},\n"
            f"    run_id={ctx.get('run_id')!r},\n"
            f")\n"
            f"with open({input_path!r}, 'rb') as fh:\n"
            "    func, args, kwargs = cloudpickle.load(fh)\n"
            "result = func(*args, **kwargs)\n"
            f"with open({output_path!r}, 'wb') as fh:\n"
            "    cloudpickle.dump(result, fh)\n"
        )
        with open(script_path, "w", encoding="utf-8") as fh:
            fh.write(script)
        
        try:
            cmd = ["conda", "run", "-n", env_name, "python", script_path]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                cwd=str(workspace_path) if workspace_path else None,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"Conda run failed: {proc.stderr}")
            with open(output_path, "rb") as fh:
                return cloudpickle.load(fh)
        finally:
            for p in (input_path, script_path, output_path):
                try:
                    os.unlink(p)
                except OSError:
                    pass
