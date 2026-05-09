# -*- coding: utf-8 -*-
"""
PangFlow Web API – REST endpoints for workflows, executions, logs, metrics,
lineage, models and system status.
"""

import logging
import subprocess
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from pangflow.database.connection import get_db_manager
from pangflow.database.models import (
    ArtifactModel,
    ArtifactVersionModel,
    ExecutionLogModel,
    FeatureModel,
    LineageEdgeModel,
    NodeLogModel,
    ServiceModel,
    TraceModel,
    WorkflowModel,
)
from pangflow.serve.flow_runner import FlowRunner, FlowResult

logger = logging.getLogger(__name__)
router = APIRouter()
flow_runner = FlowRunner()


# --------------------------------------------------------------------------- #
# Dependencies
# --------------------------------------------------------------------------- #

def get_db_session() -> Generator[Session, None, None]:
    db_manager = get_db_manager()
    session = db_manager._session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# --------------------------------------------------------------------------- #
# Pydantic models
# --------------------------------------------------------------------------- #

class TriggerRequest(BaseModel):
    parameters: Optional[Dict[str, Any]] = None


class TriggerResponse(BaseModel):
    task_id: str
    status: str


class PromoteRequest(BaseModel):
    stage: str = "production"
    promotion_note: Optional[str] = None


class PromoteResponse(BaseModel):
    version_id: str
    artifact_id: str
    stage: str


class StatusResponse(BaseModel):
    database: str
    prefect: str
    timestamp: str


# --------------------------------------------------------------------------- #
# Workflows
# --------------------------------------------------------------------------- #

@router.get("/api/workflows")
def list_workflows(
    package_name: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """List all workflows, optionally filtered by package name."""
    query = session.query(WorkflowModel)
    if package_name:
        query = query.filter_by(package_name=package_name)
    workflows = query.order_by(WorkflowModel.created_at.desc()).limit(limit).all()
    result = []
    for w in workflows:
        d = w.to_dict()
        d["name"] = d.pop("workflow_name", w.workflow_name)
        result.append(d)
    return result


@router.get("/api/workflows/{workflow_id}")
def get_workflow(
    workflow_id: str,
    session: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    """Get workflow detail by ID."""
    workflow = session.query(WorkflowModel).filter_by(id=workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow.to_dict()


@router.post("/api/workflows/{workflow_id}/trigger")
def trigger_workflow(
    workflow_id: str,
    req: Optional[TriggerRequest] = None,
    session: Session = Depends(get_db_session),
) -> TriggerResponse:
    """Trigger a workflow execution."""
    workflow = session.query(WorkflowModel).filter_by(id=workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    command = workflow.command or ""
    if not command:
        raise HTTPException(status_code=400, detail="Workflow has no command")

    run_id = str(__import__("uuid").uuid4())
    log = ExecutionLogModel(
        workflow_id=workflow_id,
        run_id=run_id,
        execution_type="trigger",
        status="running",
        triggered_by="api",
        started_at=datetime.now(),
    )
    session.add(log)
    session.commit()

    params = (req.parameters if req else None) or {}

    def _update_log_in_thread(
        log_id: int,
        status: str,
        return_code: Optional[int] = None,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        """Update execution log in a background thread.

        Uses a manually-managed session so that an exception raised afterwards
        does NOT trigger a rollback of the status update.
        """
        db_manager = get_db_manager()
        inner_session = db_manager._session_factory()
        try:
            inner_log = inner_session.query(ExecutionLogModel).filter_by(id=log_id).first()
            if inner_log:
                inner_log.status = status
                if return_code is not None:
                    inner_log.return_code = return_code
                if stdout:
                    inner_log.stdout = stdout
                if stderr:
                    inner_log.stderr = stderr
                if status != "running":
                    inner_log.completed_at = datetime.now()
            inner_session.commit()
        except Exception:
            inner_session.rollback()
            raise
        finally:
            inner_session.close()

    def _run_flow() -> Dict[str, Any]:
        """Internal flow function executed by FlowRunner."""
        try:
            import os
            env = os.environ.copy()
            env["PANGFLOW_RUN_ID"] = run_id
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=workflow.working_dir or None,
                timeout=params.get("timeout", 3600),
                env=env,
            )
            status = "success" if result.returncode == 0 else "failed"
            _update_log_in_thread(
                log.id, status=status, return_code=result.returncode,
                stdout=result.stdout, stderr=result.stderr,
            )
            return {
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except Exception as exc:
            db_manager = get_db_manager()
            inner_session = db_manager._session_factory()
            try:
                inner_log = inner_session.query(ExecutionLogModel).filter_by(id=log.id).first()
                if inner_log:
                    inner_log.status = "failed"
                    inner_log.stderr = str(exc)
                    inner_log.completed_at = datetime.now()
                inner_session.commit()
            finally:
                inner_session.close()
            raise

    task_id = flow_runner.run_async(_run_flow)
    return TriggerResponse(task_id=task_id, status="triggered")


# --------------------------------------------------------------------------- #
# Executions
# --------------------------------------------------------------------------- #

@router.get("/api/executions")
def list_executions(
    workflow_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """List execution logs."""
    query = session.query(ExecutionLogModel)
    if workflow_id:
        query = query.filter_by(workflow_id=workflow_id)
    logs = query.order_by(ExecutionLogModel.started_at.desc()).limit(limit).all()
    wf_map = {w.id: w.workflow_name for w in session.query(WorkflowModel).all()}
    result = []
    for l in logs:
        d = l.to_dict()
        d["workflow_name"] = wf_map.get(l.workflow_id, l.workflow_id)
        d["ended_at"] = d.pop("completed_at", None)
        result.append(d)
    return result


# --------------------------------------------------------------------------- #
# Execution Nodes & DAG
# --------------------------------------------------------------------------- #

@router.get("/api/executions/{run_id}/nodes")
def get_execution_nodes(
    run_id: str,
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """Get per-node execution status for a specific run."""
    logs = (
        session.query(NodeLogModel)
        .filter_by(run_id=run_id)
        .order_by(NodeLogModel.timestamp.asc())
        .all()
    )
    return [
        {
            "id": l.id,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
            "node_id": l.node_id,
            "node_name": l.node_name,
            "status": (
                "failed" if l.exception else
                "success" if (l.message and "success" in l.message) else
                "running" if (l.message and "running" in l.message) else
                "unknown"
            ),
            "duration_ms": l.duration_ms,
            "message": l.message,
            "exception": l.exception,
        }
        for l in logs
    ]


@router.get("/api/executions/{run_id}/dag")
def get_execution_dag(
    run_id: str,
    session: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    """Return DAG structure (nodes + edges) for a run.

    Prefers the persisted static DAG from the workflow record so the real
    topology (parallel branches, multi-input nodes) is preserved.  Runtime
    status from ``node_logs`` is overlaid on top of the static nodes.
    Falls back to the old chronological-chain behaviour when no static DAG
    is available.
    """
    import json

    # Resolve overall execution status first
    exec_log = (
        session.query(ExecutionLogModel)
        .filter_by(run_id=run_id)
        .first()
    )
    overall_status = exec_log.status if exec_log else "unknown"
    workflow_id = exec_log.workflow_id if exec_log else None

    # ------------------------------------------------------------------ #
    # Try the persisted static DAG first
    # ------------------------------------------------------------------ #
    if workflow_id:
        wf = session.query(WorkflowModel).filter_by(id=workflow_id).first()
        if wf and wf.dag_json:
            try:
                dag_data = json.loads(wf.dag_json)
            except json.JSONDecodeError:
                dag_data = None

            if dag_data:
                # Build runtime status map from node_logs (latest record per node)
                logs = (
                    session.query(NodeLogModel)
                    .filter_by(run_id=run_id)
                    .order_by(NodeLogModel.timestamp.asc())
                    .all()
                )
                runtime_map: Dict[str, Dict[str, Any]] = {}
                for l in logs:
                    nid = l.node_id or l.node_name or "unknown"
                    runtime_map[nid] = {
                        "status": (
                            "failed"
                            if l.exception
                            else "success"
                            if (l.message and "success" in l.message)
                            else "running"
                            if (l.message and "running" in l.message)
                            else overall_status
                        ),
                        "duration_ms": l.duration_ms,
                        "timestamp": l.timestamp.isoformat() if l.timestamp else None,
                    }

                # Merge static nodes with runtime status
                nodes = []
                for n in dag_data.get("nodes", []):
                    nid = n.get("node_id", "unknown")
                    rt = runtime_map.get(nid, {})
                    nodes.append(
                        {
                            "node_id": nid,
                            "node_name": n.get("node_name", nid),
                            "status": rt.get("status", overall_status),
                            "duration_ms": rt.get("duration_ms"),
                            "timestamp": rt.get("timestamp"),
                        }
                    )

                # Use real topological edges
                edges = []
                for e in dag_data.get("edges", []):
                    edges.append(
                        {
                            "from": e.get("from_node_id", ""),
                            "to": e.get("to_node_id", ""),
                            "type": e.get("edge_type", "data_flow"),
                        }
                    )

                return {"run_id": run_id, "nodes": nodes, "edges": edges}

    # ------------------------------------------------------------------ #
    # Fallback: old chronological-chain logic
    # ------------------------------------------------------------------ #
    logs = (
        session.query(NodeLogModel)
        .filter_by(run_id=run_id)
        .order_by(NodeLogModel.timestamp.asc())
        .all()
    )

    node_map: Dict[str, Dict[str, Any]] = {}
    for l in logs:
        nid = l.node_id or l.node_name or "unknown"
        if nid not in node_map:
            node_map[nid] = {
                "node_id": nid,
                "node_name": l.node_name or nid,
                "status": overall_status,
                "duration_ms": None,
                "timestamp": None,
            }

    nodes = list(node_map.values())

    # Fallback: if no node_logs, create a virtual node from execution_logs
    if not nodes:
        if exec_log:
            wf = (
                session.query(WorkflowModel)
                .filter_by(id=exec_log.workflow_id)
                .first()
            )
            wf_name = wf.workflow_name if wf else (exec_log.workflow_id or "workflow")
            nodes = [
                {
                    "node_id": run_id[:8],
                    "node_name": wf_name,
                    "status": overall_status,
                    "duration_ms": None,
                    "timestamp": None,
                }
            ]

    # Build chronological edges (simple chain based on node order)
    edges: List[Dict[str, str]] = []
    for i in range(len(nodes) - 1):
        edges.append(
            {
                "from": nodes[i]["node_id"],
                "to": nodes[i + 1]["node_id"],
                "type": "data_flow",
            }
        )

    return {"run_id": run_id, "nodes": nodes, "edges": edges}


# --------------------------------------------------------------------------- #
# Logs
# --------------------------------------------------------------------------- #

@router.get("/api/logs")
def query_logs(
    workflow_id: Optional[str] = Query(None),
    node_id: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """Query node logs with optional filters."""
    query = session.query(NodeLogModel)
    if workflow_id:
        query = query.filter_by(workflow_id=workflow_id)
    if node_id:
        query = query.filter_by(node_id=node_id)
    if level:
        query = query.filter_by(level=level)
    logs = query.order_by(NodeLogModel.timestamp.desc()).limit(limit).all()
    return [
        {
            "id": l.id,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
            "workflow_id": l.workflow_id,
            "workflow_name": l.workflow_name,
            "node_id": l.node_id,
            "node_name": l.node_name,
            "log_type": l.log_type,
            "level": l.level,
            "message": l.message,
            "metric_name": l.metric_name,
            "metric_value": l.metric_value,
            "trace_id": l.trace_id,
        }
        for l in logs
    ]


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #

@router.get("/api/metrics")
def query_metrics(
    workflow_id: Optional[str] = Query(None),
    metric_name: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """Query metric entries from node logs."""
    query = session.query(NodeLogModel).filter(NodeLogModel.metric_name.isnot(None))
    if workflow_id:
        query = query.filter_by(workflow_id=workflow_id)
    if metric_name:
        query = query.filter_by(metric_name=metric_name)
    logs = query.order_by(NodeLogModel.timestamp.desc()).limit(limit).all()
    return [
        {
            "id": l.id,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
            "workflow_id": l.workflow_id,
            "workflow_name": l.workflow_name,
            "node_id": l.node_id,
            "node_name": l.node_name,
            "metric_name": l.metric_name,
            "metric_value": l.metric_value,
            "trace_id": l.trace_id,
        }
        for l in logs
    ]


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #

@router.get("/api/lineage/{artifact_id}")
def get_lineage(
    artifact_id: str,
    session: Session = Depends(get_db_session),
) -> Dict[str, Any]:
    """Get lineage graph for an artifact."""
    artifact = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
    if not artifact:
        raise HTTPException(status_code=404, detail="Artifact not found")

    edges = (
        session.query(LineageEdgeModel)
        .filter(
            (LineageEdgeModel.from_artifact_id == artifact_id)
            | (LineageEdgeModel.to_artifact_id == artifact_id)
        )
        .all()
    )

    return {
        "artifact_id": artifact_id,
        "artifact": artifact.to_dict(),
        "edges": [
            {
                "edge_id": e.edge_id,
                "from_artifact_id": e.from_artifact_id,
                "to_artifact_id": e.to_artifact_id,
                "edge_type": e.edge_type,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in edges
        ],
    }


@router.get("/api/lineage")
def list_lineage(
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """List all lineage edges."""
    edges = session.query(LineageEdgeModel).order_by(LineageEdgeModel.created_at.desc()).limit(limit).all()
    return [
        {"source": e.from_artifact_id, "target": e.to_artifact_id, "type": e.edge_type}
        for e in edges
    ]


# --------------------------------------------------------------------------- #
# Artifacts (Models)
# --------------------------------------------------------------------------- #

@router.get("/api/artifacts")
def list_artifacts(
    artifact_type: Optional[str] = Query(None, description="Filter by artifact type (model, data, feature)"),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """List all artifacts (every version, not deduplicated)."""
    from collections import OrderedDict

    # unwrap FastAPI Query default when called directly in tests
    if hasattr(artifact_type, "default"):
        artifact_type = artifact_type.default
    if hasattr(limit, "default"):
        limit = limit.default

    query = session.query(ArtifactModel).order_by(ArtifactModel.created_at.desc())
    if artifact_type:
        query = query.filter_by(artifact_type=artifact_type)
    models = query.limit(limit).all()

    result = []
    for m in models:
        d = m.to_dict()
        d["id"] = d.pop("artifact_id", m.artifact_id)
        latest_version = (
            session.query(ArtifactVersionModel)
            .filter_by(artifact_id=m.artifact_id)
            .order_by(ArtifactVersionModel.created_at.desc())
            .first()
        )
        d["stage"] = latest_version.stage if latest_version else "development"
        result.append(d)
    return result


@router.get("/api/models")
def list_models_compat(
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """Backward-compatible alias for /api/artifacts?artifact_type=model."""
    return list_artifacts(artifact_type="model", limit=limit, session=session)


# CLI/import backward compatibility
list_models = list_artifacts


@router.get("/api/artifacts/{name}/versions")
def list_artifact_versions(
    name: str,
    session: Session = Depends(get_db_session),
) -> List[Dict[str, Any]]:
    """List all versions of a named artifact."""
    artifacts = (
        session.query(ArtifactModel)
        .filter_by(name=name)
        .order_by(ArtifactModel.created_at.desc())
        .all()
    )
    result = []
    for m in artifacts:
        d = m.to_dict()
        d["id"] = d.pop("artifact_id", m.artifact_id)
        latest_version = (
            session.query(ArtifactVersionModel)
            .filter_by(artifact_id=m.artifact_id)
            .order_by(ArtifactVersionModel.created_at.desc())
            .first()
        )
        d["stage"] = latest_version.stage if latest_version else "development"
        result.append(d)
    return result


@router.post("/api/models/{artifact_id}/promote")
def promote_model(
    artifact_id: str,
    req: PromoteRequest,
    session: Session = Depends(get_db_session),
) -> PromoteResponse:
    """Promote a model to a new stage."""
    artifact = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
    if not artifact:
        raise HTTPException(status_code=404, detail="Artifact not found")

    version = ArtifactVersionModel(
        artifact_id=artifact_id,
        version=artifact.version,
        storage_key=artifact.storage_key,
        checksum=artifact.checksum,
        stage=req.stage,
        promotion_note=req.promotion_note,
        created_at=datetime.now(),
    )
    session.add(version)
    session.commit()

    return PromoteResponse(
        version_id=str(version.version_id),
        artifact_id=artifact_id,
        stage=req.stage,
    )


@router.post("/api/models/{artifact_id}/rollback")
def rollback_model(
    artifact_id: str,
    session: Session = Depends(get_db_session),
) -> PromoteResponse:
    """Rollback a model to its previous stage (fallback to 'development')."""
    artifact = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
    if not artifact:
        raise HTTPException(status_code=404, detail="Artifact not found")
    versions = (
        session.query(ArtifactVersionModel)
        .filter_by(artifact_id=artifact_id)
        .order_by(ArtifactVersionModel.created_at.desc())
        .limit(2)
        .all()
    )
    if len(versions) < 2:
        # Align with CLI behaviour: fallback to development when no history exists
        prev_stage = "development"
        prev_storage_key = artifact.storage_key
        prev_checksum = artifact.checksum
    else:
        prev_stage = versions[1].stage
        prev_storage_key = versions[1].storage_key
        prev_checksum = versions[1].checksum

    current_stage = versions[0].stage if versions else "development"
    new_version = ArtifactVersionModel(
        artifact_id=artifact_id,
        version=artifact.version,
        storage_key=prev_storage_key,
        checksum=prev_checksum,
        stage=prev_stage,
        promotion_note=f"Rolled back from {current_stage} to {prev_stage}",
        created_at=datetime.now(),
    )
    session.add(new_version)
    # Also update the artifact tags so list shows the current stage
    import json
    tags = json.loads(artifact.tags_json or "{}")
    tags["stage"] = prev_stage
    artifact.tags_json = json.dumps(tags)
    session.commit()
    return PromoteResponse(
        version_id=str(new_version.version_id),
        artifact_id=artifact_id,
        stage=prev_stage,
    )


# --------------------------------------------------------------------------- #
# System Status
# --------------------------------------------------------------------------- #

@router.get("/api/status")
def get_status() -> StatusResponse:
    """Return system status (DB, Prefect, etc.)."""
    # Database
    try:
        db_manager = get_db_manager()
        from sqlalchemy import text
        with db_manager.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as exc:
        logger.warning(f"Database status check failed: {exc}")
        db_status = "disconnected"

    # Prefect
    prefect_status = "not_installed"
    try:
        from pangflow.prefect_integration.client import PrefectClient

        client = PrefectClient()
        if client.health_check():
            prefect_status = "connected"
        else:
            prefect_status = "unreachable"
    except ImportError:
        prefect_status = "not_installed"
    except Exception as exc:
        logger.warning(f"Prefect status check failed: {exc}")
        prefect_status = "error"

    return StatusResponse(
        database=db_status,
        prefect=prefect_status,
        timestamp=datetime.now().isoformat(),
    )


@router.get("/api/settings")
def get_settings() -> Dict[str, Any]:
    """Return system settings for the WebUI."""
    from pangflow import __version__
    from pangflow.core.config import get_param

    db_url = "sqlite:///~/.pangflow/pangflow.db"
    try:
        db_manager = get_db_manager()
        db_url = db_manager.database_url
    except Exception:
        pass

    return {
        "version": __version__,
        "environment": "pangflow",
        "database_url": db_url,
        "log_level": get_param("log.level", "INFO"),
    }
