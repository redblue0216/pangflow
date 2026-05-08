# -*- coding: utf-8 -*-
"""
PangFlow v0.2.12 – SQLAlchemy ORM models.

Covers workflows, execution logs, artifacts, lineage, features, environments,
services, traces and node-level logs.
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, String, DateTime, Boolean, Text, ForeignKey, Integer,
    Float, CheckConstraint, JSON
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


def _uuid() -> str:
    return str(uuid.uuid4())


# --------------------------------------------------------------------------- #
# Workflow & Execution
# --------------------------------------------------------------------------- #

class WorkflowModel(Base):
    __tablename__ = "workflows"

    id = Column(String(36), primary_key=True, default=_uuid)
    workflow_name = Column(String(255), nullable=False, index=True)
    package_name = Column(String(100), nullable=False, index=True)
    version = Column(String(20), default="1.0.0")
    description = Column(Text, nullable=True)
    workflow_type = Column(String(20), nullable=False, default="trigger")
    command = Column(Text, nullable=True)
    working_dir = Column(String(500), nullable=True)
    status = Column(String(20), nullable=False, default="created")
    is_deployed = Column(Boolean, nullable=False, default=False)
    deployment_id = Column(String(100), nullable=True)
    flow_id = Column(String(100), nullable=True)
    schedule_config = Column(Text, nullable=True)
    serve_pid = Column(Integer, nullable=True)
    serve_status = Column(String(20), nullable=True)
    flow_file_path = Column(String(500), nullable=True)
    prefect_serve_pid = Column(Integer, nullable=True)
    prefect_serve_status = Column(String(20), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    execution_logs = relationship(
        "ExecutionLogModel", back_populates="workflow",
        cascade="all, delete-orphan", order_by="desc(ExecutionLogModel.started_at)"
    )
    artifacts = relationship(
        "ArtifactModel", back_populates="workflow", cascade="all, delete-orphan"
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "workflow_name": self.workflow_name,
            "package_name": self.package_name,
            "version": self.version,
            "description": self.description,
            "workflow_type": self.workflow_type,
            "command": self.command,
            "working_dir": self.working_dir,
            "status": self.status,
            "is_deployed": self.is_deployed,
            "deployment_id": self.deployment_id,
            "flow_id": self.flow_id,
            "schedule_config": self.schedule_config,
            "serve_pid": self.serve_pid,
            "serve_status": self.serve_status,
            "flow_file_path": self.flow_file_path,
            "prefect_serve_pid": self.prefect_serve_pid,
            "prefect_serve_status": self.prefect_serve_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class ExecutionLogModel(Base):
    __tablename__ = "execution_logs"

    id = Column(String(36), primary_key=True, default=_uuid)
    workflow_id = Column(String(36), ForeignKey("workflows.id"), nullable=True, index=True)
    run_id = Column(String(36), nullable=False, index=True)
    execution_type = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False)
    return_code = Column(Integer, nullable=True)
    stdout = Column(Text, nullable=True)
    stderr = Column(Text, nullable=True)
    triggered_by = Column(String(50), nullable=True)
    trigger_source = Column(String(100), nullable=True)
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    completed_at = Column(DateTime, nullable=True)
    metadata_json = Column(Text, nullable=True)

    workflow = relationship("WorkflowModel", back_populates="execution_logs")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "execution_type": self.execution_type,
            "status": self.status,
            "return_code": self.return_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "triggered_by": self.triggered_by,
            "trigger_source": self.trigger_source,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metadata_json": self.metadata_json,
        }


# --------------------------------------------------------------------------- #
# Artifacts & Lineage
# --------------------------------------------------------------------------- #

class ArtifactModel(Base):
    __tablename__ = "artifacts"

    artifact_id = Column(String(36), primary_key=True, default=_uuid)
    workflow_id = Column(String(36), ForeignKey("workflows.id"), nullable=True, index=True)
    node_id = Column(String(36), nullable=False, index=True)
    artifact_type = Column(String(50), nullable=False)
    name = Column(String(255), nullable=False, index=True)
    version = Column(String(50), nullable=False, default="1.0.0")
    storage_backend = Column(String(50), default="sqlite")
    storage_key = Column(String(500), nullable=False)
    checksum = Column(String(64), nullable=True)
    size_bytes = Column(Integer, nullable=True)
    created_by = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    lineage_json = Column(Text, nullable=True)
    tags_json = Column(Text, nullable=True)
    lifecycle = Column(String(20), default="hot")

    workflow = relationship("WorkflowModel", back_populates="artifacts")
    versions = relationship(
        "ArtifactVersionModel", back_populates="artifact",
        cascade="all, delete-orphan"
    )

    def to_dict(self) -> dict:
        return {
            "artifact_id": self.artifact_id,
            "workflow_id": self.workflow_id,
            "node_id": self.node_id,
            "artifact_type": self.artifact_type,
            "name": self.name,
            "version": self.version,
            "storage_backend": self.storage_backend,
            "storage_key": self.storage_key,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "lineage": self.lineage_json,
            "tags": self.tags_json,
            "lifecycle": self.lifecycle,
        }


class ArtifactVersionModel(Base):
    __tablename__ = "artifact_versions"

    version_id = Column(String(36), primary_key=True, default=_uuid)
    artifact_id = Column(String(36), ForeignKey("artifacts.artifact_id"), nullable=False)
    version = Column(String(50), nullable=False)
    storage_key = Column(String(500), nullable=False)
    checksum = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    promoted_by = Column(String(100), nullable=True)
    promotion_note = Column(Text, nullable=True)
    stage = Column(String(20), default="development")

    artifact = relationship("ArtifactModel", back_populates="versions")


class LineageEdgeModel(Base):
    __tablename__ = "lineage_edges"

    edge_id = Column(String(36), primary_key=True, default=_uuid)
    from_artifact_id = Column(String(36), ForeignKey("artifacts.artifact_id"), nullable=False)
    to_artifact_id = Column(String(36), ForeignKey("artifacts.artifact_id"), nullable=False)
    edge_type = Column(String(50), default="data_flow")
    created_at = Column(DateTime, nullable=False, default=datetime.now)


# --------------------------------------------------------------------------- #
# Features
# --------------------------------------------------------------------------- #

class FeatureModel(Base):
    __tablename__ = "features"

    feature_id = Column(String(36), primary_key=True, default=_uuid)
    artifact_id = Column(String(36), ForeignKey("artifacts.artifact_id"), nullable=False)
    feature_name = Column(String(255), nullable=False, index=True)
    schema_json = Column(Text, nullable=True)
    partition_key = Column(String(255), nullable=True)
    ttl_expires_at = Column(DateTime, nullable=True)
    upstream_artifacts = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)


# --------------------------------------------------------------------------- #
# Environments
# --------------------------------------------------------------------------- #

class EnvironmentModel(Base):
    __tablename__ = "environments"

    env_id = Column(String(36), primary_key=True, default=_uuid)
    workflow_id = Column(String(36), nullable=True, index=True)
    name = Column(String(100), nullable=False, index=True)
    python_version = Column(String(20), nullable=True)
    conda_prefix = Column(String(500), nullable=True)
    conda_spec_json = Column(Text, nullable=True)
    pip_spec_json = Column(Text, nullable=True)
    status = Column(String(20), default="NotExists")
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)


# --------------------------------------------------------------------------- #
# Services & Traces
# --------------------------------------------------------------------------- #

class ServiceModel(Base):
    __tablename__ = "services"

    service_id = Column(String(36), primary_key=True, default=_uuid)
    workflow_id = Column(String(36), nullable=True, index=True)
    service_name = Column(String(255), nullable=False)
    status = Column(String(20), default="stopped")
    host = Column(String(100), default="127.0.0.1")
    port = Column(Integer, default=8000)
    pid = Column(Integer, nullable=True)
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)


class TraceModel(Base):
    __tablename__ = "traces"

    trace_id = Column(String(36), primary_key=True, default=_uuid)
    endpoint = Column(String(255), nullable=False)
    workflow_id = Column(String(36), nullable=True)
    service_id = Column(String(36), nullable=True)
    method = Column(String(10), nullable=False)
    status_code = Column(Integer, nullable=True)
    latency_ms = Column(Float, nullable=True)
    request_json = Column(Text, nullable=True)
    response_json = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)


# --------------------------------------------------------------------------- #
# Node Logs
# --------------------------------------------------------------------------- #

class NodeLogModel(Base):
    __tablename__ = "node_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now, index=True)
    workflow_id = Column(String(36), nullable=True, index=True)
    workflow_name = Column(String(255), nullable=True)
    node_id = Column(String(36), nullable=True, index=True)
    node_name = Column(String(255), nullable=True)
    log_type = Column(
        String(20),
        CheckConstraint("log_type IN ('auto', 'manual', 'metric')"),
        default="auto",
    )
    level = Column(String(20), nullable=True)
    message = Column(Text, nullable=True)
    extra_json = Column(Text, nullable=True)
    inputs_hash = Column(String(64), nullable=True)
    outputs_hash = Column(String(64), nullable=True)
    duration_ms = Column(Float, nullable=True)
    exception = Column(Text, nullable=True)
    metric_name = Column(String(100), nullable=True)
    metric_value = Column(Float, nullable=True)
    trace_id = Column(String(36), nullable=True)
    run_id = Column(String(36), nullable=True, index=True)
    storage_backend = Column(String(50), default="sqlite")
