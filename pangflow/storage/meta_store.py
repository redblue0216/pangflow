# -*- coding: utf-8 -*-
"""
MetaStore – manages artifact metadata, lineage and versions via ORM.
"""

import json
from typing import Any, Dict, List, Optional

from pangflow.database.connection import get_db_manager
from pangflow.database.models import ArtifactModel, LineageEdgeModel
from pangflow.observer.subject import get_subject


class MetaStore:
    """Central metadata registry backed by the pangflow ORM/SQLite database."""

    def __init__(self):
        self._subject = get_subject()

    def register_artifact(self, artifact_data: Dict[str, Any]) -> str:
        """Register a new artifact and return its ``artifact_id``."""
        db = get_db_manager()
        with db.get_session() as session:
            artifact = ArtifactModel(
                workflow_id=artifact_data.get("workflow_id") or None,
                node_id=artifact_data.get("node_id", ""),
                artifact_type=artifact_data.get("artifact_type", "generic"),
                name=artifact_data.get("name", "untitled"),
                version=artifact_data.get("version", "1.0.0"),
                storage_backend=artifact_data.get("storage_backend", "sqlite"),
                storage_key=artifact_data.get("storage_key", ""),
                checksum=artifact_data.get("checksum"),
                size_bytes=artifact_data.get("size_bytes"),
                created_by=artifact_data.get("created_by"),
                lineage_json=json.dumps(artifact_data.get("lineage", [])),
                tags_json=json.dumps(artifact_data.get("tags", {})),
                lifecycle=artifact_data.get("lifecycle", "hot"),
            )
            session.add(artifact)
            session.flush()
            artifact_id = artifact.artifact_id
            artifact_type = artifact.artifact_type
            version = artifact.version
            storage_key = artifact.storage_key

        self._subject.publish("ARTIFACT_REGISTERED", {
            "artifact_id": artifact_id,
            "type": artifact_type,
            "version": version,
            "storage_key": storage_key,
        })
        return artifact_id

    def get_artifact(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single artifact by id."""
        db = get_db_manager()
        with db.get_session() as session:
            artifact = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
            return artifact.to_dict() if artifact else None

    def list_artifacts(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """List artifacts, optionally filtered."""
        db = get_db_manager()
        with db.get_session() as session:
            query = session.query(ArtifactModel)
            if filters:
                if "workflow_id" in filters:
                    query = query.filter_by(workflow_id=filters["workflow_id"])
                if "name" in filters:
                    query = query.filter_by(name=filters["name"])
                if "artifact_type" in filters:
                    query = query.filter_by(artifact_type=filters["artifact_type"])
            artifacts = query.all()
            return [a.to_dict() for a in artifacts]

    def update_lineage(self, artifact_id: str, parents: List[str]) -> None:
        """Record that *artifact_id* was derived from *parents*."""
        db = get_db_manager()
        with db.get_session() as session:
            artifact = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
            if artifact is None:
                return
            lineage = json.loads(artifact.lineage_json or "[]")
            for parent_id in parents:
                if parent_id not in lineage:
                    lineage.append(parent_id)
                edge = LineageEdgeModel(
                    from_artifact_id=parent_id,
                    to_artifact_id=artifact_id,
                    edge_type="data_flow",
                )
                session.add(edge)
            artifact.lineage_json = json.dumps(lineage)

    def get_lineage(self, artifact_id: str) -> Dict[str, Any]:
        """Return all lineage edges connected to *artifact_id*."""
        db = get_db_manager()
        with db.get_session() as session:
            edges = session.query(LineageEdgeModel).filter(
                (LineageEdgeModel.from_artifact_id == artifact_id)
                | (LineageEdgeModel.to_artifact_id == artifact_id)
            ).all()
            return {
                "artifact_id": artifact_id,
                "edges": [
                    {
                        "edge_id": e.edge_id,
                        "from": e.from_artifact_id,
                        "to": e.to_artifact_id,
                        "edge_type": e.edge_type,
                    }
                    for e in edges
                ],
            }
