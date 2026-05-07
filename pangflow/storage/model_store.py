# -*- coding: utf-8 -*-
"""
ModelStore – saves/loads model objects with versioning and promotion.
"""

import hashlib
import pickle
from typing import Any, Dict, List, Optional

from pangflow.database.connection import get_db_manager, initialize_database
from pangflow.database.models import ArtifactVersionModel
from pangflow.observer.subject import get_subject
from pangflow.storage.backend import StorageBackend
from pangflow.storage.meta_store import MetaStore


class ModelStore:
    """Manages model artifacts on a file backend with MetaStore tracking."""

    def __init__(self, file_backend: StorageBackend, meta_store: MetaStore):
        self.file_backend = file_backend
        self.meta_store = meta_store
        self._subject = get_subject()

    def save_model(
        self,
        model: Any,
        metadata: Optional[Dict[str, Any]] = None,
        upstream_artifact_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Serialize *model*, persist it, register metadata, and return handles."""
        metadata = metadata or {}
        name = metadata.get("name", "model")
        version = metadata.get("version", "1.0.0")
        data = pickle.dumps(model)
        checksum = hashlib.sha256(data).hexdigest()
        key = f"models/{name}/{version}.pkl"
        storage_key = self.file_backend.write(key, data, metadata)

        artifact_id = self.meta_store.register_artifact({
            "workflow_id": metadata.get("workflow_id", ""),
            "node_id": metadata.get("node_id", ""),
            "artifact_type": "model",
            "name": name,
            "version": version,
            "storage_backend": "local",
            "storage_key": storage_key,
            "checksum": checksum,
            "size_bytes": len(data),
            "created_by": metadata.get("created_by"),
            "tags": metadata.get("tags", {}),
            "lifecycle": metadata.get("lifecycle", "hot"),
        })

        self._subject.publish("MODEL_SAVED", {
            "artifact_id": artifact_id,
            "name": name,
            "version": version,
            "workflow_id": metadata.get("workflow_id"),
            "node_id": metadata.get("node_id"),
            "upstream_artifact_ids": upstream_artifact_ids or [],
        })
        return {"artifact_id": artifact_id, "storage_key": storage_key, "checksum": checksum}

    def load_model(self, name: str, stage: Optional[str] = None) -> Any:
        """Load the latest model called *name*, optionally filtered by *stage*."""
        artifacts = self.meta_store.list_artifacts({"name": name})
        if not artifacts:
            raise FileNotFoundError(f"Model '{name}' not found")
        # If stage is requested, filter by tag; otherwise take the latest.
        if stage:
            matched = [a for a in artifacts if (a.get("tags") or {}).get("stage") == stage]
            if matched:
                artifact = matched[-1]
            else:
                artifact = artifacts[-1]
        else:
            artifact = artifacts[-1]
        data = self.file_backend.read(artifact["storage_key"])
        return pickle.loads(data)

    def list_versions(self, name: str) -> List[Dict[str, Any]]:
        """Return metadata for every registered version of *name*."""
        return self.meta_store.list_artifacts({"name": name})

    def promote_version(
        self,
        artifact_id: str,
        stage: str,
        note: Optional[str] = None,
        promoted_by: Optional[str] = None,
    ) -> None:
        """Promote an artifact to a new *stage* (e.g. ``staging``, ``production``)."""
        import json
        db = get_db_manager()
        artifact = self.meta_store.get_artifact(artifact_id)
        if artifact is None:
            raise ValueError(f"Artifact {artifact_id} not found")

        with db.get_session() as session:
            version = ArtifactVersionModel(
                artifact_id=artifact_id,
                version=artifact["version"],
                storage_key=artifact["storage_key"],
                checksum=artifact.get("checksum"),
                promoted_by=promoted_by,
                promotion_note=note or "",
                stage=stage,
            )
            session.add(version)

            # Also update the artifact's tags so model list shows the current stage
            from pangflow.database.models import ArtifactModel

            art_model = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
            if art_model:
                tags = json.loads(art_model.tags_json or "{}")
                tags["stage"] = stage
                art_model.tags_json = json.dumps(tags)

        self._subject.publish("MODEL_PROMOTED", {
            "artifact_id": artifact_id,
            "stage": stage,
        })


# --------------------------------------------------------------------------- #
# Module-level convenience API
# --------------------------------------------------------------------------- #

_DEFAULT_STORE: Optional[ModelStore] = None


def _default_store() -> ModelStore:
    """Return a lazily-initialized default ModelStore using local file backend."""
    global _DEFAULT_STORE
    if _DEFAULT_STORE is None:
        from pathlib import Path
        from pangflow.storage.backend import LocalFileBackend
        from pangflow.utils.workspace import find_workspace

        # Prefer workspace-local DB over global ~/.pangflow
        workspace_path = find_workspace()
        if workspace_path is not None:
            db_path = workspace_path / "pangflow.db"
            models_path = workspace_path / "data" / "models"
        else:
            db_path = Path.home() / ".pangflow" / "pangflow.db"
            models_path = Path.home() / ".pangflow" / "data" / "models"

        db_url = f"sqlite:///{db_path}"
        try:
            db_manager = get_db_manager()
            # If existing manager points to a different DB, re-initialize
            if db_manager.database_url != db_url:
                db_manager = initialize_database(db_url)
        except RuntimeError:
            db_manager = initialize_database(db_url)

        backend = LocalFileBackend(str(models_path))
        _DEFAULT_STORE = ModelStore(backend, MetaStore())
    return _DEFAULT_STORE


def save_model(
    name: str,
    model: Any,
    metadata: Optional[Dict[str, Any]] = None,
    upstream_artifact_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Save *model* under *name* using the default ModelStore.

    Parameters
    ----------
    name : str
        Artifact name (e.g. ``"iris_model"``).
    model : Any
        Pickle-serializable Python object.
    metadata : dict, optional
        Extra metadata such as ``accuracy``, ``version``, ``tags``.
    upstream_artifact_ids : list, optional
        Artifact IDs that this model depends on (for lineage tracking).
    """
    meta = dict(metadata or {})
    meta["name"] = name
    return _default_store().save_model(
        model, metadata=meta, upstream_artifact_ids=upstream_artifact_ids
    )


def load_model(name: str, stage: Optional[str] = None) -> Any:
    """Load the latest model called *name*, optionally filtered by *stage*.

    Parameters
    ----------
    name : str
        Artifact name registered via :func:`save_model`.
    stage : str, optional
        Promotion stage filter (e.g. ``"production"``).
    """
    return _default_store().load_model(name, stage=stage)
