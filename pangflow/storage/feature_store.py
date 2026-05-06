# -*- coding: utf-8 -*-
"""
FeatureStore – persistent feature storage with TTL and cache helpers.
"""

import hashlib
import json
import pickle
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from pangflow.database.connection import get_db_manager
from pangflow.database.models import FeatureModel
from pangflow.storage.backend import StorageBackend
from pangflow.storage.meta_store import MetaStore


class FeatureStore:
    """Manages feature artifacts on a file backend with MetaStore tracking."""

    def __init__(self, file_backend: StorageBackend, meta_store: MetaStore):
        self.file_backend = file_backend
        self.meta_store = meta_store

    def save_feature(self, features: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Persist a feature set and return its handles."""
        metadata = metadata or {}
        name = metadata.get("name", "feature")
        partition = metadata.get("partition_key", "default")
        data = pickle.dumps(features)
        checksum = hashlib.sha256(data).hexdigest()
        key = f"features/{name}/{partition}.pkl"
        storage_key = self.file_backend.write(key, data, metadata)

        artifact_id = self.meta_store.register_artifact({
            "workflow_id": metadata.get("workflow_id", ""),
            "node_id": metadata.get("node_id", ""),
            "artifact_type": "feature",
            "name": name,
            "version": metadata.get("version", "1.0.0"),
            "storage_backend": "local",
            "storage_key": storage_key,
            "checksum": checksum,
            "size_bytes": len(data),
            "created_by": metadata.get("created_by"),
            "tags": metadata.get("tags", {}),
            "lifecycle": metadata.get("lifecycle", "hot"),
        })

        db = get_db_manager()
        with db.get_session() as session:
            ttl_days = metadata.get("ttl_days")
            ttl_expires = None
            if ttl_days:
                ttl_expires = datetime.now() + timedelta(days=ttl_days)
            feature = FeatureModel(
                artifact_id=artifact_id,
                feature_name=name,
                schema_json=json.dumps(metadata.get("schema", {})),
                partition_key=partition,
                ttl_expires_at=ttl_expires,
                upstream_artifacts=json.dumps(metadata.get("upstream", [])),
            )
            session.add(feature)

        return {"artifact_id": artifact_id, "storage_key": storage_key, "checksum": checksum}

    def load_feature(self, name: str, partition: Optional[str] = None) -> Any:
        """Load the most recent feature set called *name*."""
        db = get_db_manager()
        with db.get_session() as session:
            query = session.query(FeatureModel).filter_by(feature_name=name)
            if partition:
                query = query.filter_by(partition_key=partition)
            feature = query.order_by(FeatureModel.created_at.desc()).first()
            if feature is None:
                raise FileNotFoundError(f"Feature '{name}' not found")
            artifact = self.meta_store.get_artifact(feature.artifact_id)
            if artifact is None:
                raise FileNotFoundError(f"Artifact for feature '{name}' not found")
            data = self.file_backend.read(artifact["storage_key"])
            return pickle.loads(data)

    def check_cache(self, name: str, partition: Optional[str] = None) -> bool:
        """Return True if a non-expired feature exists on disk."""
        db = get_db_manager()
        with db.get_session() as session:
            query = session.query(FeatureModel).filter_by(feature_name=name)
            if partition:
                query = query.filter_by(partition_key=partition)
            feature = query.order_by(FeatureModel.created_at.desc()).first()
            if feature is None:
                return False
            if feature.ttl_expires_at and feature.ttl_expires_at < datetime.now():
                return False
            artifact = self.meta_store.get_artifact(feature.artifact_id)
            if artifact is None:
                return False
            return self.file_backend.exists(artifact["storage_key"])

    def invalidate_cache(self, name: str) -> bool:
        """Remove all cached entries for *name* from the backend and ORM."""
        db = get_db_manager()
        with db.get_session() as session:
            features = session.query(FeatureModel).filter_by(feature_name=name).all()
            if not features:
                return False
            for feature in features:
                artifact = self.meta_store.get_artifact(feature.artifact_id)
                if artifact:
                    try:
                        self.file_backend.delete(artifact["storage_key"])
                    except Exception:
                        pass
                session.delete(feature)
            return True


def load_feature(name: str, partition: Optional[str] = None) -> Any:
    """Stub – requires a :class:`FeatureStore` instance to load features."""
    raise NotImplementedError("FeatureStore instance required to load features")
