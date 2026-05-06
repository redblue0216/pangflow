# -*- coding: utf-8 -*-
"""
StorageManager – holds backends and routes data.
"""

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from pangflow.storage.backend import StorageBackend
from pangflow.storage.data_router import DataRouter


@dataclass
class StorageKey:
    backend_name: str
    key: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class StorageManager:
    """Central registry of storage backends and data router."""

    def __init__(self, default_backend: Optional[StorageBackend] = None):
        self.default_backend = default_backend
        self.backends: Dict[str, StorageBackend] = {}
        if default_backend is not None:
            self.backends["default"] = default_backend

    def register_backend(self, name: str, backend: StorageBackend) -> None:
        self.backends[name] = backend
        backend.connect()

    def get_backend(self, name: str) -> StorageBackend:
        if name not in self.backends:
            raise KeyError(f"Backend '{name}' not found")
        return self.backends[name]

    def route_data(
        self,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
    ) -> StorageKey:
        """Serialize *data*, pick a backend, write it, and return a ``StorageKey``."""
        metadata = metadata or {}
        decision = DataRouter().route(data, metadata)
        strategy = decision.get("strategy", "direct")

        if strategy == "direct":
            backend_name = hint or "default"
            backend = self.get_backend(backend_name)
            key = f"direct/{uuid.uuid4()}"
            actual_key = backend.write(key, decision["data"], metadata)
            return StorageKey(backend_name=backend_name, key=actual_key, metadata=metadata)

        if strategy == "temp_file":
            backend_name = hint or "local"
            backend = self.get_backend(backend_name)
            key = decision.get("path", f"temp/{uuid.uuid4()}")
            actual_key = backend.write(key, decision["data"], metadata)
            return StorageKey(backend_name=backend_name, key=actual_key, metadata=metadata)

        # feature_store
        backend_name = hint or "default"
        backend = self.get_backend(backend_name)
        key = decision.get("key", f"features/{uuid.uuid4()}")
        actual_key = backend.write(key, decision["data"], metadata)
        return StorageKey(backend_name=backend_name, key=actual_key, metadata=metadata)
