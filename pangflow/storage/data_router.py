# -*- coding: utf-8 -*-
"""
Data routing logic based on payload size.
"""

import pickle
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Optional


class DataRouter:
    """Routes data to the appropriate storage strategy based on size."""

    def __init__(
        self,
        small_threshold: int = 1 * 1024 * 1024,      # 1 MB
        medium_threshold: int = 100 * 1024 * 1024,   # 100 MB
    ):
        self.small_threshold = small_threshold
        self.medium_threshold = medium_threshold

    def route(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Return a routing decision dict.

        Strategies:
        - ``direct``  – small enough to pass through in-memory.
        - ``temp_file`` – medium size; write to a temp file.
        - ``feature_store`` – large; route to long-term feature storage.
        """
        metadata = metadata or {}
        serialized = self._serialize(data)
        size = len(serialized)

        if size < self.small_threshold:
            return {"strategy": "direct", "data": serialized}

        if size < self.medium_threshold:
            run_id = metadata.get("run_id", "default")
            node_id = metadata.get("node_id", "default")
            tmp_dir = Path(tempfile.gettempdir()) / "pangflow" / run_id
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = tmp_dir / f"{node_id}.bin"
            tmp_path.write_bytes(serialized)
            return {"strategy": "temp_file", "path": str(tmp_path), "data": serialized}

        # Large data
        name = metadata.get("name", "large_data")
        key = f"features/{name}/{uuid.uuid4()}.bin"
        return {"strategy": "feature_store", "key": key, "data": serialized}

    @staticmethod
    def _serialize(data: Any) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        return pickle.dumps(data)
