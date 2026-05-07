# -*- coding: utf-8 -*-
"""
LineageObserver – records data-lineage edges in the lineage_edges table.

Reacts to:
    ARTIFACT_REGISTERED – caches the artifact payload for later linking.
    NODE_COMPLETE       – writes edges from input_artifact_ids -> output_artifact_id.

Usage
-----
    from pangflow.observer.lineage_observer import LineageObserver
    obs = LineageObserver()
    obs.start()
"""

import logging
from typing import Any, Dict, List

from pangflow.database.connection import get_db_manager
from pangflow.database.models import LineageEdgeModel
from pangflow.observer.subject import get_subject

logger = logging.getLogger(__name__)


class LineageObserver:
    """Subscriber that persists lineage edges on artifact / node events."""

    def __init__(self, max_pending: int = 1000):
        # Cache of recently seen artifact registrations, keyed by artifact_id.
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._max_pending = max_pending

    # -- callbacks ---------------------------------------------------------- #

    def on_artifact_registered(self, payload: Dict[str, Any]) -> None:
        artifact_id = payload.get("artifact_id")
        if not artifact_id:
            return
        self._pending[artifact_id] = payload
        # Prevent unbounded memory growth
        if len(self._pending) > self._max_pending:
            # Remove oldest entries (simple FIFO)
            to_remove = list(self._pending.keys())[:len(self._pending) - self._max_pending]
            for key in to_remove:
                del self._pending[key]

        # If the payload itself carries upstream links, write them immediately.
        upstream: List[str] = payload.get("upstream_artifact_ids", [])
        if upstream:
            self._write_edges(upstream, artifact_id, payload.get("edge_type", "data_flow"))

    def on_node_complete(self, payload: Dict[str, Any]) -> None:
        output_id = payload.get("output_artifact_id") or payload.get("artifact_id")
        if not output_id:
            return
        # If input_artifact_ids provided directly, use them
        input_ids: List[str] = payload.get("input_artifact_ids", [])
        if input_ids:
            self._write_edges(input_ids, output_id, payload.get("edge_type", "data_flow"))
            return
        # Fallback: try to resolve upstream artifacts from the pending cache
        # by matching workflow_id + node_id of upstream nodes (best-effort)
        upstream_ids = payload.get("upstream_artifact_ids", [])
        if upstream_ids:
            self._write_edges(upstream_ids, output_id, payload.get("edge_type", "data_flow"))

    # -- persistence -------------------------------------------------------- #

    def _write_edges(
        self,
        from_ids: List[str],
        to_id: str,
        edge_type: str,
    ) -> None:
        try:
            db = get_db_manager()
            with db.get_session() as session:
                for from_id in from_ids:
                    if not from_id:
                        continue
                    # Avoid duplicate edges
                    existing = (
                        session.query(LineageEdgeModel)
                        .filter_by(
                            from_artifact_id=from_id,
                            to_artifact_id=to_id,
                            edge_type=edge_type,
                        )
                        .first()
                    )
                    if existing:
                        continue
                    edge = LineageEdgeModel(
                        from_artifact_id=from_id,
                        to_artifact_id=to_id,
                        edge_type=edge_type,
                    )
                    session.add(edge)
        except Exception:
            logger.exception("LineageObserver failed to write lineage edges")

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        get_subject().attach("ARTIFACT_REGISTERED", self.on_artifact_registered)
        get_subject().attach("MODEL_SAVED", self.on_artifact_registered)
        get_subject().attach("NODE_COMPLETE", self.on_node_complete)

    def stop(self) -> None:
        get_subject().detach("ARTIFACT_REGISTERED", self.on_artifact_registered)
        get_subject().detach("MODEL_SAVED", self.on_artifact_registered)
        get_subject().detach("NODE_COMPLETE", self.on_node_complete)
