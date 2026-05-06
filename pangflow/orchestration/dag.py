# -*- coding: utf-8 -*-
"""
DAGBuilder + Edge – graph construction, validation, and topological sort.
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pangflow.orchestration.registry import NodeMetadata

logger = logging.getLogger(__name__)


@dataclass
class Edge:
    from_node_id: str
    to_node_id: str
    param_mapping: Optional[Dict[str, str]] = None
    edge_type: str = "default"


class CyclicDependencyError(Exception):
    """Raised when a cycle is detected in the DAG."""
    pass


class DAGBuilder:
    """Incremental DAG builder with validation and layered topological sort."""

    def __init__(self) -> None:
        self.nodes: Dict[str, NodeMetadata] = {}
        self.edges: List[Edge] = []

    # ------------------------------------------------------------------ #
    # Mutation
    # ------------------------------------------------------------------ #
    def add_node(self, node: NodeMetadata) -> None:
        """Idempotent node registration."""
        self.nodes[node.node_id] = node

    def add_edge(
        self,
        from_node_id: str,
        to_node_id: str,
        param_mapping: Optional[Dict[str, str]] = None,
        edge_type: str = "default",
    ) -> None:
        """Add an edge unless an identical one already exists."""
        for e in self.edges:
            if (
                e.from_node_id == from_node_id
                and e.to_node_id == to_node_id
                and e.param_mapping == param_mapping
                and e.edge_type == edge_type
            ):
                return
        self.edges.append(
            Edge(from_node_id, to_node_id, param_mapping, edge_type)
        )

    # ------------------------------------------------------------------ #
    # Validation
    # ------------------------------------------------------------------ #
    def validate(self) -> None:
        """Run full validation suite."""
        self._detect_cycles()
        self._check_types()
        self._check_params()

    def _detect_cycles(self) -> None:
        adj: Dict[str, List[str]] = {n: [] for n in self.nodes}
        for e in self.edges:
            adj.setdefault(e.from_node_id, []).append(e.to_node_id)

        WHITE, GRAY, BLACK = 0, 1, 2
        colour = {n: WHITE for n in self.nodes}

        def dfs(node: str, path: List[str]) -> None:
            colour[node] = GRAY
            path.append(node)
            for neighbour in adj.get(node, []):
                if neighbour not in colour:
                    continue
                if colour[neighbour] == GRAY:
                    cycle_start = path.index(neighbour)
                    cycle = path[cycle_start:] + [neighbour]
                    raise CyclicDependencyError(
                        f"Cycle detected: {' -> '.join(cycle)}"
                    )
                if colour[neighbour] == WHITE:
                    dfs(neighbour, path)
            path.pop()
            colour[node] = BLACK

        for node in list(self.nodes.keys()):
            if colour[node] == WHITE:
                dfs(node, [])

    def _check_types(self) -> None:
        for edge in self.edges:
            from_node = self.nodes.get(edge.from_node_id)
            to_node = self.nodes.get(edge.to_node_id)
            if not from_node or not to_node:
                continue

            out_type = from_node.output_type
            if out_type is None or out_type is inspect.Parameter.empty:
                continue

            in_type = None
            if edge.param_mapping:
                for param_name in edge.param_mapping:
                    if param_name.startswith("__pos_"):
                        idx = int(param_name.split("_pos_")[1].strip("_"))
                        params = list(to_node.signature.parameters.values())
                        if idx < len(params):
                            in_type = params[idx].annotation
                    else:
                        param = to_node.signature.parameters.get(param_name)
                        if param:
                            in_type = param.annotation
            else:
                params = list(to_node.signature.parameters.values())
                if params:
                    in_type = params[0].annotation

            if in_type is not None and in_type is not inspect.Parameter.empty:
                try:
                    if not issubclass(out_type, in_type):
                        logger.warning(
                            "Type mismatch on edge %s -> %s: "
                            "output %s is not a subclass of input %s",
                            edge.from_node_id,
                            edge.to_node_id,
                            out_type,
                            in_type,
                        )
                except TypeError:
                    pass  # Generic types, unions, etc.

    def _check_params(self) -> None:
        for node in self.nodes.values():
            required: List[str] = []
            for name, param in node.signature.parameters.items():
                if (
                    param.default is inspect.Parameter.empty
                    and param.kind
                    in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    )
                ):
                    required.append(name)

            provided: set = set()
            for edge in self.edges:
                if edge.to_node_id != node.node_id:
                    continue
                if edge.param_mapping:
                    for param_name in edge.param_mapping:
                        if param_name.startswith("__pos_"):
                            idx = int(param_name.split("_pos_")[1].strip("_"))
                            params = list(node.signature.parameters.values())
                            if idx < len(params):
                                provided.add(params[idx].name)
                        else:
                            provided.add(param_name)
                else:
                    params = list(node.signature.parameters.values())
                    if params:
                        provided.add(params[0].name)

            missing = [p for p in required if p not in provided]
            if missing:
                logger.warning(
                    "Node %s (%s) missing required parameters: %s",
                    node.node_id,
                    node.name,
                    missing,
                )

    # ------------------------------------------------------------------ #
    # Topological sort (layered)
    # ------------------------------------------------------------------ #
    def topological_sort(self) -> List[List[NodeMetadata]]:
        """Return a list of layers; nodes inside a layer may run in parallel."""
        in_degree = {n: 0 for n in self.nodes}
        adj = {n: [] for n in self.nodes}
        for e in self.edges:
            if e.from_node_id in in_degree and e.to_node_id in in_degree:
                in_degree[e.to_node_id] += 1
                adj[e.from_node_id].append(e.to_node_id)

        layers: List[List[NodeMetadata]] = []
        current = [n for n, d in in_degree.items() if d == 0]

        while current:
            layer = [self.nodes[n] for n in current]
            layers.append(layer)
            next_level: List[str] = []
            for n in current:
                for neighbour in adj[n]:
                    in_degree[neighbour] -= 1
                    if in_degree[neighbour] == 0:
                        next_level.append(neighbour)
            current = next_level

        if any(d > 0 for d in in_degree.values()):
            raise CyclicDependencyError("Cycle detected during topological sort")

        return layers

    def get_upstream_edges(self, node_id: str) -> List[Edge]:
        """Return all edges whose destination is *node_id*."""
        return [e for e in self.edges if e.to_node_id == node_id]
