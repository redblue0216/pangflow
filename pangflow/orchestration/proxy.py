# -*- coding: utf-8 -*-
"""
NodeProxy – overloads ``>>`` to build DAG edges.

Supported syntax
----------------
- ``A >> B``                → A's output as B's first positional arg.
- ``[A, B] >> C``           → A and B parallel, then C.
- ``A >> [B, C]``           → A then B and C parallel.
- ``A >> B(param=A)``       → explicit param mapping (handled via ``__call__``).
"""

from __future__ import annotations

import threading
from typing import Any, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from pangflow.orchestration.dag import DAGBuilder
    from pangflow.orchestration.registry import NodeMetadata


_dag_local = threading.local()


def get_active_dag() -> Optional[DAGBuilder]:
    return getattr(_dag_local, "builder", None)


def set_active_dag(builder: Optional[DAGBuilder]) -> None:
    _dag_local.builder = builder


def clear_active_dag() -> None:
    _dag_local.builder = None


class NodeProxy:
    """Lightweight proxy that overloads ``>>`` and intercepts calls for DAG building."""

    def __init__(self, metadata: NodeMetadata, dag_builder: Optional[DAGBuilder] = None):
        self._meta = metadata
        self._dag = dag_builder

    # ------------------------------------------------------------------ #
    # Introspection helpers
    # ------------------------------------------------------------------ #
    @property
    def node_id(self) -> str:
        return self._meta.node_id

    @property
    def metadata(self) -> NodeMetadata:
        return self._meta

    # ------------------------------------------------------------------ #
    # Call interception
    # ------------------------------------------------------------------ #
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        builder = self._dag or get_active_dag()
        if builder is None:
            # Not inside a @workflow – execute the raw callable.
            return self._meta.func_ref(*args, **kwargs)

        builder.add_node(self._meta)

        # Register edges for every proxy argument.
        for idx, arg in enumerate(args):
            if isinstance(arg, NodeProxy):
                builder.add_edge(
                    arg.node_id,
                    self.node_id,
                    param_mapping={f"__pos_{idx}__": arg.node_id},
                )
        for key, val in kwargs.items():
            if isinstance(val, NodeProxy):
                builder.add_edge(
                    val.node_id,
                    self.node_id,
                    param_mapping={key: val.node_id},
                )

        # Return self so the variable still holds a proxy.
        return self

    # ------------------------------------------------------------------ #
    # ``>>`` overloads
    # ------------------------------------------------------------------ #
    def __rshift__(self, other: Any) -> Any:
        builder = self._dag or get_active_dag()
        if builder is None:
            raise RuntimeError(
                ">> operator can only be used inside a @workflow or with an active DAG builder"
            )

        builder.add_node(self._meta)

        if isinstance(other, NodeProxy):
            builder.add_node(other._meta)
            builder.add_edge(self.node_id, other.node_id)
            return other

        if isinstance(other, list):
            for item in other:
                if isinstance(item, NodeProxy):
                    builder.add_node(item._meta)
                    builder.add_edge(self.node_id, item.node_id)
                else:
                    raise TypeError(f"Cannot connect NodeProxy to {type(item)}")
            return other

        raise TypeError(f"Cannot connect NodeProxy to {type(other)}")

    def __rrshift__(self, other: Any) -> NodeProxy:
        builder = self._dag or get_active_dag()
        if builder is None:
            raise RuntimeError(
                ">> operator can only be used inside a @workflow or with an active DAG builder"
            )

        if isinstance(other, list):
            for item in other:
                if isinstance(item, NodeProxy):
                    builder.add_node(item._meta)
                    builder.add_edge(item.node_id, self.node_id)
                else:
                    raise TypeError(f"Cannot connect {type(item)} to NodeProxy")
            return self

        raise TypeError(f"Cannot connect {type(other)} to NodeProxy")
