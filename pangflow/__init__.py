# -*- coding: utf-8 -*-
# Author: shihua
# License: Apache-2.0
# Copyright (c) 2026 The PangFlow Authors. All rights reserved.

"""
PangFlow - Algorithm OPS Orchestration Tool

A workflow management framework that lets algorithm engineers write pure Python
functions, connect them with the ``>>`` operator, and automatically compile to
Prefect flows and HTTP services.

Public API
----------
- ``@pangflow.node``        : decorate an algorithm function as a workflow node
- ``@pangflow.workflow``    : decorate a workflow orchestration function
- ``@pangflow.serve``       : expose a function as an HTTP endpoint
- ``pangflow.log``          : inject structured logs from inside a node
- ``pangflow.log_metric``   : record business metrics
- ``pangflow.save_model``   : save a model artifact to the registry
- ``pangflow.load_model``   : load a registered model by name / stage
- ``pangflow.load_feature`` : load a registered feature by name / partition
- ``pangflow.get_param``    : retrieve runtime parameters
- ``pangflow.get_trace_id`` : retrieve the current request trace id

Example
-------
>>> import pangflow as pf
>>>
>>> @pf.node(name="train", artifact="model")
>>> def train_model(data: pd.DataFrame) -> GradientBoostingRegressor:
...     pf.log("start training", level="INFO")
...     model = GradientBoostingRegressor()
...     model.fit(data.drop("target", axis=1), data["target"])
...     pf.log_metric("train_score", model.score(...))
...     pf.save_model("my_model", model, metadata={"accuracy": 0.95})
...     return model
>>>
>>> @pf.workflow(name="forecast", schedule="0 * * * *")
>>> def main():
...     data = load_data()
...     model = train_model(data)
...     forecast = generate_forecast(model, data)
...     return forecast
"""

import importlib.metadata as _im
from pathlib import Path

def _read_version() -> str:
    """Read version from pyproject.toml so development never goes stale."""
    try:
        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        import re as _re
        m = _re.search(r'^version\s*=\s*"([^"]+)"', text, _re.M)
        return m.group(1) if m else "0.3.1"
    except Exception:
        pass
    try:
        return _im.version("pangflow")
    except Exception:
        return "0.3.1"

__version__ = _read_version()

from typing import Any, Dict, Optional

from pangflow.orchestration import registry as _registry
node = _registry.node
workflow = _registry.workflow
serve = _registry.serve
from pangflow.observer.subject import get_subject
from pangflow.storage.model_store import save_model as _save_model_impl, load_model

def save_model(name: str, model: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Save *model* under *name* using the default ModelStore."""
    meta = dict(metadata or {})
    ctx = _get_log_context()
    meta.setdefault("workflow_id", ctx.get("workflow_id") or "")
    meta.setdefault("node_id", ctx.get("node_id") or "")
    upstream_artifact_id = getattr(_log_context_local, '_last_artifact_id', None)
    result = _save_model_impl(
        name, model, meta,
        upstream_artifact_ids=[upstream_artifact_id] if upstream_artifact_id else None,
    )
    # Cache the artifact_id in log context so lineage can link it to the current node
    _log_context_local._last_artifact_id = result.get("artifact_id")
    return result


from pangflow.storage.feature_store import load_feature
from pangflow.core.config import get_param
from pangflow.serve.tracer import get_trace_id
from pangflow.observer import setup_default_observers

# Re-bind after sub-module import to prevent module object shadowing the decorator
serve = _registry.serve

# Logging API injected at runtime by NodeTask pre_execute hook.
# These module-level placeholders are overwritten when a node runs.
import threading
_log_context_local = threading.local()

def _set_log_context(workflow_id: str = None, node_id: str = None, node_name: str = None, run_id: str = None) -> None:
    """Set the current execution context for log/metric injection."""
    _log_context_local.workflow_id = workflow_id
    _log_context_local.node_id = node_id
    _log_context_local.node_name = node_name
    _log_context_local.run_id = run_id

def _get_log_context() -> dict:
    """Get the current execution context."""
    return {
        "workflow_id": getattr(_log_context_local, "workflow_id", None),
        "node_id": getattr(_log_context_local, "node_id", None),
        "node_name": getattr(_log_context_local, "node_name", None),
        "run_id": getattr(_log_context_local, "run_id", None),
    }


def log(message: str, level: str = "INFO", **extra) -> None:
    """Emit a structured log record from inside a node function.

    Parameters
    ----------
    message : str
        Log message text.
    level : str, optional
        One of DEBUG, INFO, WARNING, ERROR, CRITICAL. Defaults to INFO.
    **extra :
        Arbitrary key-value pairs attached to the log record.
    """
    from pangflow.observer.log_observer import LogObserver

    ctx = _get_log_context()
    subject = get_subject()
    subject.publish(
        "LOG_RECORD",
        {
            "level": level,
            "message": message,
            "extra": extra,
            "timestamp": __import__("datetime").datetime.now().isoformat(),
            "workflow_id": ctx["workflow_id"],
            "node_id": ctx["node_id"],
            "node_name": ctx["node_name"],
            "trace_id": ctx["run_id"],
        },
    )


def log_metric(name: str, value: float, **tags) -> None:
    """Record a business metric from inside a node function.

    Parameters
    ----------
    name : str
        Metric name.
    value : float
        Metric value.
    **tags :
        Additional dimension tags.
    """
    ctx = _get_log_context()
    subject = get_subject()
    subject.publish(
        "METRIC_RECORD",
        {
            "metric_name": name,
            "metric_value": value,
            "tags": tags,
            "timestamp": __import__("datetime").datetime.now().isoformat(),
            "workflow_id": ctx["workflow_id"],
            "node_id": ctx["node_id"],
            "node_name": ctx["node_name"],
            "run_id": ctx["run_id"],
        },
    )


__all__ = [
    "__version__",
    "node",
    "workflow",
    "serve",
    "log",
    "log_metric",
    "save_model",
    "load_model",
    "load_feature",
    "get_param",
    "get_trace_id",
]

setup_default_observers()
