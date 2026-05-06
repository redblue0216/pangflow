# -*- coding: utf-8 -*-
# Author: shihua
# License: Apache-2.0
# Copyright (c) 2026 The PangFlow Authors. All rights reserved.

"""
PangFlow v0.2.7 - Algorithm OPS Orchestration Tool

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

__version__ = "0.2.7"

from pangflow.orchestration import registry as _registry
node = _registry.node
workflow = _registry.workflow
serve = _registry.serve
from pangflow.observer.subject import get_subject
from pangflow.storage.model_store import save_model, load_model
from pangflow.storage.feature_store import load_feature
from pangflow.core.config import get_param
from pangflow.serve.tracer import get_trace_id
# Re-bind after sub-module import to prevent module object shadowing the decorator
serve = _registry.serve

# Logging API injected at runtime by NodeTask pre_execute hook.
# These module-level placeholders are overwritten when a node runs.
_log_context = None


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

    subject = get_subject()
    subject.publish(
        "LOG_RECORD",
        {
            "level": level,
            "message": message,
            "extra": extra,
            "timestamp": __import__("datetime").datetime.now().isoformat(),
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
    subject = get_subject()
    subject.publish(
        "METRIC_RECORD",
        {
            "metric_name": name,
            "metric_value": value,
            "tags": tags,
            "timestamp": __import__("datetime").datetime.now().isoformat(),
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
