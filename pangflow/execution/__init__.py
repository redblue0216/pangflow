# -*- coding: utf-8 -*-
"""
Execution layer – strategies for running tasks in different environments.
"""

from pangflow.execution.strategy import ExecutionStrategy
from pangflow.execution.local import LocalStrategy
from pangflow.execution.conda import CondaStrategy
from pangflow.execution.http_service import HTTPServiceStrategy
from pangflow.execution.storage import StorageStrategy

__all__ = [
    "ExecutionStrategy",
    "LocalStrategy",
    "CondaStrategy",
    "HTTPServiceStrategy",
    "StorageStrategy",
]
