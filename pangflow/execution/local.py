# -*- coding: utf-8 -*-
"""
Local execution strategy – runs tasks directly in the current process.
"""

import logging

from pangflow.execution.strategy import ExecutionStrategy, Result
from pangflow.task.base import BaseTask, ExecutionContext


class LocalStrategy(ExecutionStrategy):
    """Execute tasks directly via in-process function calls."""

    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def prepare_environment(self, env_spec) -> None:
        # Local execution relies on the ambient environment.
        pass

    def cleanup(self) -> None:
        pass

    def execute(self, task: BaseTask, context: ExecutionContext) -> Result:
        """Call ``task.execute(context)`` directly."""
        self._logger.debug("LocalStrategy executing task %s", getattr(task, "task_id", "<?>"))
        return task.execute(context)
