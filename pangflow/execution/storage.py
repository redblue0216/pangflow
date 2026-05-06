# -*- coding: utf-8 -*-
"""
Storage execution strategy – routes data through StorageManager.
"""

import logging
from typing import Any, Dict, Optional

from pangflow.execution.strategy import ExecutionStrategy, Result
from pangflow.task.base import BaseTask, ExecutionContext
from pangflow.storage.manager import StorageManager, StorageKey


class StorageStrategy(ExecutionStrategy):
    """Strategy that persists data via :class:`StorageManager`."""

    def __init__(self, storage_manager: Optional[StorageManager] = None) -> None:
        self.storage_manager = storage_manager or StorageManager()
        self._logger = logging.getLogger(__name__)

    def prepare_environment(self, env_spec) -> None:
        pass

    def cleanup(self) -> None:
        pass

    def execute(self, task: BaseTask, context: ExecutionContext) -> Result:
        """Not supported – use :meth:`route_data` instead."""
        raise NotImplementedError("StorageStrategy does not execute tasks; use route_data()")

    def route_data(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> StorageKey:
        """Serialize *data* and return a :class:`StorageKey`."""
        self._logger.debug("StorageStrategy routing data")
        return self.storage_manager.route_data(data, metadata)
