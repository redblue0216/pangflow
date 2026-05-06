# -*- coding: utf-8 -*-
"""
Execution layer – base strategy.
"""

from abc import ABC, abstractmethod

from pangflow.task.base import BaseTask, ExecutionContext, Result


class ExecutionStrategy(ABC):
    """Abstract base class for execution strategies."""

    @abstractmethod
    def execute(self, task: BaseTask, context: ExecutionContext) -> Result:
        """Execute *task* within *context* and return a :class:`Result`."""
        ...

    @abstractmethod
    def prepare_environment(self, env_spec) -> None:
        """Prepare the runtime environment described by *env_spec*."""
        ...

    @abstractmethod
    def cleanup(self) -> None:
        """Release any resources acquired by this strategy."""
        ...
