# -*- coding: utf-8 -*-
"""
Conda execution strategy – runs tasks inside a managed Conda environment.
"""

import logging
import os
import tempfile
from typing import Any, Optional

import cloudpickle

from pangflow.execution.strategy import ExecutionStrategy, Result
from pangflow.task.base import BaseTask, ExecutionContext
from pangflow.env.manager import EnvManager
from pangflow.env.spec import EnvSpec


class CondaStrategy(ExecutionStrategy):
    """Execute tasks inside a Conda environment managed by :class:`EnvManager`."""

    def __init__(self, env_manager: Optional[EnvManager] = None) -> None:
        self.env_manager = env_manager or EnvManager()
        self._env: Optional[Any] = None
        self._logger = logging.getLogger(__name__)

    def prepare_environment(self, env_spec: EnvSpec) -> None:
        """Create or update the Conda environment from *env_spec*."""
        env = self.env_manager.create_env(env_spec)
        if env is None:
            raise RuntimeError("Failed to prepare Conda environment")
        self._env = env

    def cleanup(self) -> None:
        self._env = None

    def execute(self, task: BaseTask, context: ExecutionContext) -> Result:
        """Run *task* inside the prepared Conda environment.

        For tasks that carry a ``func`` callable the call is serialized into a
        subprocess via ``conda run``. Otherwise ``task.execute(context)`` is
        invoked directly (useful when the current process already lives in the
        target environment).
        """
        env = self._resolve_env(context)
        if env is None:
            raise RuntimeError("No Conda environment available for execution")

        func = getattr(task, "func", None)
        if func is not None:
            return self._execute_in_conda(env, func, context)

        return task.execute(context)

    def _resolve_env(self, context: ExecutionContext) -> Optional[Any]:
        if self._env is not None:
            return self._env
        if getattr(context, "env", None) is not None:
            return context.env
        return self.env_manager.get_env(context.workflow_id)

    def _execute_in_conda(self, env, func, context: ExecutionContext) -> Result:
        # Serialize payload to a temp file.
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pkl", delete=False) as fh:
            cloudpickle.dump((func, context), fh)
            input_path = fh.name

        output_path = input_path + ".out"
        script_path = input_path + ".py"

        script = (
            "import cloudpickle\n"
            f"with open({input_path!r}, 'rb') as fh:\n"
            "    func, ctx = cloudpickle.load(fh)\n"
            "result = func(ctx)\n"
            f"with open({output_path!r}, 'wb') as fh:\n"
            "    cloudpickle.dump(result, fh)\n"
        )
        with open(script_path, "w", encoding="utf-8") as fh:
            fh.write(script)

        cmd = f"python {script_path}"
        self._logger.debug("CondaStrategy running command in env '%s'", env.name)
        try:
            proc = env.run_command(cmd)
        finally:
            for p in (input_path, script_path):
                try:
                    os.unlink(p)
                except OSError:
                    pass

        if proc.returncode != 0:
            try:
                os.unlink(output_path)
            except OSError:
                pass
            return Result(status="failed", error=proc.stderr)

        try:
            with open(output_path, "rb") as fh:
                data = cloudpickle.load(fh)
        except Exception as exc:
            return Result(status="failed", error=f"Failed to deserialize result: {exc}")
        finally:
            try:
                os.unlink(output_path)
            except OSError:
                pass

        return Result(status="success", data=data)
