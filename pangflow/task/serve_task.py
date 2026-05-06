# -*- coding: utf-8 -*-
"""
Task Layer – ServeTask for HTTP endpoint handlers.
"""

import inspect
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from pangflow.task.base import BaseTask, ExecutionContext, Result


@dataclass
class HTTPEndpoint:
    """HTTP endpoint descriptor."""

    path: str = "/"
    method: str = "POST"
    host: Optional[str] = None
    port: Optional[int] = None


@dataclass
class ServeMetadata:
    """Metadata describing a serve task."""

    name: Optional[str] = None
    description: Optional[str] = None
    endpoint: HTTPEndpoint = field(default_factory=HTTPEndpoint)
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None


class ServeTask(BaseTask):
    """Concrete task implementation for HTTP service handlers."""

    def __init__(
        self,
        task_id: Optional[str] = None,
        name: Optional[str] = None,
        func: Optional[Callable] = None,
        config: Optional[Dict[str, Any]] = None,
        metadata: Optional[ServeMetadata] = None,
    ):
        super().__init__(task_id=task_id, name=name, func=func, config=config)
        self.metadata = metadata or ServeMetadata(name=self.name)

    # --------------------------------------------------------------------- #
    # Request / response helpers
    # --------------------------------------------------------------------- #

    def validate_request(self, request: Any) -> bool:
        """Validate an incoming request payload.

        Returns *True* when the request is a dict (basic validation).
        Subclasses may override for stricter schema checks.
        """
        if not isinstance(request, dict):
            return False
        if self.metadata.input_schema:
            for key, expected_type in self.metadata.input_schema.items():
                if key not in request:
                    return False
                if expected_type is not None and not isinstance(request[key], expected_type):
                    return False
        return True

    def build_response(self, result: Any) -> Dict[str, Any]:
        """Wrap a handler result into a standard HTTP-like response dict."""
        return {
            "status": "success",
            "data": result,
            "timestamp": datetime.now().isoformat(),
        }

    # --------------------------------------------------------------------- #
    # Execution
    # --------------------------------------------------------------------- #

    def execute(self, context: ExecutionContext) -> Result:
        self.on_start(context)
        try:
            data = self._do_execute(context)
            self.result = Result(
                status="success",
                data=data,
                started_at=datetime.now(),
                completed_at=datetime.now(),
            )
            self.on_success(context, self.result)
        except Exception as exc:
            self.result = Result(
                status="failed",
                error=str(exc),
                started_at=datetime.now(),
                completed_at=datetime.now(),
            )
            self.on_failure(context, exc)
        return self.result

    def _do_execute(self, context: ExecutionContext) -> Any:
        if self.func is None:
            raise RuntimeError("ServeTask.func is not set")

        request = context.runtime_params.get("request", {})
        if not self.validate_request(request):
            raise ValueError("Invalid request payload")

        sig = inspect.signature(self.func)
        bound: Dict[str, Any] = {}

        for param_name, param in sig.parameters.items():
            if param_name == "context":
                bound["context"] = context
                continue
            if param_name == "request":
                bound["request"] = request
                continue
            if param_name in context.runtime_params:
                bound[param_name] = context.runtime_params[param_name]
            elif param.default is not inspect.Parameter.empty:
                bound[param_name] = param.default
            else:
                raise TypeError(
                    f"Missing required argument '{param_name}' for serve handler '{self.name}'"
                )

        raw_result = self.func(**bound)
        return self.build_response(raw_result)
