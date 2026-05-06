# -*- coding: utf-8 -*-
"""
HTTP service execution strategy – delegates to a FastAPI server managed by ServeManager.
"""

import logging
from typing import Any, Dict, Optional

import requests

from pangflow.execution.strategy import ExecutionStrategy, Result
from pangflow.task.base import BaseTask, ExecutionContext


class HTTPServiceStrategy(ExecutionStrategy):
    """Strategy that sends tasks to a local FastAPI/uvicorn server."""

    def __init__(self, serve_manager: Optional[Any] = None) -> None:
        from pangflow.serve.manager import ServeManager

        self.serve_manager = serve_manager or ServeManager()
        self._logger = logging.getLogger(__name__)

    def prepare_environment(self, env_spec) -> None:
        # Environment is owned by the server process.
        pass

    def cleanup(self) -> None:
        self.stop_server()

    def start_server(self, app, host: str = "127.0.0.1", port: int = 8000) -> None:
        """Start the uvicorn server with the given FastAPI *app*."""
        self.serve_manager.start(app, host=host, port=port)

    def stop_server(self) -> None:
        """Signal the server to shut down."""
        self.serve_manager.stop()

    def execute(self, task: BaseTask, context: ExecutionContext) -> Result:
        """POST to the endpoint described by *task.config* and return the response.

        ``task.config`` must contain ``endpoint`` and optionally ``request``.
        """
        endpoint = task.config.get("endpoint") if hasattr(task, "config") else None
        request_payload = task.config.get("request") if hasattr(task, "config") else None
        if endpoint is None:
            return Result(
                status="failed",
                error="HTTPServiceStrategy requires task.config['endpoint']",
            )

        url = f"{self.serve_manager.base_url()}{endpoint}"
        self._logger.debug("HTTPServiceStrategy POST %s", url)
        try:
            resp = requests.post(url, json=request_payload or {}, timeout=30)
            resp.raise_for_status()
            return Result(status="success", data=resp.json())
        except Exception as exc:
            self._logger.exception("HTTP request failed")
            return Result(status="failed", error=str(exc))

    def execute_endpoint(self, endpoint: str, request: Optional[Dict[str, Any]] = None) -> Any:
        """Convenience method to call an *endpoint* directly.

        Returns the raw response JSON or raises on failure.
        """
        url = f"{self.serve_manager.base_url()}{endpoint}"
        resp = requests.post(url, json=request or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()
