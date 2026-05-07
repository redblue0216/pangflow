# -*- coding: utf-8 -*-
"""
ServeCompiler – compiles @serve endpoints into a FastAPI application.
"""

from __future__ import annotations

import inspect
import logging
import uuid
from typing import Any, Callable, List

try:
    from fastapi import FastAPI, Request
    from pydantic import BaseModel

    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False
    FastAPI = None
    Request = None
    BaseModel = None

from pangflow.orchestration.registry import ServeMetadata

logger = logging.getLogger(__name__)


class ServeCompiler:
    """Generates a FastAPI app from a list of :class:`ServeMetadata`."""

    def compile(self, endpoints: List[ServeMetadata]) -> Any:
        if not FASTAPI_AVAILABLE:
            raise RuntimeError(
                "FastAPI / Pydantic not installed. "
                "Install them with: pip install fastapi pydantic uvicorn"
            )

        app: FastAPI = FastAPI(title="PangFlow Serve")

        # ------------------------------------------------------------------ #
        # Tracing middleware
        # ------------------------------------------------------------------ #
        @app.middleware("http")
        async def tracing_middleware(request: Request, call_next: Callable):
            trace_id = request.headers.get("x-trace-id", str(uuid.uuid4()))
            request.state.trace_id = trace_id
            response = await call_next(request)
            response.headers["x-trace-id"] = trace_id
            return response

        # ------------------------------------------------------------------ #
        # Register routes
        # ------------------------------------------------------------------ #
        for ep in endpoints:
            self._add_route(app, ep)

        return app

    def _add_route(self, app: FastAPI, ep: ServeMetadata) -> None:
        sig = inspect.signature(ep.func_ref)

        # Determine response model (best-effort).
        return_annotation = sig.return_annotation if sig.return_annotation is not inspect.Parameter.empty else Any

        # Read raw JSON body and pass to the decorated function.
        sig = inspect.signature(ep.func_ref)
        params = list(sig.parameters.values())
        if (
            params
            and len(params) == 1
            and params[0].annotation is not inspect.Parameter.empty
            and isinstance(params[0].annotation, type)
            and issubclass(params[0].annotation, BaseModel)
        ):
            RequestModel = params[0].annotation
            if hasattr(RequestModel, "model_rebuild"):
                RequestModel.model_rebuild()
            async def _handler(request: Request, _ep=ep, _Model=RequestModel):
                body = await request.json()
                data = _Model(**body)
                return _ep.func_ref(data)
        else:
            async def _handler(request: Request, _ep=ep):
                body = await request.json()
                return _ep.func_ref(**body)

        app.add_api_route(
            path=ep.endpoint,
            endpoint=_handler,
            methods=[ep.method],
            name=ep.name,
            response_model=return_annotation if return_annotation is not Any else None,
        )
