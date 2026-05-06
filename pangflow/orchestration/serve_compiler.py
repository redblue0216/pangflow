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
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, create_model

    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False
    FastAPI = None
    Request = None
    JSONResponse = None
    BaseModel = None
    create_model = None

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

        # Build Pydantic request model from signature.
        fields: dict = {}
        for name, param in sig.parameters.items():
            ann = param.annotation if param.annotation is not inspect.Parameter.empty else Any
            default = param.default if param.default is not inspect.Parameter.empty else ...
            fields[name] = (ann, default)

        RequestModel = create_model(f"{ep.name}_Request", **fields)

        # Determine response model (best-effort).
        return_annotation = sig.return_annotation if sig.return_annotation is not inspect.Parameter.empty else Any

        async def _handler(data: RequestModel):  # type: ignore[valid-type]
            # Pydantic v2 compat
            if hasattr(data, "model_dump"):
                payload = data.model_dump()
            else:
                payload = data.dict()
            return ep.func_ref(**payload)

        app.add_api_route(
            path=ep.endpoint,
            endpoint=_handler,
            methods=[ep.method],
            name=ep.name,
            response_model=return_annotation if return_annotation is not Any else None,
        )
