# -*- coding: utf-8 -*-
"""
HTTP endpoint definition for pangflow serve subsystem.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional, Type

from fastapi.routing import APIRoute
from pydantic import BaseModel


@dataclass
class HTTPEndpoint:
    """Definition of a single HTTP endpoint exposed by pangflow serve."""

    name: str
    path: str
    method: str
    handler_module: str
    handler_func: str
    request_model: Optional[Type[BaseModel]] = None
    response_model: Optional[Type[BaseModel]] = None
    validate_request: bool = True

    def build_route(self) -> APIRoute:
        """Build a FastAPI APIRoute from this endpoint definition."""
        import importlib

        module = importlib.import_module(self.handler_module)
        endpoint: Callable[..., Any] = getattr(module, self.handler_func)

        return APIRoute(
            path=self.path,
            endpoint=endpoint,
            methods=[self.method.upper()],
            name=self.name,
            response_model=self.response_model,
        )
