# -*- coding: utf-8 -*-
"""
PangFlow Web Server – FastAPI sub-app with static files and API router.
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from pangflow.web.api import router as api_router


_STATIC_DIR = Path(__file__).parent / "static"


def create_web_app() -> FastAPI:
    """Create and configure the pangflow web FastAPI application."""
    app = FastAPI(
        title="PangFlow Web",
        description="PangFlow workflow engine web backend",
        version="0.2.7",
    )

    # Include API router
    app.include_router(api_router)

    # Mount static files for the UI
    if _STATIC_DIR.exists():
        app.mount("/ui", StaticFiles(directory=str(_STATIC_DIR), html=True), name="ui")
        # Also serve index at root for convenience
        app.mount("/", StaticFiles(directory=str(_STATIC_DIR), html=True), name="static")

    return app
