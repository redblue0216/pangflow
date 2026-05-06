# -*- coding: utf-8 -*-
"""
ServeManager – lifecycle manager for FastAPI/uvicorn servers.
"""

import threading
from typing import Optional

import uvicorn
from fastapi import FastAPI


class ServeManager:
    """Manages a uvicorn server running in a background thread."""

    def __init__(self) -> None:
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None
        self._host: str = "127.0.0.1"
        self._port: int = 8000

    def start(
        self,
        app: FastAPI,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> None:
        """Start *app* on the given *host* and *port*."""
        self._host = host
        self._port = port
        config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the server to shut down and wait for the thread."""
        if self._server is not None:
            self._server.should_exit = True
            if self._thread is not None:
                self._thread.join(timeout=10.0)
            self._server = None
            self._thread = None

    @property
    def is_running(self) -> bool:
        """Return True if the server thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    def base_url(self) -> str:
        """Return the base URL for the running server."""
        return f"http://{self._host}:{self._port}"
