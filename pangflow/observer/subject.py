# -*- coding: utf-8 -*-
"""
Observer Layer – Central event bus (Subject).
"""

import logging
import threading
from typing import Dict, List, Callable, Any

logger = logging.getLogger(__name__)


class Subject:
    """Singleton event bus."""

    _instance: "Subject" = None
    _lock = threading.Lock()

    def __new__(cls) -> "Subject":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._observers: Dict[str, List[Callable]] = {}
        return cls._instance

    def attach(self, event_type: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        self._observers.setdefault(event_type, []).append(callback)

    def detach(self, event_type: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        if event_type in self._observers:
            try:
                self._observers[event_type].remove(callback)
            except ValueError:
                pass

    def publish(self, event_type: str, payload: Dict[str, Any]) -> None:
        observers = self._observers.get(event_type, [])
        for callback in observers:
            try:
                callback(payload)
            except Exception:
                logger.exception("Observer callback failed for event %s", event_type)

    def notify(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Alias for publish."""
        self.publish(event_type, payload)


def get_subject() -> Subject:
    return Subject()
