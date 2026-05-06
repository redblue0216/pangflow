# -*- coding: utf-8 -*-
"""
PrefectUIObserver – stub that forwards selected events to the Prefect UI layer.

Currently only logs events at DEBUG level.  In future iterations this can be
wired to Prefect's logging or state-reporting APIs.

Usage
-----
    from pangflow.observer.prefect_ui import PrefectUIObserver
    obs = PrefectUIObserver()
    obs.start()
"""

import logging
from functools import partial
from typing import Any, Callable, Dict, List, Optional

from pangflow.observer.subject import get_subject

logger = logging.getLogger(__name__)


class PrefectUIObserver:
    """Subscriber that mirrors workflow events to the Prefect UI."""

    def __init__(self, event_types: Optional[List[str]] = None):
        self.event_types = event_types or [
            "NODE_START",
            "NODE_COMPLETE",
            "NODE_FAILURE",
            "METRIC_RECORD",
        ]
        self._handlers: Dict[str, Callable[[Dict[str, Any]], None]] = {}

    # -- callbacks ---------------------------------------------------------- #

    def _handle(self, event_type: str, payload: Dict[str, Any]) -> None:
        logger.debug("[PrefectUI] %s: %s", event_type, payload)

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        subject = get_subject()
        for event_type in self.event_types:
            handler: Callable[[Dict[str, Any]], None] = partial(self._handle, event_type)
            self._handlers[event_type] = handler
            subject.attach(event_type, handler)

    def stop(self) -> None:
        subject = get_subject()
        for event_type, handler in self._handlers.items():
            subject.detach(event_type, handler)
        self._handlers.clear()
