# -*- coding: utf-8 -*-
"""
AlertObserver – evaluates alert rules and dispatches notifications.

Architecture
------------
    NotificationChannel (abstract)
    └── WebhookChannel

    AlertRule
        - condition:  simple Python expression evaluated against the event payload
        - severity:   info | warning | critical
        - cooldown:   minimum seconds between triggers

    AlertObserver
        - holds a list of AlertRule objects
        - listens to METRIC_RECORD and NODE_FAILURE events

Usage
-----
    from pangflow.observer.alert_observer import (
        AlertObserver, AlertRule, WebhookChannel
    )

    channel = WebhookChannel("https://hooks.example.com/alerts")
    rule = AlertRule(
        name="high_loss",
        condition="metric_name == 'loss' and metric_value > 0.5",
        severity="critical",
        cooldown_seconds=60,
        channels=[channel],
    )

    obs = AlertObserver()
    obs.add_rule(rule)
    obs.start()
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from pangflow.observer.subject import get_subject

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Channels
# --------------------------------------------------------------------------- #

class NotificationChannel(ABC):
    """Abstract sink for alert notifications."""

    @abstractmethod
    def send(self, alert: Dict[str, Any]) -> None:
        """Deliver the alert payload."""
        ...


class WebhookChannel(NotificationChannel):
    """Stub channel that logs what would be sent to a webhook URL."""

    def __init__(self, url: str):
        self.url = url

    def send(self, alert: Dict[str, Any]) -> None:
        logger.warning("[WebhookChannel] → %s | %s", self.url, alert)


# --------------------------------------------------------------------------- #
# Rule
# --------------------------------------------------------------------------- #

@dataclass
class AlertRule:
    """A single alert rule with expression-based evaluation."""

    name: str
    condition: str
    severity: str = "warning"
    cooldown_seconds: int = 300
    channels: List[NotificationChannel] = field(default_factory=list)
    _last_triggered: Optional[float] = field(default=None, repr=False)

    def evaluate(self, payload: Dict[str, Any]) -> bool:
        """Evaluate the rule condition against the event payload."""
        try:
            # Restricted eval – no builtins, only payload keys as locals.
            result = eval(self.condition, {"__builtins__": {}}, payload)
            return bool(result)
        except Exception:
            return False

    def is_on_cooldown(self) -> bool:
        if self._last_triggered is None:
            return False
        return (time.time() - self._last_triggered) < self.cooldown_seconds

    def trigger(self, payload: Dict[str, Any]) -> None:
        if self.is_on_cooldown():
            return
        self._last_triggered = time.time()

        alert = {
            "rule_name": self.name,
            "severity": self.severity,
            "timestamp": datetime.now().isoformat(),
            "payload": payload,
        }
        for channel in self.channels:
            try:
                channel.send(alert)
            except Exception:
                logger.exception("Alert channel failed for rule '%s'", self.name)


# --------------------------------------------------------------------------- #
# Observer
# --------------------------------------------------------------------------- #

class AlertObserver:
    """Subscriber that evaluates alert rules against incoming events."""

    def __init__(self, rules: Optional[List[AlertRule]] = None):
        self.rules: List[AlertRule] = rules or []

    def add_rule(self, rule: AlertRule) -> None:
        self.rules.append(rule)

    # -- callbacks ---------------------------------------------------------- #

    def on_event(self, payload: Dict[str, Any]) -> None:
        for rule in self.rules:
            if rule.evaluate(payload):
                rule.trigger(payload)

    # -- lifecycle ---------------------------------------------------------- #

    def start(self) -> None:
        get_subject().attach("METRIC_RECORD", self.on_event)
        get_subject().attach("NODE_FAILURE", self.on_event)

    def stop(self) -> None:
        get_subject().detach("METRIC_RECORD", self.on_event)
        get_subject().detach("NODE_FAILURE", self.on_event)
