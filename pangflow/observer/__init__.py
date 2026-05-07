# -*- coding: utf-8 -*-
"""
Observer Layer – event-driven observability subsystem for PangFlow.

Public API
----------
- get_subject()          : singleton event bus
- LogObserver            : persists LOG_RECORD events to node_logs
- MetricObserver         : captures METRIC_RECORD events (in-memory + optional DB)
- LineageObserver        : records lineage_edges on ARTIFACT_REGISTERED / NODE_COMPLETE
- PrefectUIObserver      : stub that logs events for Prefect UI integration
- AlertObserver          : rule-based alerting with cooldown and channels
- AlertRule              : single alert rule definition
- NotificationChannel    : abstract notification sink
- WebhookChannel         : webhook notification stub
- LogFilterChain         : composable AND filter for LogObserver
- LevelFilter            : filter by log level
- NodeFilter             : filter by node_id
- WorkflowFilter         : filter by workflow_id
- TimeRangeFilter        : filter by timestamp range
- RegexFilter            : filter by regex on message
"""

from pangflow.observer.subject import Subject, get_subject
from pangflow.observer.log_observer import (
    LogObserver,
    LogFilterChain,
    LevelFilter,
    NodeFilter,
    WorkflowFilter,
    TimeRangeFilter,
    RegexFilter,
)
from pangflow.observer.metric_observer import MetricObserver
from pangflow.observer.lineage_observer import LineageObserver
from pangflow.observer.prefect_ui import PrefectUIObserver
from pangflow.observer.alert_observer import (
    AlertObserver,
    AlertRule,
    NotificationChannel,
    WebhookChannel,
)

__all__ = [
    # Bus
    "Subject",
    "get_subject",
    # Observers
    "LogObserver",
    "MetricObserver",
    "LineageObserver",
    "PrefectUIObserver",
    "AlertObserver",
    # Alert primitives
    "AlertRule",
    "NotificationChannel",
    "WebhookChannel",
    # Log filters
    "LogFilterChain",
    "LevelFilter",
    "NodeFilter",
    "WorkflowFilter",
    "TimeRangeFilter",
    "RegexFilter",
]


def setup_default_observers() -> list:
    """Instantiate and start the standard observer set.

    Returns a list of started observer instances so the caller can stop them
    later if desired.
    """
    observers = [
        LogObserver(),
        MetricObserver(store_in_db=True),
        LineageObserver(),
        PrefectUIObserver(),
    ]
    for obs in observers:
        obs.start()
    return observers
