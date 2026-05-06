# -*- coding: utf-8 -*-
"""
PangFlow environment subsystem.

Provides dataclasses for environment specifications, a thin wrapper around
Conda CLI, and a singleton manager that caches state in SQLite.
"""

from pangflow.env.spec import CondaSpec, EnvSpec, PipSpec
from pangflow.env.conda_env import CondaEnv
from pangflow.env.manager import EnvManager

__all__ = [
    "CondaSpec",
    "EnvSpec",
    "PipSpec",
    "CondaEnv",
    "EnvManager",
]
