# -*- coding: utf-8 -*-
"""
Environment specification dataclasses.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class CondaSpec:
    """Conda package specification."""

    channels: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)


@dataclass
class PipSpec:
    """Pip package specification."""

    dependencies: List[str] = field(default_factory=list)


@dataclass
class EnvSpec:
    """Full environment specification."""

    name: str
    python: str = "3.10"
    conda: CondaSpec = field(default_factory=CondaSpec)
    pip: PipSpec = field(default_factory=PipSpec)
