# -*- coding: utf-8 -*-
"""
Request tracer utilities.
"""

import uuid


def get_trace_id() -> str:
    """Return the current request trace id, or generate a new one."""
    # TODO: integrate with context vars / request state.
    return str(uuid.uuid4())
