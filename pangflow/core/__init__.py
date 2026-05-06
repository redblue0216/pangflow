# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: pangflow
# Copyright (c) 2026 The pangflow Authors. All rights reserved.



'''
Module Introduction
-------------------

This is the core module for pangflow workflow engine.

- Design mode:

    (1) Module aggregation pattern

- Key points:

    (1) Centralized imports for all core components

    (2) Clean API exposure through __all__

- Main functions:

    (1) Expose WorkflowState and WorkflowStateManager for state management

    (2) Expose WorkflowTask and TaskFactory for task execution

    (3) Expose WorkflowScheduler and SchedulerFactory for workflow scheduling

    (4) Expose WorkflowRunner and RunnerFactory for workflow execution

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Import core components
    from pangflow.core import WorkflowState, WorkflowStateManager
    from pangflow.core import WorkflowTask, TaskFactory
    from pangflow.core import WorkflowScheduler, SchedulerFactory
    from pangflow.core import WorkflowRunner, RunnerFactory

Description of Class and Function
-----------------
(1) WorkflowState: Data class representing a workflow's state at a point in time

(2) WorkflowStateManager: Manager class for workflow state transitions

(3) WorkflowTask: Abstract base class for workflow tasks

(4) TaskFactory: Factory for creating task instances

(5) WorkflowScheduler: Abstract base class for workflow schedulers

(6) SchedulerFactory: Factory for creating scheduler instances

(7) WorkflowRunner: Abstract base class for workflow runners

(8) RunnerFactory: Factory for creating runner instances

References
----------
pangflow Documentation
'''



####### Load Packages ##############################################################################
####################################################################################################



### Import core state management components
from pangflow.core.state import WorkflowState, WorkflowStateManager
### Import core task execution components
from pangflow.core.task import WorkflowTask, TaskFactory
### Import core scheduling components
from pangflow.core.scheduler import WorkflowScheduler, SchedulerFactory
### Import core runner components
from pangflow.core.runner import WorkflowRunner, RunnerFactory
### Import core configuration components
from pangflow.core.config import (
    WorkflowConfig,
    WorkflowEnv,
    WorkflowStorage,
    WorkflowLog,
    WorkflowServe,
    NodeConfig,
    ConfigLoader,
    get_param,
    set_runtime_param,
    set_cli_params,
    clear_runtime_params,
    set_default_loader,
    set_current_node,
)



####### Public API #################################################################################
####################################################################################################



__all__ = [
    ### State management
    "WorkflowState",
    "WorkflowStateManager",
    ### Task execution
    "WorkflowTask",
    "TaskFactory",
    ### Scheduling
    "WorkflowScheduler",
    "SchedulerFactory",
    ### Runners
    "WorkflowRunner",
    "RunnerFactory",
    ### Configuration
    "WorkflowConfig",
    "WorkflowEnv",
    "WorkflowStorage",
    "WorkflowLog",
    "WorkflowServe",
    "NodeConfig",
    "ConfigLoader",
    "get_param",
    "set_runtime_param",
    "set_cli_params",
    "clear_runtime_params",
    "set_default_loader",
    "set_current_node",
]



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
