# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: masflow
# Copyright (c) 2026 The masflow Authors. All rights reserved.



'''
Module Introduction
-------------------

MasFlow - A workflow orchestration tool based on Prefect.

MasFlow is designed to schedule and trigger workflows for ML training and inference.
It provides a CLI interface for managing workflows, with support for both scheduled
and triggered execution patterns.

- Design mode:

    (1) Workflow orchestration

- Key points:

    (1) Prefect integration

    (2) CLI interface

- Main functions:

    (1) Workflow scheduling

    (2) Workflow triggering

    (3) Workflow deployment

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    import masflow
    from masflow import WorkflowState, WorkflowRunner

    ### Create a workflow
    workflow = WorkflowState(...)

Description of Class and Function
-----------------
(1)WorkflowState: Data class representing workflow state

(2)WorkflowStateManager: Manager class for workflow state transitions

(3)WorkflowTask: Abstract base class for workflow tasks

(4)TaskFactory: Factory for creating task instances

(5)WorkflowScheduler: Abstract base class for workflow schedulers

(6)SchedulerFactory: Factory for creating scheduler instances

(7)WorkflowRunner: Abstract base class for workflow runners

(8)RunnerFactory: Factory for creating runner instances

References
----------
Prefect Documentation <https://docs.prefect.io/>
'''



####### Version Info ###############################################################################
####################################################################################################



from pangflow.core.state import WorkflowState, WorkflowStateManager
from pangflow.core.task import WorkflowTask, TaskFactory
from pangflow.core.scheduler import WorkflowScheduler, SchedulerFactory
from pangflow.core.runner import WorkflowRunner, RunnerFactory

__all__ = [
    "WorkflowState",
    "WorkflowStateManager",
    "WorkflowTask",
    "TaskFactory",
    "WorkflowScheduler",
    "SchedulerFactory",
    "WorkflowRunner",
    "RunnerFactory",
]



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
