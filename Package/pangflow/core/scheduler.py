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

This is the scheduler module for pangflow workflow engine.

- Design mode:

    (1) Strategy Pattern - Different scheduler implementations

    (2) Factory Pattern - Scheduler creation through SchedulerFactory

- Key points:

    (1) Abstract base class WorkflowScheduler defines the scheduler interface

    (2) ImmediateScheduler for one-time immediate execution

    (3) CronScheduler for cron-based recurring execution

    (4) IntervalScheduler for interval-based recurring execution

    (5) SchedulerFactory creates appropriate scheduler instances

- Main functions:

    (1) Schedule workflows for execution

    (2) Support multiple scheduling strategies (immediate, cron, interval)

    (3) Calculate next run times

    (4) Unschedule workflows

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Create an immediate scheduler
    from pangflow.core.scheduler import SchedulerFactory
    
    scheduler = SchedulerFactory.create("immediate")
    schedule_id = scheduler.schedule("wf-001", task_factory)
    
    ### Create a cron scheduler
    cron_scheduler = SchedulerFactory.create(
        "cron", 
        config={"cron": "0 0 * * *"}
    )

Description of Class and Function
-----------------
(1) ScheduleType: Enumeration of schedule types

(2) WorkflowScheduler: Abstract base class for workflow schedulers (Strategy Pattern)

(3) ImmediateScheduler: Scheduler for immediate, one-time execution

(4) CronScheduler: Scheduler for cron-based recurring execution

(5) IntervalScheduler: Scheduler for interval-based recurring execution

(6) SchedulerFactory: Factory for creating scheduler instances (Factory Pattern)

References
----------
croniter `"croniter Documentation"<https://github.com/kiorky/croniter>`_
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, List
from enum import Enum
### Configure logging
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: ScheduleType
### ------Enumeration of schedule types
###
### class: WorkflowScheduler
### ------Abstract base class for workflow schedulers (Strategy Pattern)
###
### class: ImmediateScheduler
### ------Scheduler for immediate, one-time execution
###
### class: CronScheduler
### ------Scheduler for cron-based recurring execution
###
### class: IntervalScheduler
### ------Scheduler for interval-based recurring execution
###
### class: SchedulerFactory
### ------Factory for creating scheduler instances (Factory Pattern)
###
######################################################################################################



class ScheduleType(Enum):
    '''Class Introduction:

        Enumeration of schedule types.
        
        Defines the available scheduling strategies for workflow execution.
    '''

    IMMEDIATE = "immediate"  # Run immediately (one-time)
    CRON = "cron"           # Cron-based scheduling
    INTERVAL = "interval"   # Interval-based scheduling
    ONCE = "once"          # Run once at a specific time



class WorkflowScheduler(ABC):
    '''Class Introduction:

        Abstract base class for workflow schedulers (Strategy Pattern).
        
        This class defines the interface for different scheduling strategies.
        Subclasses implement specific scheduling behaviors.
    '''


    def __init__(self, scheduler_id: str, name: str, config: Optional[Dict[str, Any]] = None):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - scheduler_id (str) - Unique identifier for the scheduler
            - name (str) - Human-readable name of the scheduler
            - config (dict) - Scheduler-specific configuration
            - _is_active (bool) - Flag indicating if scheduler is active
            - _next_run_time (datetime) - Next scheduled run time
        '''

        self.scheduler_id = scheduler_id
        self.name = name
        self.config = config or {}
        self._is_active = False
        self._next_run_time: Optional[datetime] = None
    

    @property
    def is_active(self) -> bool:
        '''Attribute Function:

            Check if the scheduler is currently active

        :parameters:
            - None

        :return:
            - bool - True if scheduler is active, False otherwise
        '''

        return self._is_active
    

    @property
    def next_run_time(self) -> Optional[datetime]:
        '''Attribute Function:

            Get the next scheduled run time

        :parameters:
            - None

        :return:
            - datetime - The next run time, or None if not scheduled
        '''

        return self._next_run_time
    

    @abstractmethod
    def schedule(self, workflow_id: str, task_factory: Callable, **kwargs) -> Any:
        '''Method Function:

            Schedule a workflow for execution

        :parameters:
            - workflow_id (str) - The workflow to schedule
            - task_factory (callable) - Factory function to create the task
            - **kwargs - Additional scheduling parameters

        :return:
            - Any - Schedule identifier or object
        '''

        pass
    

    @abstractmethod
    def unschedule(self, schedule_id: str) -> bool:
        '''Method Function:

            Remove a scheduled workflow

        :parameters:
            - schedule_id (str) - The schedule identifier to remove

        :return:
            - bool - True if successfully unscheduled
        '''

        pass
    

    @abstractmethod
    def get_next_run_time(self) -> Optional[datetime]:
        '''Method Function:

            Calculate the next scheduled run time

        :parameters:
            - None

        :return:
            - datetime - The next run time, or None if not scheduled
        '''

        pass
    

    def start(self) -> None:
        '''Method Function:

            Start the scheduler

        :parameters:
            - None

        :return:
            - None
        '''

        self._is_active = True
        logger.info(f"Scheduler {self.name} started")
    

    def stop(self) -> None:
        '''Method Function:

            Stop the scheduler

        :parameters:
            - None

        :return:
            - None
        '''

        self._is_active = False
        logger.info(f"Scheduler {self.name} stopped")
    

    def pause(self) -> None:
        '''Method Function:

            Pause the scheduler temporarily

        :parameters:
            - None

        :return:
            - None
        '''

        self._is_active = False
        logger.info(f"Scheduler {self.name} paused")
    

    def resume(self) -> None:
        '''Method Function:

            Resume a paused scheduler

        :parameters:
            - None

        :return:
            - None
        '''

        self._is_active = True
        logger.info(f"Scheduler {self.name} resumed")



class ImmediateScheduler(WorkflowScheduler):
    '''Class Introduction:

        Scheduler for immediate, one-time execution.
        
        This scheduler executes the workflow immediately when triggered.
        Used for trigger-based workflows like training.
    '''


    def __init__(self, scheduler_id: str, name: str = "Immediate Scheduler", config: Optional[Dict[str, Any]] = None):
        '''Attribute Function:

            Initialize the immediate scheduler

        :parameters:
            - scheduler_id (str) - Unique identifier for the scheduler
            - name (str) - Human-readable name of the scheduler
            - config (dict) - Scheduler-specific configuration
        '''

        super().__init__(scheduler_id, name, config)
    

    def schedule(self, workflow_id: str, task_factory: Callable, **kwargs) -> str:
        '''Method Function:

            Schedule immediate execution

        :parameters:
            - workflow_id (str) - The workflow to schedule
            - task_factory (callable) - Factory function to create the task
            - **kwargs - Additional scheduling parameters

        :return:
            - str - A unique schedule identifier
        '''

        import uuid
        schedule_id = f"immediate-{workflow_id}-{uuid.uuid4().hex[:8]}"
        self._next_run_time = datetime.now()
        logger.info(f"Scheduled immediate execution for workflow {workflow_id}")
        return schedule_id
    

    def unschedule(self, schedule_id: str) -> bool:
        '''Method Function:

            Unschedule an immediate execution (no-op for immediate scheduler)

        :parameters:
            - schedule_id (str) - The schedule identifier to remove

        :return:
            - bool - Always True as there's nothing to unschedule
        '''

        logger.debug(f"Unscheduling immediate execution: {schedule_id}")
        return True
    

    def get_next_run_time(self) -> Optional[datetime]:
        '''Method Function:

            Get next run time (always now for immediate scheduler)

        :parameters:
            - None

        :return:
            - datetime - Current time
        '''

        return datetime.now()



class CronScheduler(WorkflowScheduler):
    '''Class Introduction:

        Scheduler for cron-based recurring execution.
        
        This scheduler uses cron expressions to define execution schedules.
        Used for scheduled workflows like inference.
    '''


    def __init__(
        self,
        scheduler_id: str,
        name: str = "Cron Scheduler",
        cron_expression: str = "0 0 * * *",  # Daily at midnight
        config: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Initialize the cron scheduler

        :parameters:
            - scheduler_id (str) - Unique identifier for the scheduler
            - name (str) - Human-readable name of the scheduler
            - cron_expression (str) - The cron expression for scheduling
            - config (dict) - Scheduler-specific configuration
            - _schedules (dict) - Dictionary of active schedules
        '''

        super().__init__(scheduler_id, name, config)
        self.cron_expression = cron_expression
        self._schedules: Dict[str, Any] = {}
    

    def schedule(self, workflow_id: str, task_factory: Callable, **kwargs) -> str:
        '''Method Function:

            Schedule a workflow with a cron expression

        :parameters:
            - workflow_id (str) - The workflow to schedule
            - task_factory (callable) - Factory function to create the task
            - **kwargs - May include 'cron' to override default expression

        :return:
            - str - The schedule identifier
        '''

        import uuid
        cron = kwargs.get('cron', self.cron_expression)
        schedule_id = f"cron-{workflow_id}-{uuid.uuid4().hex[:8]}"
        self._schedules[schedule_id] = {
            "workflow_id": workflow_id,
            "cron": cron,
            "task_factory": task_factory,
            "created_at": datetime.now()
        }
        # Calculate next run time
        self._next_run_time = self._calculate_next_run(cron)
        logger.info(f"Scheduled workflow {workflow_id} with cron: {cron}")
        return schedule_id
    

    def unschedule(self, schedule_id: str) -> bool:
        '''Method Function:

            Remove a scheduled workflow

        :parameters:
            - schedule_id (str) - The schedule identifier to remove

        :return:
            - bool - True if successfully removed
        '''

        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            logger.info(f"Unscheduled: {schedule_id}")
            return True
        return False
    

    def get_next_run_time(self) -> Optional[datetime]:
        '''Method Function:

            Calculate the next scheduled run time based on cron expression

        :parameters:
            - None

        :return:
            - datetime - The next run time
        '''

        if not self._schedules:
            return None
        # Use the first schedule's cron expression
        first_schedule = next(iter(self._schedules.values()))
        return self._calculate_next_run(first_schedule["cron"])


    def _calculate_next_run(self, cron_expression: str) -> datetime:
        '''Method Function:

            Calculate the next run time from a cron expression
            
            This is a simplified implementation. In production, you might want
            to use a library like 'croniter' for more accurate calculations.

        :parameters:
            - cron_expression (str) - The cron expression (e.g., "0 0 * * *")

        :return:
            - datetime - The next scheduled run time
        '''

        try:
            # Try to use croniter if available
            from croniter import croniter
            return croniter(cron_expression, datetime.now()).get_next(datetime)
        except ImportError:
            # Fallback: simple interval-based approximation
            logger.warning("croniter not available, using fallback scheduling")
            # Parse simple cron: "*/n * * * *" means every n minutes
            parts = cron_expression.split()
            if len(parts) >= 2 and parts[0].startswith("*/"):
                try:
                    minutes = int(parts[0][2:])
                    return datetime.now() + timedelta(minutes=minutes)
                except ValueError:
                    pass
            # Default: schedule for 1 hour from now
            return datetime.now() + timedelta(hours=1)
    

    def list_schedules(self) -> List[Dict[str, Any]]:
        '''Method Function:

            List all active schedules

        :parameters:
            - None

        :return:
            - list - List of schedule information dictionaries
        '''

        return [
            {
                "schedule_id": sid,
                "workflow_id": info["workflow_id"],
                "cron": info["cron"],
                "created_at": info["created_at"]
            }
            for sid, info in self._schedules.items()
        ]



class IntervalScheduler(WorkflowScheduler):
    '''Class Introduction:

        Scheduler for interval-based recurring execution.
        
        This scheduler executes workflows at fixed time intervals.
    '''


    def __init__(
        self,
        scheduler_id: str,
        name: str = "Interval Scheduler",
        interval_minutes: int = 60,
        config: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Initialize the interval scheduler

        :parameters:
            - scheduler_id (str) - Unique identifier for the scheduler
            - name (str) - Human-readable name of the scheduler
            - interval_minutes (int) - Interval between executions in minutes
            - config (dict) - Scheduler-specific configuration
            - _schedules (dict) - Dictionary of active schedules
        '''

        super().__init__(scheduler_id, name, config)
        self.interval_minutes = interval_minutes
        self._schedules: Dict[str, Any] = {}
    

    def schedule(self, workflow_id: str, task_factory: Callable, **kwargs) -> str:
        '''Method Function:

            Schedule a workflow with an interval

        :parameters:
            - workflow_id (str) - The workflow to schedule
            - task_factory (callable) - Factory function to create the task
            - **kwargs - May include 'interval_minutes' to override default

        :return:
            - str - The schedule identifier
        '''

        import uuid
        interval = kwargs.get('interval_minutes', self.interval_minutes)
        schedule_id = f"interval-{workflow_id}-{uuid.uuid4().hex[:8]}"
        self._schedules[schedule_id] = {
            "workflow_id": workflow_id,
            "interval_minutes": interval,
            "task_factory": task_factory,
            "created_at": datetime.now(),
            "next_run": datetime.now() + timedelta(minutes=interval)
        }
        self._next_run_time = self._schedules[schedule_id]["next_run"]
        logger.info(f"Scheduled workflow {workflow_id} with interval: {interval} minutes")
        return schedule_id
    

    def unschedule(self, schedule_id: str) -> bool:
        '''Method Function:

            Remove a scheduled workflow

        :parameters:
            - schedule_id (str) - The schedule identifier to remove

        :return:
            - bool - True if successfully removed
        '''
        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            logger.info(f"Unscheduled: {schedule_id}")
            return True
        return False
    

    def get_next_run_time(self) -> Optional[datetime]:
        '''Method Function:

            Get the next scheduled run time

        :parameters:
            - None

        :return:
            - datetime - The next run time, or None if not scheduled
        '''
        if not self._schedules:
            return None
        # Return the earliest next run time
        return min(s["next_run"] for s in self._schedules.values())



class SchedulerFactory:
    '''Class Introduction:

        Factory for creating scheduler instances (Factory Pattern).
        
        This factory creates appropriate Scheduler objects based on schedule type.
    '''


    _scheduler_types: Dict[str, type] = {
        "immediate": ImmediateScheduler,
        "cron": CronScheduler,
        "interval": IntervalScheduler,
    }
    

    @classmethod
    def register_scheduler_type(cls, scheduler_type: str, scheduler_class: type) -> None:
        '''Method Function:

            Register a new scheduler type

        :parameters:
            - scheduler_type (str) - The type identifier for the scheduler
            - scheduler_class (type) - The scheduler class to instantiate

        :return:
            - None
        '''

        cls._scheduler_types[scheduler_type] = scheduler_class
        logger.debug(f"Registered scheduler type: {scheduler_type}")
    

    @classmethod
    def create(
        cls,
        scheduler_type: str,
        scheduler_id: Optional[str] = None,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> WorkflowScheduler:
        '''Method Function:

            Create a scheduler instance of the specified type

        :parameters:
            - scheduler_type (str) - The type of scheduler to create
            - scheduler_id (str) - Optional scheduler ID (generated if not provided)
            - name (str) - Optional scheduler name
            - config (dict) - Scheduler configuration

        :return:
            - WorkflowScheduler - The created scheduler instance

        :raises:
            - ValueError - If the scheduler type is not recognized
        '''

        import uuid
        if scheduler_type not in cls._scheduler_types:
            raise ValueError(f"Unknown scheduler type: {scheduler_type}")
        scheduler_id = scheduler_id or f"scheduler-{uuid.uuid4().hex[:8]}"
        name = name or f"{scheduler_type.title()} Scheduler"
        config = config or {}
        scheduler_class = cls._scheduler_types[scheduler_type]
        # Handle specific parameters for different scheduler types
        if scheduler_type == "cron" and "cron" in config:
            return scheduler_class(
                scheduler_id=scheduler_id,
                name=name,
                cron_expression=config.pop("cron"),
                config=config
            )
        elif scheduler_type == "interval" and "interval_minutes" in config:
            return scheduler_class(
                scheduler_id=scheduler_id,
                name=name,
                interval_minutes=config.pop("interval_minutes"),
                config=config
            )
        return scheduler_class(
            scheduler_id=scheduler_id,
            name=name,
            config=config
        )
    

    @classmethod
    def create_for_workflow_type(
        cls,
        workflow_type: str,
        scheduler_id: Optional[str] = None,
        **kwargs
    ) -> WorkflowScheduler:
        '''Method Function:

            Create an appropriate scheduler based on workflow type

        :parameters:
            - workflow_type (str) - "trigger" or "scheduled"
            - scheduler_id (str) - Optional scheduler ID
            - **kwargs - Additional configuration

        :return:
            - WorkflowScheduler - The appropriate scheduler for the workflow type
        '''

        if workflow_type == "trigger":
            return cls.create("immediate", scheduler_id, config=kwargs)
        elif workflow_type == "scheduled":
            # Default to interval scheduler for scheduled workflows
            config = {"interval_minutes": 60}
            config.update(kwargs)
            return cls.create("interval", scheduler_id, config=config)
        else:
            raise ValueError(f"Unknown workflow type: {workflow_type}")
    

    @classmethod
    def get_available_types(cls) -> List[str]:
        '''Method Function:

            Get a list of available scheduler types

        :parameters:
            - None

        :return:
            - list - List of available scheduler type strings
        '''

        return list(cls._scheduler_types.keys())



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
