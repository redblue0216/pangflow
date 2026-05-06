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

This is the runner module for pangflow workflow engine.

- Design mode:

    (1) Strategy Pattern - Different runner implementations

    (2) Factory Pattern - Runner creation through RunnerFactory

    (3) Dependency Injection - ExecutionContext for flexible configuration

- Key points:

    (1) Abstract base class WorkflowRunner defines the runner interface

    (2) LocalRunner executes workflows in the local process

    (3) PrefectRunner integrates with Prefect platform for remote execution

    (4) RunnerFactory creates appropriate runner instances

    (5) RunnerContainer manages runner dependencies

- Main functions:

    (1) Execute workflows using different strategies

    (2) Track execution history and status

    (3) Support for stopping workflow execution

    (4) Integration with Prefect for distributed execution

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Create a local runner
    from pangflow.core.runner import RunnerFactory, ExecutionContext
    
    runner = RunnerFactory.create("local", name="My Runner")
    
    context = ExecutionContext(
        workflow_id="wf-001",
        workflow_name="Training Workflow",
        workflow_type="trigger",
        command="python train.py"
    )
    
    result = runner.run(context)

Description of Class and Function
-----------------
(1) ExecutionContext: Context object for workflow execution (Dependency Injection)

(2) WorkflowRunner: Abstract base class for workflow runners (Strategy Pattern)

(3) LocalRunner: Local workflow runner that executes workflows in the local process

(4) PrefectRunner: Prefect-based workflow runner for remote execution

(5) RunnerFactory: Factory for creating runner instances (Factory Pattern)

(6) RunnerContainer: Container for managing runner dependencies (Dependency Injection)

References
----------
Prefect `"Prefect Documentation"<https://docs.prefect.io/>`_
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
### Core imports
from pangflow.core.task import WorkflowTask, TaskResult, TaskFactory, TaskStatus
from pangflow.core.scheduler import WorkflowScheduler, SchedulerFactory
### Configure logging
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: ExecutionContext
### ------Context object for workflow execution (Dependency Injection)
###
### class: WorkflowRunner
### ------Abstract base class for workflow runners (Strategy Pattern)
###
### class: LocalRunner
### ------Local workflow runner that executes workflows in the local process
###
### class: PrefectRunner
### ------Prefect-based workflow runner for remote execution
###
### class: RunnerFactory
### ------Factory for creating runner instances (Factory Pattern)
###
### class: RunnerContainer
### ------Container for managing runner dependencies (Dependency Injection)
###
######################################################################################################



@dataclass
class ExecutionContext:
    '''Class Introduction:

        Context object for workflow execution (Dependency Injection).
        
        This class holds all dependencies and configuration needed for
        workflow execution, allowing for flexible dependency injection.
    '''

    workflow_id: str
    workflow_name: str
    workflow_type: str
    command: str
    working_dir: Optional[str] = None
    env_vars: Dict[str, str] = field(default_factory=dict)
    timeout: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Execution tracking
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None



class WorkflowRunner(ABC):
    '''Class Introduction:

        Abstract base class for workflow runners (Strategy Pattern).
        
        This class defines the interface for different workflow execution strategies.
        Subclasses implement specific execution behaviors.
    '''


    def __init__(
        self,
        runner_id: str,
        name: str,
        config: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - runner_id (str) - Unique identifier for the runner
            - name (str) - Human-readable name of the runner
            - config (dict) - Runner-specific configuration
            - _is_running (bool) - Flag indicating if runner is executing
            - _execution_history (list) - History of workflow executions
        '''

        self.runner_id = runner_id
        self.name = name
        self.config = config or {}
        self._is_running = False
        self._execution_history: List[Dict[str, Any]] = []
    

    @property
    def is_running(self) -> bool:
        '''Attribute Function:

            Check if the runner is currently executing a workflow

        :parameters:
            - None

        :return:
            - bool - True if runner is executing, False otherwise
        '''

        return self._is_running
    

    @abstractmethod
    def run(self, context: ExecutionContext) -> TaskResult:
        '''Method Function:

            Execute a workflow

        :parameters:
            - context (ExecutionContext) - The execution context containing workflow details

        :return:
            - TaskResult - The result of the workflow execution
        '''

        pass
    

    @abstractmethod
    def stop(self) -> bool:
        '''Method Function:

            Stop the current execution

        :parameters:
            - None

        :return:
            - bool - True if successfully stopped
        '''

        pass
    

    def get_execution_history(self) -> List[Dict[str, Any]]:
        '''Method Function:

            Get the history of workflow executions

        :parameters:
            - None

        :return:
            - list - List of execution history records
        '''

        return self._execution_history.copy()
    

    def _record_execution(self, context: ExecutionContext, result: TaskResult) -> None:
        '''Method Function:

            Record an execution in the history

        :parameters:
            - context (ExecutionContext) - The execution context
            - result (TaskResult) - The result of the execution

        :return:
            - None
        '''

        self._execution_history.append({
            "run_id": context.run_id,
            "workflow_id": context.workflow_id,
            "workflow_name": context.workflow_name,
            "started_at": context.started_at,
            "completed_at": context.completed_at,
            "status": result.status.value,
            "return_code": result.return_code,
        })



class LocalRunner(WorkflowRunner):
    '''Class Introduction:

        Local workflow runner that executes workflows in the local process.
        
        This runner creates a task and executes it directly using the TaskFactory.
    '''

    def __init__(
        self,
        runner_id: str,
        name: str = "Local Runner",
        config: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Initialize the local runner

        :parameters:
            - runner_id (str) - Unique identifier for the runner
            - name (str) - Human-readable name of the runner
            - config (dict) - Runner-specific configuration
            - _current_task (WorkflowTask) - Currently executing task
        '''

        super().__init__(runner_id, name, config)
        self._current_task: Optional[WorkflowTask] = None
    
    def run(self, context: ExecutionContext) -> TaskResult:
        '''Method Function:

            Execute a workflow locally

        :parameters:
            - context (ExecutionContext) - The execution context

        :return:
            - TaskResult - The result of the execution
        '''

        self._is_running = True
        context.started_at = datetime.now()
        try:
            logger.info(f"Starting local execution for workflow: {context.workflow_name}")
            # Create a task using the TaskFactory
            self._current_task = TaskFactory.create_cli_task(
                name=f"{context.workflow_name}-{context.run_id}",
                command=context.command,
                working_dir=context.working_dir,
                env_vars=context.env_vars,
                timeout=context.timeout or self.config.get("timeout")
            )
            # Execute the task
            result = self._current_task.execute()
            context.completed_at = datetime.now()
            self._record_execution(context, result)
            logger.info(
                f"Workflow {context.workflow_name} completed with status: {result.status.value}"
            )
            return result
        except Exception as e:
            logger.exception(f"Error executing workflow {context.workflow_name}")
            result = TaskResult(
                task_id=str(uuid.uuid4()),
                status=TaskStatus.FAILED,
                stderr=str(e),
                completed_at=datetime.now()
            )
            self._record_execution(context, result)
            return result
        finally:
            self._is_running = False
            self._current_task = None
    

    def stop(self) -> bool:
        '''Method Function:

            Stop the current execution
            
            Note: For local runner, this is a best-effort operation as
            subprocess termination may not be immediate.

        :parameters:
            - None

        :return:
            - bool - True if a task was running and stopped
        '''

        if self._current_task and self._is_running:
            logger.warning("Attempting to stop current task (best effort)")
            # Note: Actual subprocess termination would require storing
            # the subprocess object in the task
            self._is_running = False
            return True
        return False



class PrefectRunner(WorkflowRunner):
    '''Class Introduction:

        Prefect-based workflow runner.
        
        This runner integrates with Prefect to execute workflows through
        the Prefect platform, enabling remote execution and monitoring.
    '''


    def __init__(
        self,
        runner_id: str,
        name: str = "Prefect Runner",
        config: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Initialize the Prefect runner

        :parameters:
            - runner_id (str) - Unique identifier for the runner
            - name (str) - Human-readable name of the runner
            - config (dict) - Runner-specific configuration
            - _prefect_flow - Prefect flow object
            - _prefect_deployment - Prefect deployment object
            - _current_run_id (str) - Current flow run identifier
        '''

        super().__init__(runner_id, name, config)
        self._prefect_flow = None
        self._prefect_deployment = None
        self._current_run_id: Optional[str] = None
    

    def run(self, context: ExecutionContext) -> TaskResult:
        '''Method Function:

            Execute a workflow through Prefect

        :parameters:
            - context (ExecutionContext) - The execution context

        :return:
            - TaskResult - The result of the execution
        '''

        self._is_running = True
        context.started_at = datetime.now()
        try:
            logger.info(f"Starting Prefect execution for workflow: {context.workflow_name}")
            # Import Prefect modules here to avoid dependency issues if not installed
            from prefect import flow, get_run_logger
            from prefect.states import State, StateType
            # Create or get the flow
            flow_func = self._get_or_create_flow(context)
            # Run the flow
            self._current_run_id = str(uuid.uuid4())
            state = flow_func()
            # Convert Prefect state to TaskResult
            result = self._convert_prefect_state(state, context)
            context.completed_at = datetime.now()
            self._record_execution(context, result)
            logger.info(
                f"Workflow {context.workflow_name} completed via Prefect with status: {result.status.value}"
            )
            return result
        except Exception as e:
            logger.exception(f"Error in Prefect execution for {context.workflow_name}")
            result = TaskResult(
                task_id=str(uuid.uuid4()),
                status=TaskStatus.FAILED,
                stderr=str(e),
                completed_at=datetime.now()
            )
            self._record_execution(context, result)
            return result
        finally:
            self._is_running = False
    

    def _get_or_create_flow(self, context: ExecutionContext):
        '''Method Function:

            Create or retrieve a Prefect flow for the workflow

        :parameters:
            - context (ExecutionContext) - The execution context

        :return:
            - The Prefect flow function
        '''

        from prefect import flow, get_run_logger
        import subprocess
        @flow(name=context.workflow_name, flow_run_name=f"{context.workflow_name}-{context.run_id}")
        def workflow_flow():
            logger = get_run_logger()
            logger.info(f"Executing command: {context.command}")
            # Execute the command
            result = subprocess.run(
                context.command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=context.working_dir,
                timeout=context.timeout
            )
            logger.info(f"Command completed with return code: {result.returncode}")
            if result.returncode != 0:
                raise RuntimeError(f"Command failed: {result.stderr}")
            return result
        return workflow_flow
    

    def _convert_prefect_state(self, state, context: ExecutionContext) -> TaskResult:
        '''Method Function:

            Convert a Prefect state to a TaskResult

        :parameters:
            - state - The Prefect state object
            - context (ExecutionContext) - The execution context

        :return:
            - TaskResult - The converted result
        '''

        from prefect.states import StateType
        # Map Prefect state types to TaskStatus
        status_map = {
            StateType.COMPLETED: TaskStatus.SUCCESS,
            StateType.FAILED: TaskStatus.FAILED,
            StateType.CRASHED: TaskStatus.FAILED,
            StateType.CANCELLED: TaskStatus.CANCELLED,
            StateType.PENDING: TaskStatus.PENDING,
            StateType.RUNNING: TaskStatus.RUNNING,
        }
        status = status_map.get(state.type, TaskStatus.FAILED)
        return TaskResult(
            task_id=self._current_run_id or str(uuid.uuid4()),
            status=status,
            return_code=0 if status == TaskStatus.SUCCESS else 1,
            stdout=str(state.result()) if hasattr(state, 'result') else "",
            stderr=state.message() if hasattr(state, 'message') else "",
            started_at=context.started_at,
            completed_at=datetime.now(),
            metadata={
                "prefect_state_type": state.type.value if hasattr(state.type, 'value') else str(state.type),
                "workflow_id": context.workflow_id,
            }
        )
    

    def stop(self) -> bool:
        '''Method Function:

            Stop the current Prefect flow run

        :parameters:
            - None

        :return:
            - bool - True if stop was initiated
        '''

        if self._current_run_id:
            logger.info(f"Stopping Prefect flow run: {self._current_run_id}")
            # Prefect flow cancellation would be implemented here
            self._is_running = False
            return True
        return False



class RunnerFactory:
    '''Class Introduction:

        Factory for creating runner instances (Factory Pattern).
        
        This factory creates appropriate Runner objects based on runner type.
    '''


    _runner_types: Dict[str, type] = {
        "local": LocalRunner,
        "prefect": PrefectRunner,
    }
    

    @classmethod
    def register_runner_type(cls, runner_type: str, runner_class: type) -> None:
        '''Method Function:

            Register a new runner type

        :parameters:
            - runner_type (str) - The type identifier for the runner
            - runner_class (type) - The runner class to instantiate

        :return:
            - None
        '''

        cls._runner_types[runner_type] = runner_class
        logger.debug(f"Registered runner type: {runner_type}")
    

    @classmethod
    def create(
        cls,
        runner_type: str,
        runner_id: Optional[str] = None,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> WorkflowRunner:
        '''Method Function:

            Create a runner instance of the specified type

        :parameters:
            - runner_type (str) - The type of runner to create (e.g., "local", "prefect")
            - runner_id (str) - Optional runner ID (generated if not provided)
            - name (str) - Optional runner name
            - config (dict) - Runner configuration

        :return:
            - WorkflowRunner - The created runner instance

        :raises:
            - ValueError - If the runner type is not recognized
        '''

        if runner_type not in cls._runner_types:
            raise ValueError(f"Unknown runner type: {runner_type}")
        
        runner_id = runner_id or f"runner-{uuid.uuid4().hex[:8]}"
        name = name or f"{runner_type.title()} Runner"
        config = config or {}
        
        runner_class = cls._runner_types[runner_type]
        return runner_class(
            runner_id=runner_id,
            name=name,
            config=config
        )
    

    @classmethod
    def create_with_scheduler(
        cls,
        runner_type: str,
        scheduler_type: str,
        runner_id: Optional[str] = None,
        scheduler_id: Optional[str] = None,
        **kwargs
    ) -> tuple[WorkflowRunner, WorkflowScheduler]:
        '''Method Function:

            Create a runner and scheduler pair

        :parameters:
            - runner_type (str) - Type of runner to create
            - scheduler_type (str) - Type of scheduler to create
            - runner_id (str) - Optional runner ID
            - scheduler_id (str) - Optional scheduler ID
            - **kwargs - Configuration for both runner and scheduler

        :return:
            - tuple - (WorkflowRunner, WorkflowScheduler) pair
        '''

        runner_config = kwargs.get("runner_config", {})
        scheduler_config = kwargs.get("scheduler_config", {})
        runner = cls.create(runner_type, runner_id, config=runner_config)
        scheduler = SchedulerFactory.create(scheduler_type, scheduler_id, config=scheduler_config)
        return runner, scheduler
    

    @classmethod
    def get_available_types(cls) -> List[str]:
        '''Method Function:

            Get a list of available runner types

        :parameters:
            - None

        :return:
            - list - List of available runner type strings
        '''

        return list(cls._runner_types.keys())



class RunnerContainer:
    '''Class Introduction:

        Container for managing runner dependencies (Dependency Injection).
        
        This class provides a central registry for runner instances and
        their dependencies, enabling easy access and management.
    '''


    def __init__(self):
        '''Attribute Function:

            Initialize the runner container

        :parameters:
            - _runners (dict) - Dictionary of registered runners
            - _default_runner (str) - ID of the default runner
        '''

        self._runners: Dict[str, WorkflowRunner] = {}
        self._default_runner: Optional[str] = None
    

    def register(
        self,
        runner: WorkflowRunner,
        as_default: bool = False
    ) -> None:
        '''Method Function:

            Register a runner in the container

        :parameters:
            - runner (WorkflowRunner) - The runner to register
            - as_default (bool) - Whether to set as the default runner

        :return:
            - None
        '''

        self._runners[runner.runner_id] = runner
        if as_default or self._default_runner is None:
            self._default_runner = runner.runner_id
        logger.debug(f"Registered runner: {runner.runner_id}")
    

    def get(self, runner_id: Optional[str] = None) -> WorkflowRunner:
        '''Method Function:

            Get a runner from the container

        :parameters:
            - runner_id (str) - The runner ID, or None for default

        :return:
            - WorkflowRunner - The requested runner

        :raises:
            - KeyError - If the runner is not found
        '''

        if runner_id is None:
            runner_id = self._default_runner
        if runner_id is None:
            raise KeyError("No default runner configured")
        return self._runners[runner_id]
    

    def remove(self, runner_id: str) -> bool:
        '''Method Function:

            Remove a runner from the container

        :parameters:
            - runner_id (str) - The ID of the runner to remove

        :return:
            - bool - True if removed successfully, False otherwise
        '''

        if runner_id in self._runners:
            del self._runners[runner_id]
            if self._default_runner == runner_id:
                self._default_runner = next(iter(self._runners)) if self._runners else None
            return True
        return False
    

    def list_runners(self) -> List[str]:
        '''Method Function:

            List all registered runner IDs

        :parameters:
            - None

        :return:
            - list - List of registered runner IDs
        '''

        return list(self._runners.keys())



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
