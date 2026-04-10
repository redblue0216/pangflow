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

This is the task module for pangflow workflow engine.

- Design mode:

    (1) Template Method Pattern - Common task execution flow

    (2) Factory Pattern - Task creation through TaskFactory

- Key points:

    (1) Abstract base class WorkflowTask defines the task execution template

    (2) CliTask implements concrete CLI command execution

    (3) TaskFactory creates appropriate task instances

    (4) TaskStatus enum for execution status tracking

    (5) TaskResult dataclass for execution results

- Main functions:

    (1) Define task execution workflow with pre/post hooks

    (2) Execute CLI commands as subprocess

    (3) Track task status and results

    (4) Support timeout and environment variable configuration

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Create a CLI task using factory
    from pangflow.core.task import TaskFactory
    
    task = TaskFactory.create_cli_task(
        name="Training Task",
        command="python train.py --epochs 10",
        working_dir="/path/to/project",
        timeout=3600
    )
    
    ### Execute the task
    result = task.execute()
    print(f"Status: {result.status}")
    print(f"Output: {result.stdout}")

Description of Class and Function
-----------------
(1) TaskStatus: Enumeration of task execution statuses

(2) TaskResult: Data class representing the result of a task execution

(3) WorkflowTask: Abstract base class for workflow tasks (Template Method Pattern)

(4) CliTask: Concrete task implementation for executing CLI commands

(5) TaskFactory: Factory for creating task instances (Factory Pattern)

References
----------
Template Method Pattern `"Template Method Pattern"<https://refactoring.guru/design-patterns/template-method>`_
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
import subprocess
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
### Configure logging
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: TaskStatus
### ------Enumeration of task execution statuses
###
### class: TaskResult
### ------Data class representing the result of a task execution
###
### class: WorkflowTask
### ------Abstract base class for workflow tasks (Template Method Pattern)
###
### class: CliTask
### ------Concrete task implementation for executing CLI commands
###
### class: TaskFactory
### ------Factory for creating task instances (Factory Pattern)
###
######################################################################################################



class TaskStatus(Enum):
    '''Class Introduction:

        Enumeration of task execution statuses.
        
        Defines all possible states during task execution lifecycle.
    '''


    PENDING = "pending"     # Task is pending execution
    RUNNING = "running"     # Task is currently running
    SUCCESS = "success"     # Task completed successfully
    FAILED = "failed"       # Task execution failed
    CANCELLED = "cancelled" # Task was cancelled




@dataclass
class TaskResult:
    '''Class Introduction:

        Data class representing the result of a task execution.
        
        Contains all information about the execution outcome including
        status, output, timing, and metadata.
    '''

    task_id: str
    status: TaskStatus
    return_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    

    @property
    def duration_seconds(self) -> Optional[float]:
        '''Attribute Function:

            Calculate the execution duration in seconds

        :parameters:
            - None

        :return:
            - float - Duration in seconds, or None if timing not available
        '''

        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    

    def to_dict(self) -> Dict[str, Any]:
        '''Method Function:

            Convert the result to a dictionary

        :parameters:
            - None

        :return:
            - dict - Dictionary representation of the task result
        '''
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "return_code": self.return_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "metadata": self.metadata,
        }



class WorkflowTask(ABC):
    '''Class Introduction:

        Abstract base class for workflow tasks (Template Method Pattern).
        
        This class defines the template for task execution, with hooks for
        subclasses to customize specific steps.
    '''


    def __init__(
        self,
        task_id: str,
        name: str,
        command: str,
        working_dir: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - task_id (str) - Unique identifier for the task
            - name (str) - Human-readable name of the task
            - command (str) - Command line string to execute
            - working_dir (str) - Working directory for command execution
            - env_vars (dict) - Environment variables to set for execution
            - timeout (int) - Timeout in seconds (None for no timeout)
            - metadata (dict) - Additional task metadata
            - _result (TaskResult) - Execution result (available after execution)
        '''

        self.task_id = task_id
        self.name = name
        self.command = command
        self.working_dir = working_dir
        self.env_vars = env_vars or {}
        self.timeout = timeout
        self.metadata = metadata or {}
        self._result: Optional[TaskResult] = None
    

    @property
    def result(self) -> Optional[TaskResult]:
        '''Attribute Function:

            Get the execution result (available after execution)

        :parameters:
            - None

        :return:
            - TaskResult - The execution result, or None if not executed
        '''

        return self._result
    

    def execute(self) -> TaskResult:
        '''Method Function:

            Execute the task following the template method pattern
            
            Steps:
            1. Pre-execution hook (_before_execute)
            2. Execute the task (_do_execute)
            3. Post-execution hook (_after_execute)

        :parameters:
            - None

        :return:
            - TaskResult - The result of the task execution
        '''

        # Step 1: Pre-execution hook
        self._before_execute()
        # Step 2: Execute the task
        try:
            self._result = self._do_execute()
        except Exception as e:
            logger.exception(f"Task {self.task_id} execution failed")
            self._result = TaskResult(
                task_id=self.task_id,
                status=TaskStatus.FAILED,
                stderr=str(e),
                completed_at=datetime.now()
            )
        # Step 3: Post-execution hook
        self._after_execute(self._result)
        return self._result
    

    def _before_execute(self) -> None:
        '''Method Function:

            Hook method called before task execution
            
            Subclasses can override this to perform setup operations.

        :parameters:
            - None

        :return:
            - None
        '''
        
        logger.info(f"Preparing to execute task: {self.name} (ID: {self.task_id})")
        logger.debug(f"Command: {self.command}")
    

    @abstractmethod
    def _do_execute(self) -> TaskResult:
        '''Method Function:

            Abstract method for the actual task execution
            
            Subclasses must implement this method to define how the task is executed.

        :parameters:
            - None

        :return:
            - TaskResult - The result of the task execution
        '''
        
        pass
    

    def _after_execute(self, result: TaskResult) -> None:
        '''Method Function:

            Hook method called after task execution
            
            Subclasses can override this to perform cleanup or result processing.

        :parameters:
            - result (TaskResult) - The result of the task execution

        :return:
            - None
        '''

        status_str = result.status.value.upper()
        duration = result.duration_seconds
        duration_str = f" ({duration:.2f}s)" if duration else ""
        logger.info(f"Task {self.task_id} completed with status: {status_str}{duration_str}")



class CliTask(WorkflowTask):
    '''Class Introduction:

        Concrete task implementation for executing CLI commands.
        
        This class implements the _do_execute method to run command line commands
        using subprocess.
    '''


    def __init__(
        self,
        task_id: str,
        name: str,
        command: str,
        working_dir: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        shell: bool = True,
        capture_output: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Initialize a CLI task

        :parameters:
            - task_id (str) - Unique identifier for the task
            - name (str) - Human-readable name of the task
            - command (str) - Command line string to execute
            - working_dir (str) - Working directory for command execution
            - env_vars (dict) - Environment variables to set for execution
            - timeout (int) - Timeout in seconds
            - shell (bool) - Whether to use shell execution
            - capture_output (bool) - Whether to capture stdout/stderr
            - metadata (dict) - Additional task metadata
        '''

        super().__init__(task_id, name, command, working_dir, env_vars, timeout, metadata)
        self.shell = shell
        self.capture_output = capture_output
    

    def _do_execute(self) -> TaskResult:
        '''Method Function:

            Execute the CLI command using subprocess

        :parameters:
            - None

        :return:
            - TaskResult - The result of the command execution
        '''

        started_at = datetime.now()
        try:
            # Log the execution
            logger.info(f"Executing command: {self.command}")
            # Prepare environment variables
            import os
            env = os.environ.copy()
            env.update(self.env_vars)
            # Execute the command
            process = subprocess.Popen(
                self.command,
                shell=self.shell,
                cwd=self.working_dir,
                env=env,
                stdout=subprocess.PIPE if self.capture_output else None,
                stderr=subprocess.PIPE if self.capture_output else None,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            # Wait for completion with timeout
            try:
                stdout, stderr = process.communicate(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                logger.warning(f"Task {self.task_id} timed out after {self.timeout}s")
            completed_at = datetime.now()
            # Determine status based on return code
            if process.returncode == 0:
                status = TaskStatus.SUCCESS
            elif process.returncode == -1:  # Typically indicates timeout/kill
                status = TaskStatus.CANCELLED
            else:
                status = TaskStatus.FAILED
            return TaskResult(
                task_id=self.task_id,
                status=status,
                return_code=process.returncode,
                stdout=stdout or "",
                stderr=stderr or "",
                started_at=started_at,
                completed_at=completed_at,
                metadata={
                    "command": self.command,
                    "working_dir": self.working_dir,
                    "shell": self.shell,
                }
            )
        except Exception as e:
            logger.exception(f"Error executing task {self.task_id}")
            return TaskResult(
                task_id=self.task_id,
                status=TaskStatus.FAILED,
                return_code=-1,
                stderr=str(e),
                started_at=started_at,
                completed_at=datetime.now(),
                metadata={"error": str(e)}
            )
        


class TaskFactory:
    '''Class Introduction:

        Factory for creating task instances (Factory Pattern).
        
        This factory creates appropriate Task objects based on task type and configuration.
    '''


    _task_types: Dict[str, type] = {
        "cli": CliTask,
    }

    
    @classmethod
    def register_task_type(cls, task_type: str, task_class: type) -> None:
        '''Method Function:

            Register a new task type

        :parameters:
            - task_type (str) - The type identifier for the task
            - task_class (type) - The task class to instantiate

        :return:
            - None
        '''

        cls._task_types[task_type] = task_class
        logger.debug(f"Registered task type: {task_type}")
    

    @classmethod
    def create(
        cls,
        task_type: str,
        name: str,
        command: str,
        task_id: Optional[str] = None,
        **kwargs
    ) -> WorkflowTask:
        '''Method Function:

            Create a task instance of the specified type

        :parameters:
            - task_type (str) - The type of task to create (e.g., "cli")
            - name (str) - Human-readable name of the task
            - command (str) - Command line string to execute
            - task_id (str) - Optional task ID (generated if not provided)
            - **kwargs - Additional arguments passed to the task constructor

        :return:
            - WorkflowTask - The created task instance
        '''

        if task_type not in cls._task_types:
            raise ValueError(f"Unknown task type: {task_type}")
        task_id = task_id or str(uuid.uuid4())
        task_class = cls._task_types[task_type]
        return task_class(
            task_id=task_id,
            name=name,
            command=command,
            **kwargs
        )
    

    @classmethod
    def create_cli_task(
        cls,
        name: str,
        command: str,
        working_dir: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        **kwargs
    ) -> CliTask:
        '''Method Function:

            Convenience method to create a CLI task

        :parameters:
            - name (str) - Human-readable name of the task
            - command (str) - Command line string to execute
            - working_dir (str) - Working directory for command execution
            - env_vars (dict) - Environment variables to set
            - timeout (int) - Timeout in seconds
            - **kwargs - Additional arguments

        :return:
            - CliTask - The created CLI task instance
        '''

        task_id = str(uuid.uuid4())
        return CliTask(
            task_id=task_id,
            name=name,
            command=command,
            working_dir=working_dir,
            env_vars=env_vars,
            timeout=timeout,
            **kwargs
        )
    

    @classmethod
    def get_available_types(cls) -> List[str]:
        '''Method Function:

            Get a list of available task types

        :parameters:
            - None

        :return:
            - list - List of available task type strings
        '''

        return list(cls._task_types.keys())



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
