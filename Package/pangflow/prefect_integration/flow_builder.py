# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: pangflow
# Copyright (c) 2026 The pangflow Authors. All rights reserved.



'''
Module Introduction
-----------------

Flow builder module for pangflow.

This module provides utilities for building Prefect flows from
pangflow workflow definitions. It enables conversion of CLI-based
workflows into Prefect orchestrated flows with tasks, schedules,
and execution monitoring.

- Design mode:

    (1) Builder pattern for flow construction

    (2) Method chaining for configuration

    (3) Template method pattern for flow generation

- Key points:

    (1) Wraps CLI commands as Prefect tasks and flows

    (2) Supports flow and task-level configuration

    (3) Creates execution artifacts for monitoring

    (4) Handles subprocess execution with logging

- Main functions:

    (1) Build Prefect flows from workflow definitions

    (2) Configure flow execution parameters

    (3) Configure task execution and caching

    (4) Build and serve deployments directly

    (5) Create simple flows with default configuration

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.prefect_integration.flow_builder import FlowBuilder, create_simple_flow

    ### Build a flow with custom configuration
    builder = FlowBuilder(
        workflow_id="wf-001",
        workflow_name="data-processing",
        command="python process_data.py"
    )
    
    flow = (
        builder
        .with_flow_config(timeout=3600, retries=3, log_prints=True)
        .with_task_config(timeout=1800)
        .build()
    )

    ### Execute the flow
    result = flow()

    ### Create and serve a deployment
    builder.build_and_serve(work_pool="default-process", cron="0 0 * * *")

    ### Quick simple flow creation
    flow = create_simple_flow("wf-002", "backup-job", "./backup.sh")

Description of Class and Function
---------------------------------
(1)FlowBuilder: Builder class for constructing Prefect flows from workflow definitions.
    - Uses builder pattern for flexible, chainable configuration
    - Wraps CLI commands as executable Prefect flows
    - Supports scheduling and deployment directly from builder

(2)create_simple_flow: Convenience function for quickly creating flows.
    - Creates a flow with default configuration
    - Useful for simple use cases without builder setup

References
----------
Prefect Flows: https://docs.prefect.io/latest/concepts/flows/
Prefect Tasks: https://docs.prefect.io/latest/concepts/tasks/
Prefect Scheduling: https://docs.prefect.io/latest/concepts/schedules/
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import logging
from typing import Optional, Dict, Any, Callable
### Prefect imports
try:
    from prefect import flow, task, get_run_logger
    from prefect.tasks import task_input_hash
    from prefect.artifacts import create_markdown_artifact
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False
    flow = None
    task = None
    get_run_logger = None
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: FlowBuilder
### ------Builder class for constructing Prefect flows from workflow definitions
###
### function: create_simple_flow
### ---------Convenience function for quickly creating flows with default configuration
###
######################################################################################################



class FlowBuilder:
    '''
    Class Introduction
    ------------------
    
    Builder for creating Prefect flows from pangflow workflows.
    
    This class uses the Builder pattern to construct Prefect flows
    that wrap CLI commands for execution on the Prefect platform.
    It provides a fluent, chainable API for configuring flow and
    task execution parameters.
    
    The builder creates:
    - A Prefect task for command execution with subprocess
    - A Prefect flow that orchestrates the task
    - Optional execution artifacts for monitoring
    
    Attributes
    ----------
    workflow_id : str
        Unique identifier for the workflow
    workflow_name : str
        Human-readable name of the workflow
    command : str
        CLI command to execute
    _flow_config : Dict[str, Any]
        Flow-level configuration options
    _task_config : Dict[str, Any]
        Task-level configuration options
    _schedule : Optional[Any]
        Optional schedule configuration
    '''
    

    def __init__(self, workflow_id: str, workflow_name: str, command: str):
        '''Attribute Function:
        
        Initialize the flow builder.
        
        Creates a new FlowBuilder instance with the basic workflow
        identification and command information. Validates that
        Prefect is installed before initialization.
        
        :parameters:
            - workflow_id (str) - Unique identifier for the workflow
            - workflow_name (str) - Human-readable name of the workflow
            - command (str) - CLI command to execute
        
        :return: 
            None
        '''

        if not PREFECT_AVAILABLE:
            raise RuntimeError(
                "Prefect is not installed. "
                "Please install it with: pip install prefect==3.6.21"
            )
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.command = command
        self._flow_config: Dict[str, Any] = {}
        self._task_config: Dict[str, Any] = {}
        self._schedule = None
    

    def with_flow_config(
        self,
        description: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: int = 0,
        retry_delay: int = 0,
        log_prints: bool = True
    ) -> 'FlowBuilder':
        '''Method Function:
        
        Configure the flow execution parameters.
        
        Sets flow-level configuration options including description,
        timeout, retry behavior, and logging preferences. Supports
        method chaining.
        
        :parameters:
            - description (Optional[str]) - Flow description
            - timeout (Optional[int]) - Flow timeout in seconds
            - retries (int) - Number of retries on failure (default: 0)
            - retry_delay (int) - Delay between retries in seconds (default: 0)
            - log_prints (bool) - Whether to log print statements (default: True)
        
        :return: 
            Self for method chaining
        
        :example:
            >>> builder.with_flow_config(timeout=3600, retries=3)
        '''

        self._flow_config = {
            "description": description or f"pangflow workflow: {self.workflow_name}",
            "timeout_seconds": timeout,
            "retries": retries,
            "retry_delay_seconds": retry_delay,
            "log_prints": log_prints,
        }
        return self
    

    def with_task_config(
        self,
        timeout: Optional[int] = None,
        cache_key_fn: Optional[Callable] = None,
        cache_expiration: Optional[int] = None
    ) -> 'FlowBuilder':
        '''Method Function:
        
        Configure the task execution parameters.
        
        Sets task-level configuration options including timeout and
        caching behavior. Supports method chaining.
        
        :parameters:
            - timeout (Optional[int]) - Task timeout in seconds
            - cache_key_fn (Optional[Callable]) - Function for cache key generation
            - cache_expiration (Optional[int]) - Cache expiration in seconds
        
        :return: Self for method chaining
        
        :example:
            >>> builder.with_task_config(timeout=1800, cache_key_fn=task_input_hash)
        '''

        self._task_config = {
            "timeout_seconds": timeout,
            "cache_key_fn": cache_key_fn,
            "cache_expiration": cache_expiration,
        }
        return self
    
    def with_schedule(self, schedule: Any) -> 'FlowBuilder':
        '''Method Function:
        
        Add a schedule to the flow.
        
        Attaches a Prefect schedule object to the flow configuration.
        Supports method chaining.
        
        :parameters:
            - schedule (Any) - Prefect schedule object
        
        :return: Self for method chaining
        
        :example:
            >>> from prefect.schedules import IntervalSchedule
            >>> builder.with_schedule(IntervalSchedule(interval=timedelta(hours=1)))
        '''

        self._schedule = schedule
        return self
    
    def build(self) -> Callable:
        '''Method Function:
        
        Build and return the Prefect flow function.
        
        Creates the Prefect task for command execution and the flow
        that orchestrates it. Applies all configured options and
        creates execution artifacts.
        
        :parameters: 
            None
        
        :return: 
            The decorated flow function ready for execution
        '''

        import subprocess
        import os
        
        # Create the task for command execution
        @task(
            name=f"{self.workflow_name}_execute",
            **{k: v for k, v in self._task_config.items() if v is not None}
        )
        def execute_command(
            cmd: str,
            working_dir: Optional[str] = None,
            env_vars: Optional[Dict[str, str]] = None
        ) -> Dict[str, Any]:
            """Execute the CLI command as a Prefect task."""
            run_logger = get_run_logger()
            run_logger.info(f"Executing command: {cmd}")
            # Prepare environment
            env = os.environ.copy()
            if env_vars:
                env.update(env_vars)
            # Execute command
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_dir,
                env=env
            )
            # Log results
            run_logger.info(f"Command completed with return code: {result.returncode}")
            if result.stdout:
                run_logger.info(f"STDOUT:\n{result.stdout}")
            if result.stderr:
                if result.returncode != 0:
                    run_logger.error(f"STDERR:\n{result.stderr}")
                else:
                    run_logger.warning(f"STDERR:\n{result.stderr}")
            # Create artifact with execution results
            artifact_content = f"""
# Workflow Execution: {self.workflow_name}

**Command:** `{cmd}`

**Return Code:** {result.returncode}

**Status:** {'✅ Success' if result.returncode == 0 else '❌ Failed'}

## Output

```
{result.stdout}
```

## Errors

```
{result.stderr}
```
"""
            try:
                create_markdown_artifact(
                    key=f"execution-{self.workflow_id}",
                    markdown=artifact_content,
                    description=f"Execution results for {self.workflow_name}"
                )
            except Exception as e:
                run_logger.warning(f"Failed to create artifact: {e}")
            if result.returncode != 0:
                raise RuntimeError(f"Command failed with code {result.returncode}: {result.stderr}")
            return {
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        # Create the flow
        @flow(
            name=self.workflow_name,
            **{k: v for k, v in self._flow_config.items() if v is not None}
        )
        def workflow_flow(
            working_dir: Optional[str] = None,
            env_vars: Optional[Dict[str, str]] = None
        ) -> Dict[str, Any]:
            """The main workflow flow function."""
            flow_logger = get_run_logger()
            flow_logger.info(f"Starting workflow: {self.workflow_name}")
            flow_logger.info(f"Workflow ID: {self.workflow_id}")
            # Execute the command
            result = execute_command(
                cmd=self.command,
                working_dir=working_dir,
                env_vars=env_vars
            )
            flow_logger.info(f"Workflow completed: {self.workflow_name}")
            return result
        return workflow_flow
    

    def build_and_serve(
        self,
        work_pool: str = "default-process",
        cron: Optional[str] = None,
        interval: Optional[int] = None
    ) -> str:
        '''Method Function:
        
        Build the flow and serve it as a deployment.
        
        This is a convenience method that builds the flow and immediately
        creates a deployment using Prefect's serve functionality.
        Blocks indefinitely while serving the deployment.
        
        :parameters:
            - work_pool (str) - Name of the work pool to use (default: "default-process")
            - cron (Optional[str]) - Optional cron schedule (e.g., "0 0 * * *")
            - interval (Optional[int]) - Optional interval in seconds

        :return: The deployment name
        '''

        from prefect import serve
        flow_func = self.build()
        deployment = flow_func.to_deployment(
            name=f"{self.workflow_name}-deployment",
            work_pool_name=work_pool,
            cron=cron,
            interval=interval,
        )
        # Serve the deployment
        serve(deployment)
        return f"{self.workflow_name}-deployment"



def create_simple_flow(workflow_id: str, workflow_name: str, command: str) -> Callable:
    '''Function Introduction:
    
    Create a simple Prefect flow from a workflow definition.
    
    This is a convenience function for quickly creating flows without
    using the builder pattern. Creates a flow with default configuration.
    
    :parameters:
        - workflow_id (str) - Unique identifier for the workflow
        - workflow_name (str) - Human-readable name of the workflow
        - command (str) - CLI command to execute
    
    :return:
        The flow function ready for execution
    
    :example:
        >>> flow = create_simple_flow("wf-001", "daily-backup", "./backup.sh")
        >>> result = flow()
    '''

    builder = FlowBuilder(workflow_id, workflow_name, command)
    return builder.build()



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
