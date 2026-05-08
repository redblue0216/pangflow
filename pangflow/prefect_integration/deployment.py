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

Deployment manager module for pangflow.

This module provides functionality for creating and managing Prefect deployments
for pangflow workflows. It handles the complete deployment lifecycle from
creation to deletion, including scheduled deployments and serve processes.

- Design mode:

    (1) Manager pattern for deployment lifecycle

    (2) Template generation for flow files

    (3) Subprocess management for serve processes

- Key points:

    (1) Creates Python flow files dynamically for Prefect 3.x compatibility

    (2) Supports cron and interval scheduling

    (3) Manages serve processes in background

    (4) Tracks deployment state internally

- Main functions:

    (1) Create deployments from workflow state

    (2) Deploy workflows to Prefect server

    (3) Trigger deployment runs

    (4) Delete deployments

    (5) List and get deployment information

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.prefect_integration.deployment import DeploymentManager, quick_deploy
    from pangflow.core.state import WorkflowState

    ### Create deployment manager
    manager = DeploymentManager(work_pool="default-process")

    ### Deploy a workflow
    result = manager.deploy_to_server(workflow_state=my_workflow, schedule_cron="0 0 * * *")

    ### Trigger a deployment
    trigger_result = manager.trigger_deployment(workflow_id="wf-123")

    ### Quick deploy without manager instance
    result = quick_deploy(workflow_state=my_workflow, schedule_cron="0 */6 * * *")

Description of Class and Function
---------------------------------
(1)DeploymentManager: Manager class for Prefect deployments.
    - Handles creation, update, and deletion of Prefect deployments
    - Manages serve processes and scheduling configuration

(2)quick_deploy: Convenience function for one-off deployments.
    - Creates a temporary DeploymentManager and deploys a workflow
    - Useful for simple deployment scenarios without manager lifecycle

References
----------
Prefect Deployments: https://docs.prefect.io/latest/concepts/deployments/
Prefect Serve: https://docs.prefect.io/latest/concepts/flows/#serving-a-flow
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
### Internal package
from pangflow.core.state import WorkflowState, WorkflowType
from pangflow.prefect_integration.flow_builder import FlowBuilder
### Prefect imports
try:
    from prefect import flow
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: DeploymentManager
### ------Manager class for creating and managing Prefect deployments
###
### function: quick_deploy
### ---------Convenience function for one-off deployments without manager instance
###
######################################################################################################



class DeploymentManager:
    '''
    Class Introduction
    ------------------
    
    Manager for Prefect deployments.
    
    This class handles the creation, update, and deletion of Prefect deployments
    for pangflow workflows. It manages the complete deployment lifecycle including:
    - Creating flow files compatible with Prefect 3.x
    - Configuring schedules (cron or interval)
    - Starting and managing serve processes
    - Tracking deployment metadata
    
    The manager uses the `serve()` approach recommended for Prefect 3.x,
    which creates deployments and runs a process to serve them.
    
    Attributes
    ----------
    work_pool : str
        Name of the Prefect work pool to use
    _deployments : Dict[str, Any]
        Internal registry of managed deployments
    '''
    

    def __init__(self, work_pool: str = "default-process"):
        '''Attribute Function:
        
        Initialize the deployment manager.
        
        Creates a new DeploymentManager instance configured for the
        specified work pool. Validates that Prefect is installed.
        
        :parameters:
            - work_pool (str) - Name of the Prefect work pool to use (default: "default-process")
        
        :return: 
            None
        '''

        if not PREFECT_AVAILABLE:
            raise RuntimeError(
                "Prefect is not installed. "
                "Please install it with: pip install prefect==3.6.21"
            )
        self.work_pool = work_pool
        self._deployments: Dict[str, Any] = {}
    

    def _create_flow_file(self, workflow_state: WorkflowState, flow_file_path: Path, 
                          schedule_cron: Optional[str] = None, 
                          schedule_interval: Optional[int] = None,
                          workspace_path: Optional[Path] = None) -> str:
        '''Method Function:
        
        Create a temporary Python file containing the flow definition with serve.
        
        Generates a complete Python script that defines a Prefect flow
        wrapping the workflow's CLI command. Prefect 3.x requires the flow
        to be defined in a file within the current working directory.
        
        :parameters:
            - workflow_state (WorkflowState) - The workflow state containing command and metadata
            - flow_file_path (Path) - Path where the flow file should be created
            - schedule_cron (Optional[str]) - Optional cron schedule string
            - schedule_interval (Optional[int]) - Optional interval in seconds
        
        :return: 
            The flow function name used in the generated file
        '''

        # Escape backslashes and quotes in the command
        escaped_command = workflow_state.command.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")
        flow_func_name = workflow_state.workflow_name.replace("-", "_").replace(" ", "_")
        # Build schedule config
        schedule_config = ""
        if schedule_cron:
            schedule_config = f'    cron="{schedule_cron}",\n'
        elif schedule_interval:
            schedule_config = f'    interval={schedule_interval},\n'
        elif workflow_state.workflow_type == WorkflowType.SCHEDULED:
            schedule_config = '    interval=3600,\n'  # Default 1 hour
        # Embed workspace path so the flow file can locate the DB even when
        # running in a subprocess with a different cwd.
        ws_path_str = str(workspace_path).replace("\\", "\\\\") if workspace_path else ""
        lines = [
            '"""',
            f'Auto-generated Prefect flow for pangflow workflow: {workflow_state.workflow_name}',
            '"""',
            'import subprocess',
            'import os',
            'import sys',
            'import uuid',
            'from datetime import datetime',
            'from typing import Optional, Dict, Any',
            'from prefect import flow, task, get_run_logger, serve',
            '',
            f'WORKFLOW_ID = "{workflow_state.workflow_id}"',
            f'WORKFLOW_NAME = "{workflow_state.workflow_name}"',
            f'COMMAND = "{escaped_command}"',
            f'WORKSPACE_PATH = "{ws_path_str}"',
            '',
            '',
            'def _record_execution(status: str, return_code: int = 0, stdout: str = "", stderr: str = "", run_id: str = "") -> None:',
            '    """Write or update an execution log in the pangflow SQLite database."""',
            '    try:',
            '        from pangflow.database.connection import initialize_database',
            '        from pangflow.database.models import ExecutionLogModel',
            '        from pathlib import Path',
            '        ws = Path(WORKSPACE_PATH) if WORKSPACE_PATH else None',
            '        db_url = f"sqlite:///{ws / \"pangflow.db\"}" if ws else None',
            '        db = initialize_database(db_url)',
            '        with db.get_session() as session:',
            '            existing = session.query(ExecutionLogModel).filter_by(run_id=run_id).first()',
            '            if existing is not None:',
            '                existing.status = status',
            '                existing.return_code = return_code',
            '                existing.stdout = stdout',
            '                existing.stderr = stderr',
            '                if status != "running":',
            '                    existing.completed_at = datetime.now()',
            '            else:',
            '                log = ExecutionLogModel(',
            '                    workflow_id=WORKFLOW_ID,',
            '                    run_id=run_id or str(uuid.uuid4()),',
            '                    execution_type="scheduled" if not os.environ.get("PANGFLOW_RUN_ID") else "trigger",',
            '                    status=status,',
            '                    return_code=return_code,',
            '                    stdout=stdout,',
            '                    stderr=stderr,',
            '                    triggered_by="prefect",',
            '                    started_at=datetime.now(),',
            '                    completed_at=datetime.now() if status != "running" else None,',
            '                )',
            '                session.add(log)',
            '    except Exception:',
            '        pass  # Best-effort logging',
            '',
            '',
            '@task(name=f"{WORKFLOW_NAME}_execute")',
            'def execute_command(',
            '    cmd: str,',
            '    working_dir: Optional[str] = None,',
            '    env_vars: Optional[Dict[str, str]] = None',
            ') -> Dict[str, Any]:',
            '    """Execute the CLI command as a Prefect task."""',
            '    run_id = os.environ.get("PANGFLOW_RUN_ID", str(uuid.uuid4()))',
            '    run_logger = get_run_logger()',
            '    run_logger.info(f"Executing command: {cmd}")',
            '    ',
            '    _record_execution("running", run_id=run_id)',
            '    ',
            '    # Prepare environment',
            '    env = os.environ.copy()',
            '    if env_vars:',
            '        env.update(env_vars)',
            '    ',
            '    # Execute command',
            '    result = subprocess.run(',
            '        cmd,',
            '        shell=True,',
            '        capture_output=True,',
            '        text=True,',
            '        cwd=working_dir,',
            '        env=env',
            '    )',
            '    ',
            '    run_logger.info(f"Command completed with return code: {result.returncode}")',
            '    if result.stdout:',
            '        run_logger.info("STDOUT: " + result.stdout)',
            '    if result.stderr:',
            '        if result.returncode != 0:',
            '            run_logger.error("STDERR: " + result.stderr)',
            '        else:',
            '            run_logger.warning("STDERR: " + result.stderr)',
            '    ',
            '    if result.returncode != 0:',
            '        _record_execution("failed", return_code=result.returncode, stdout=result.stdout, stderr=result.stderr, run_id=run_id)',
            '        raise RuntimeError(f"Command failed with code {result.returncode}: {result.stderr}")',
            '    ',
            '    _record_execution("success", return_code=0, stdout=result.stdout, stderr=result.stderr, run_id=run_id)',
            '    return {',
            '        "return_code": result.returncode,',
            '        "stdout": result.stdout,',
            '        "stderr": result.stderr,',
            '    }',
            '',
            '',
            f'@flow(name=WORKFLOW_NAME, log_prints=True)',
            f'def {flow_func_name}_flow(',
            '    working_dir: Optional[str] = None,',
            '    env_vars: Optional[Dict[str, str]] = None',
            ') -> Dict[str, Any]:',
            f'    """pangflow workflow: {workflow_state.workflow_name}"""',
            '    flow_logger = get_run_logger()',
            '    flow_logger.info(f"Starting workflow: {WORKFLOW_NAME}")',
            '    flow_logger.info(f"Workflow ID: {WORKFLOW_ID}")',
            '    ',
            '    result = execute_command(',
            '        cmd=COMMAND,',
            '        working_dir=working_dir,',
            '        env_vars=env_vars',
            '    )',
            '    ',
            '    flow_logger.info(f"Workflow completed: {WORKFLOW_NAME}")',
            '    return result',
            '',
            '',
            'if __name__ == "__main__":',
            '    # Serve the flow for deployment',
            f'    deployment = {flow_func_name}_flow.to_deployment(',
            f'        name="{workflow_state.workflow_name}-deployment",',
            schedule_config,
            '    )',
            '    serve(deployment)',
            '',
        ]
        flow_content = '\n'.join(lines)
        flow_file_path.write_text(flow_content, encoding="utf-8")
        logger.info(f"Created flow file: {flow_file_path}")
        return flow_func_name


    def create_deployment(
        self,
        workflow_state: WorkflowState,
        schedule_cron: Optional[str] = None,
        schedule_interval: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        '''Method Function:
        
        Create a Prefect deployment for a workflow.
        
        Uses the `serve()` method which creates a deployment and runs a process
        to serve it. This is the recommended approach for Prefect 3.x.
        Generates a Python flow file and prepares deployment metadata.
        
        :parameters:
            - workflow_state (WorkflowState) - The workflow state to deploy
            - schedule_cron (Optional[str]) - Optional cron schedule for the deployment
            - schedule_interval (Optional[int]) - Optional interval schedule in seconds
            - tags (Optional[List[str]]) - Optional list of tags for the deployment
        
        :return: 
            Deployment result with success status and deployment info
        '''

        logger.info(f"Creating deployment for workflow: {workflow_state.workflow_name}")
        try:
            # Determine schedule
            cron = schedule_cron
            interval = schedule_interval
            if workflow_state.workflow_type == WorkflowType.SCHEDULED:
                # Default schedule for scheduled workflows: hourly
                if not cron and not interval:
                    interval = 3600  # 1 hour in seconds
            # Create a flow file with serve() in the current working directory
            from pangflow.utils.workspace import find_workspace
            ws = find_workspace()
            flow_file_name = f"pangflow_flow_{workflow_state.workflow_id[:8]}.py"
            flow_file_path = Path.cwd() / flow_file_name
            flow_func_name = self._create_flow_file(
                workflow_state, 
                flow_file_path,
                schedule_cron=cron,
                schedule_interval=interval,
                workspace_path=ws,
            )
            # Store deployment info
            deployment_info = {
                "workflow_id": workflow_state.workflow_id,
                "workflow_name": workflow_state.workflow_name,
                "deployment_name": f"{workflow_state.workflow_name}-deployment",
                "flow_name": workflow_state.workflow_name,
                "flow_file": str(flow_file_path),
                "flow_func_name": flow_func_name,
                "work_pool": self.work_pool,
                "cron": cron,
                "interval": interval,
                "tags": tags or ["pangflow", workflow_state.package_name],
            }
            self._deployments[workflow_state.workflow_id] = deployment_info
            logger.info(f"Created deployment for workflow: {workflow_state.workflow_name}")
            logger.info(f"To serve this deployment, run: python {flow_file_path}")
            return {
                "success": True,
                "deployment": deployment_info,
                "message": f"Deployment created for {workflow_state.workflow_name}. Run 'python {flow_file_name}' to serve.",
            }
        except Exception as e:
            logger.exception(f"Failed to create deployment for {workflow_state.workflow_name}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to create deployment: {e}",
            }
    

    def _start_serve_process(self, flow_file_path: Path, workflow_name: str) -> Optional[subprocess.Popen]:
        '''Method Function:
        
        Start the serve process for a flow file in the background.
        
        Launches a subprocess to run the flow file, which starts the
        Prefect serve process. Handles platform-specific process creation
        flags for proper background execution.
        
        stdout/stderr are redirected to a log file to prevent PIPE buffer
        exhaustion which causes the serve process to crash silently.
        
        :parameters:
            - flow_file_path (Path) - Path to the flow file
            - workflow_name (str) - Name of the workflow
        
        :return: 
            The subprocess.Popen instance if started successfully, None otherwise
        '''

        try:
            logger.info(f"Starting serve process for {workflow_name}")
            # Redirect stdout/stderr to a log file to avoid PIPE buffer exhaustion
            log_dir = Path.cwd() / "logs"
            log_dir.mkdir(exist_ok=True)
            log_file = log_dir / f"{workflow_name}_serve.log"
            log_fp = open(log_file, "a", encoding="utf-8")
            log_fp.write(f"\n--- Serve started at {datetime.now().isoformat()} ---\n")
            log_fp.flush()

            if sys.platform == "win32":
                process = subprocess.Popen(
                    [sys.executable, str(flow_file_path)],
                    stdout=log_fp,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                process = subprocess.Popen(
                    [sys.executable, str(flow_file_path)],
                    stdout=log_fp,
                    stderr=subprocess.STDOUT,
                    start_new_session=True
                )
            # Wait a moment to see if the process starts successfully
            time.sleep(2)
            # Check if process is still running
            if process.poll() is None:
                logger.info(f"Serve process started for {workflow_name} (PID: {process.pid}, log: {log_file})")
                return process
            else:
                log_fp.write(f"Serve process exited early with code {process.returncode}\n")
                log_fp.flush()
                log_fp.close()
                logger.error(f"Serve process failed to start; see {log_file}")
                return None
        except Exception as e:
            logger.exception(f"Failed to start serve process: {e}")
            return None


    def deploy_to_server(
        self,
        workflow_state: WorkflowState,
        schedule_cron: Optional[str] = None,
        schedule_interval: Optional[int] = None,
        auto_start: bool = True
    ) -> Dict[str, Any]:
        '''Method Function:
        
        Deploy a workflow to the Prefect server.
        
        This method creates the deployment and applies it to the Prefect server,
        making it available for execution. Optionally starts the serve process
        in the background.
        
        :parameters:
            - workflow_state (WorkflowState) - The workflow state to deploy
            - schedule_cron (Optional[str]) - Optional cron schedule
            - schedule_interval (Optional[int]) - Optional interval schedule in seconds
            - auto_start (bool) - Whether to automatically start the serve process (default: True)
        
        :return: 
            Deployment result with ID, status, and serve process info
        '''

        logger.info(f"Deploying workflow to Prefect server: {workflow_state.workflow_name}")
        try:
            # Create deployment first
            result = self.create_deployment(
                workflow_state=workflow_state,
                schedule_cron=schedule_cron,
                schedule_interval=schedule_interval
            )
            if not result["success"]:
                return result
            deployment_info = result["deployment"]
            flow_file_path = Path(deployment_info["flow_file"])
            # Start serve process if auto_start is enabled
            serve_process = None
            if auto_start and flow_file_path.exists():
                serve_process = self._start_serve_process(flow_file_path, workflow_state.workflow_name)
            deployment_id = f"deployment-{workflow_state.workflow_id[:8]}"
            flow_id = f"flow-{workflow_state.workflow_id[:8]}"
            deployment_info["deployment_id"] = deployment_id
            deployment_info["flow_id"] = flow_id
            if serve_process:
                deployment_info["serve_pid"] = serve_process.pid
            logger.info(f"Deployed to Prefect server: {deployment_id}")
            return {
                "success": True,
                "deployment_id": deployment_id,
                "flow_id": flow_id,
                "serve_pid": serve_process.pid if serve_process else None,
                "deployment": deployment_info,
                "message": f"Successfully deployed {workflow_state.workflow_name}" + 
                          (f" (serve PID: {serve_process.pid})" if serve_process else ""),
            }
        except Exception as e:
            logger.exception(f"Failed to deploy {workflow_state.workflow_name}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to deploy: {e}",
            }
    

    def trigger_deployment(
        self,
        workflow_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        '''Method Function:
        
        Trigger a deployment run.
        
        Initiates a flow run for the specified workflow deployment
        using the Prefect API client.
        
        :parameters:
            - workflow_id (str) - The workflow ID
            - parameters (Optional[Dict[str, Any]]) - Optional parameters for the flow run
        
        :return:
            Trigger result with flow run ID if successful
        '''

        if workflow_id not in self._deployments:
            return {
                "success": False,
                "error": f"Deployment not found for workflow: {workflow_id}",
            }
        deployment_info = self._deployments[workflow_id]
        try:
            # Trigger the deployment via Prefect API
            from pangflow.prefect_integration.client import get_prefect_client
            client = get_prefect_client()
            deployment_id = deployment_info.get("deployment_id")
            if not deployment_id:
                return {
                    "success": False,
                    "error": "Deployment ID not available",
                }
            flow_run_id = client.trigger_deployment(deployment_id, parameters)
            if flow_run_id:
                return {
                    "success": True,
                    "flow_run_id": flow_run_id,
                    "message": f"Triggered deployment: {deployment_info['deployment_name']}",
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to trigger deployment",
                }
        except Exception as e:
            logger.exception(f"Failed to trigger deployment for {workflow_id}")
            return {
                "success": False,
                "error": str(e),
            }
    

    def delete_deployment(self, workflow_id: str) -> Dict[str, Any]:
        '''Method Function:
        
        Delete a deployment.
        
        Removes the deployment from the Prefect server and from the
        internal deployment registry.
        
        :parameters:
            - workflow_id (str) - The workflow ID
        
        :return: 
            Deletion result with status
        '''

        if workflow_id not in self._deployments:
            return {
                "success": False,
                "error": f"Deployment not found for workflow: {workflow_id}",
            }
        deployment_info = self._deployments[workflow_id]
        deployment_id = deployment_info.get("deployment_id")
        try:
            if deployment_id:
                from pangflow.prefect_integration.client import get_prefect_client
                client = get_prefect_client()
                client.delete_deployment(deployment_id)
            del self._deployments[workflow_id]
            return {
                "success": True,
                "message": f"Deleted deployment for workflow: {workflow_id}",
            }
        except Exception as e:
            logger.exception(f"Failed to delete deployment for {workflow_id}")
            return {
                "success": False,
                "error": str(e),
            }
    

    def get_deployment_info(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        '''Method Function:
        
        Get information about a deployment.
        
        Retrieves the stored deployment information for the specified
        workflow ID from the internal registry.
        
        :parameters:
            - workflow_id (str) - The workflow ID
        
        :return: 
            Deployment information dictionary, or None if not found
        '''

        return self._deployments.get(workflow_id)
    
    def list_deployments(self) -> List[Dict[str, Any]]:
        '''Method Function:
        
        List all managed deployments.
        
        Returns a list of all deployments currently tracked by this
        DeploymentManager instance.
        
        :parameters:
            None
        
        :return: 
            List of deployment information dictionaries
        '''

        return list(self._deployments.values())


def quick_deploy(
    workflow_state: WorkflowState,
    work_pool: str = "default-process",
    schedule_cron: Optional[str] = None
) -> Dict[str, Any]:
    '''Function Introduction:
    
    Quickly deploy a workflow to Prefect.
    
    This is a convenience function for one-off deployments without
    managing a DeploymentManager instance. Creates a temporary manager,
    deploys the workflow, and returns the result.
    
    :parameters:
        - workflow_state (WorkflowState) - The workflow state to deploy
        - work_pool (str) - Name of the work pool (default: "default-process")
        - schedule_cron (Optional[str]) - Optional cron schedule string
    
    :return: 
        Deployment result from deploy_to_server
    
    :example:
        >>> result = quick_deploy(workflow_state, schedule_cron="0 0 * * *")
        >>> if result["success"]:
        ...     print(f"Deployed: {result['deployment_id']}")
    '''
    manager = DeploymentManager(work_pool=work_pool)
    return manager.deploy_to_server(workflow_state, schedule_cron=schedule_cron)



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
