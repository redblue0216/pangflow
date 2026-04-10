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

This is the main CLI module for pangflow

- Design mode:

    (1) Command pattern - Each CLI command is encapsulated as a function

    (2) Group pattern - Commands are organized under the main CLI group

    (3) Factory pattern - Used for creating runners and deployment managers

- Key points:

    (1) Click framework for CLI interface

    (2) Workspace management for isolated environments

    (3) Database integration for workflow persistence

    (4) Process management for serve operations

    (5) Cross-platform support (Windows and Unix-like systems)

- Main functions:

    (1) CLI command handling and routing

    (2) Workspace initialization and management

    (3) Workflow registration from TOML files

    (4) Workflow deployment to Prefect

    (5) Workflow execution and triggering

    (6) Execution logs retrieval and display

    (7) Serve process management (start, stop, status)

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.cli.main import cli

    ### Initialize a new workspace
    # pangflow init /path/to/workspace

    ### Register a workflow
    # pangflow register workflows/my-workflow.toml

    ### Deploy a workflow
    # pangflow deploy my-workflow --cron "0 * * * *"

    ### List all workflows
    # pangflow list --package PangTS

    ### Trigger a workflow
    # pangflow trigger my-workflow --wait

    ### Show workflow details
    # pangflow show my-workflow

    ### View execution logs
    # pangflow logs my-workflow --limit 20

    ### Manage serve processes
    # pangflow serve start my-workflow --detach
    # pangflow serve stop my-workflow
    # pangflow serve status

Description of Class and Function
-----------------
(1)cli: Main CLI group that defines the entry point and global options

(2)init: Initialize a new pangflow workspace

(3)register: Register a workflow from a TOML file

(4)deploy: Deploy a registered workflow to Prefect

(5)list_workflows: List all registered workflows

(6)show: Show detailed information about a workflow

(7)trigger: Trigger a workflow execution

(8)create: Create a new workflow TOML file

(9)logs: Show execution logs for workflows

(10)serve: Manage serve processes for deployed workflows

(11)serve_start: Start the serve process for a deployed workflow

(12)serve_stop: Stop the serve process for a workflow

(13)serve_status: Show status of all serve processes

(14)is_process_running: Check if a process with the given PID is running

(15)get_workspace: Helper to get workspace manager with error handling

(16)generate_uuid: Generate a unique identifier for workflow state

References
----------
Click Documentation - https://click.palletsprojects.com/
Prefect Documentation - https://docs.prefect.io/
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
import sys
import logging
import os
from pathlib import Path
from typing import Optional
### Third-party packages
import click
### Internal packages
from pangflow.utils.workspace import WorkspaceManager, require_workspace, find_workspace
from pangflow.utils.toml_parser import TomlParser, parse_workflow_toml
from pangflow.utils.logger import setup_logging
from pangflow.database.connection import initialize_database, get_db_manager
from pangflow.database.repository import WorkflowRepository, ExecutionLogRepository
from pangflow.core.state import WorkflowState, WorkflowType, WorkflowStatus
from pangflow.core.task import TaskFactory
from pangflow.core.runner import RunnerFactory, ExecutionContext
from pangflow.prefect_integration.deployment import DeploymentManager



####### Global Variables ###########################################################################
####################################################################################################



# Set up logging
logger = logging.getLogger(__name__)



####### Helper Functions ###########################################################################
####################################################################################################



def is_process_running(pid: int) -> bool:
    '''
    Function Introduction
    ---------------------
    Check if a process with the given PID is running

    Attribute Function
    ------------------
    :parameters:
        - pid: Process ID to check
    :return:
        - bool: True if process is running, False otherwise

    Method Function
    ---------------
    - Check if the given process ID is currently running
    - Uses platform-specific methods (tasklist on Windows, os.kill on Unix)
    - Handles exceptions gracefully and returns False for invalid PIDs
    '''

    if pid is None:
        return False
    try:
        if sys.platform == "win32":
            # Windows: use tasklist to check if process exists
            import subprocess
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True,
                text=True,
                check=False
            )
            return str(pid) in result.stdout
        else:
            # Unix-like: send signal 0 to check if process exists
            os.kill(pid, 0)
            return True
    except (ProcessLookupError, OSError, ValueError):
        return False
    except Exception:
        return False



def get_workspace(path: Optional[str] = None) -> WorkspaceManager:
    '''
    Function Introduction
    ---------------------
    Helper to get workspace manager with error handling

    Attribute Function
    ------------------
    :parameters:
        - path: Optional path to the workspace directory
    :return:
        - WorkspaceManager: Configured workspace manager instance

    Method Function
    ---------------
    - Creates or retrieves a WorkspaceManager instance
    - Uses provided path or finds workspace in current directory
    - Exits with error message if workspace is not found
    '''

    try:
        if path:
            return WorkspaceManager(Path(path))
        return require_workspace()
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)



####### Classes and Functions #######################################################################
#####################################################################################################
###
### function: cli
### ------Define the main CLI group with global options
###
######################################################################################################



# Main CLI Group
@click.group()
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=False),
    help="Path to the pangflow workspace",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.pass_context
def cli(ctx: click.Context, workspace: Optional[str], verbose: bool):
    '''
    Class Introduction
    ------------------
    pangflow - Workflow orchestration tool based on Prefect

    - Design mode:
        (1) Command group pattern using Click framework
        (2) Context passing for shared state

    - Key points:
        (1) Version information display
        (2) Global workspace option
        (3) Verbose logging control
        (4) Context object for command sharing

    Attribute Function
    ------------------
    :parameters:
        - ctx: Click context object for passing data between commands
        - workspace: Optional path to the workspace directory
        - verbose: Flag to enable verbose/debug logging
    :return:
        - None: Sets up context object for subcommands

    Method Function
    ---------------
    - Initialize CLI context with workspace and verbosity settings
    - Set up logging level based on verbose flag
    - Store configuration in context for subcommand access
    '''

    # Ensure context object exists
    ctx.ensure_object(dict)
    # Set up logging
    log_level = logging.DEBUG if verbose else logging.INFO
    setup_logging(level=log_level)
    # Store workspace path in context
    ctx.obj["workspace_path"] = workspace
    ctx.obj["verbose"] = verbose



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: init
### ------Initialize a new pangflow workspace
###
######################################################################################################



# Init Command
@cli.command()
@click.argument("path", required=False, default=".")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force reinitialization if workspace exists",
)
def init(path: str, force: bool):
    '''
    Class Introduction
    ------------------
    Initialize a new pangflow workspace

    - Design mode:
        (1) Command pattern for workspace initialization

    - Key points:
        (1) Creates directory structure
        (2) Generates configuration files
        (3) Supports force reinitialization
        (4) Provides next steps guidance

    Attribute Function
    ------------------
    :parameters:
        - path: Directory path for the new workspace (default: current directory)
        - force: Flag to force reinitialization of existing workspace
    :return:
        - None: Creates workspace structure and prints confirmation

    Method Function
    ---------------
    - Validates workspace path and existing configuration
    - Creates workspace directory structure
    - Generates pangflow.toml configuration file
    - Prints workspace structure and next steps
    '''

    workspace_path = Path(path).resolve()
    if workspace_path.exists() and any(workspace_path.iterdir()) and not force:
        if (workspace_path / "pangflow.toml").exists():
            click.echo(f"Workspace already initialized: {workspace_path}")
            click.echo("Use --force to reinitialize.")
            return
    manager = WorkspaceManager(workspace_path)
    try:
        manager.initialize(force=force)
        click.echo(f"[OK] Initialized pangflow workspace: {workspace_path}")
        click.echo(f"\nWorkspace structure:")
        click.echo(f"  {workspace_path}/")
        click.echo(f"  ├── pangflow.toml      # Workspace configuration")
        click.echo(f"  ├── pangflow.db        # SQLite database")
        click.echo(f"  ├── workflows/        # Workflow definitions")
        click.echo(f"  ├── logs/            # Execution logs")
        click.echo(f"  └── data/            # Workflow data")
        click.echo(f"\nNext steps:")
        click.echo(f"  1. Edit workflow files in {workspace_path}/workflows/")
        click.echo(f"  2. Register workflows: pangflow register <workflow.toml>")
        click.echo(f"  3. Deploy workflows: pangflow deploy <workflow_name>")
    except Exception as e:
        click.echo(f"Error initializing workspace: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: register
### ------Register a workflow from a TOML file
###
######################################################################################################



# Register Command
@cli.command()
@click.argument("toml_file", type=click.Path(exists=True))
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
def register(toml_file: str, workspace: Optional[str]):
    '''
    Class Introduction
    ------------------
    Register a workflow from a TOML file

    - Design mode:
        (1) Command pattern for workflow registration
        (2) Repository pattern for database operations

    - Key points:
        (1) TOML file parsing
        (2) Workflow state creation
        (3) Database persistence
        (4) UUID generation for workflows

    Attribute Function
    ------------------
    :parameters:
        - toml_file: Path to the TOML workflow definition file
        - workspace: Optional path to the workspace directory
    :return:
        - None: Registers workflow and prints confirmation

    Method Function
    ---------------
    - Parses TOML configuration file
    - Creates WorkflowState object with metadata
    - Persists workflow to database via repository
    - Displays registration confirmation with details
    '''

    # Get workspace
    manager = get_workspace(workspace)
    # Initialize database
    db_manager = initialize_database(str(manager.database_path))
    try:
        # Parse TOML file
        config = parse_workflow_toml(toml_file)
        # Create workflow state
        workflow_state = WorkflowState(
            workflow_id=WorkflowState.generate_uuid(),
            workflow_name=config.name,
            package_name=config.package,
            command=config.command,
            workflow_type=WorkflowType(config.workflow_type),
            status=WorkflowStatus.REGISTERED,
            is_deployed=False,
            metadata={
                "description": config.description,
                "working_dir": config.working_dir,
                "env_vars": config.env_vars,
                "timeout": config.timeout,
                "schedule": config.schedule,
            }
        )
        # Store in database
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            model = repo.create(workflow_state)
            click.echo(f"[OK] Registered workflow: {config.name}")
            click.echo(f"   ID: {model.id}")
            click.echo(f"   Package: {config.package}")
            click.echo(f"   Type: {config.workflow_type}")
            click.echo(f"   Command: {config.command[:60]}{'...' if len(config.command) > 60 else ''}")
    except Exception as e:
        click.echo(f"Error registering workflow: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: deploy
### ------Deploy a registered workflow to Prefect
###
######################################################################################################



# Deploy Command
@cli.command()
@click.argument("workflow_name")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
@click.option(
    "--cron",
    "-c",
    help="Cron schedule for scheduled workflows",
)
@click.option(
    "--no-serve",
    is_flag=True,
    help="Do not automatically start the serve process",
)
def deploy(workflow_name: str, workspace: Optional[str], cron: Optional[str], no_serve: bool):
    '''
    Class Introduction
    ------------------
    Deploy a registered workflow to Prefect

    - Design mode:
        (1) Command pattern for deployment
        (2) Deployment manager pattern for Prefect integration

    - Key points:
        (1) Workflow lookup by name
        (2) Prefect deployment creation
        (3) Optional serve process startup
        (4) Database status updates

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Name of the registered workflow to deploy
        - workspace: Optional path to the workspace directory
        - cron: Optional cron schedule expression
        - no_serve: Flag to prevent automatic serve process startup
    :return:
        - None: Deploys workflow and prints deployment details

    Method Function
    ---------------
    - Retrieves workflow from database by name
    - Creates Prefect deployment via DeploymentManager
    - Optionally starts serve process for the deployment
    - Updates workflow status and deployment info in database
    '''

    # Get workspace
    manager = get_workspace(workspace)
    # Initialize database
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            # Find workflow by name
            model = repo.get_by_name(workflow_name)
            if not model:
                click.echo(f"Error: Workflow not found: {workflow_name}", err=True)
                sys.exit(1)
            # Convert to workflow state
            workflow_state = repo.to_workflow_state(model)
            # Create deployment
            deployment_manager = DeploymentManager()
            result = deployment_manager.deploy_to_server(
                workflow_state=workflow_state,
                schedule_cron=cron,
                auto_start=not no_serve
            )
            if result["success"]:
                # Update database
                deployment_id = result.get("deployment_id")
                flow_id = result.get("flow_id")
                deployment_info = result.get("deployment", {})
                flow_file = deployment_info.get("flow_file", "")
                serve_pid = result.get("serve_pid")
                repo.update_deployment_info(
                    workflow_id=model.id,
                    deployment_id=deployment_id,
                    flow_id=flow_id,
                    is_deployed=True
                )
                repo.update_status(model.id, WorkflowStatus.DEPLOYED)
                # Update serve info if serve process was started
                if serve_pid:
                    repo.update_serve_info(
                        workflow_id=model.id,
                        serve_pid=serve_pid,
                        serve_status="running"
                    )
                click.echo(f"[OK] Deployed workflow: {workflow_name}")
                click.echo(f"   Deployment ID: {deployment_id}")
                click.echo(f"   Flow ID: {flow_id}")
                if flow_file:
                    click.echo(f"   Flow File: {flow_file}")
                if serve_pid:
                    click.echo(f"   Serve PID: {serve_pid}")
                    click.echo(f"\nDeployment is now running and serving requests.")
                else:
                    click.echo(f"\nTo start serving this deployment, run:")
                    click.echo(f"   python {flow_file}")
            else:
                click.echo(f"Error deploying workflow: {result.get('error')}", err=True)
                sys.exit(1)
    except Exception as e:
        click.echo(f"Error deploying workflow: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: list
### ------List all registered workflows
###
######################################################################################################



# List Command
@cli.command(name="list")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
@click.option(
    "--package",
    "-p",
    help="Filter by package name",
)
@click.option(
    "--type",
    "-t",
    type=click.Choice(["trigger", "scheduled"]),
    help="Filter by workflow type",
)
def list_workflows(workspace: Optional[str], package: Optional[str], type: Optional[str]):
    '''
    Class Introduction
    ------------------
    List all registered workflows

    - Design mode:
        (1) Command pattern for listing workflows
        (2) Repository pattern for data retrieval

    - Key points:
        (1) Filtering by package name
        (2) Filtering by workflow type
        (3) Process status checking
        (4) Tabular output formatting

    Attribute Function
    ------------------
    :parameters:
        - workspace: Optional path to the workspace directory
        - package: Optional package name filter
        - type: Optional workflow type filter (trigger or scheduled)
    :return:
        - None: Displays list of workflows in tabular format

    Method Function
    ---------------
    - Retrieves workflows from database with optional filters
    - Checks serve process status for each workflow
    - Formats and displays workflow information in a table
    '''

    # Get workspace
    manager = get_workspace(workspace)
    # Initialize database
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            models = repo.list_all(package_name=package)
            if not models:
                click.echo("No workflows found.")
                return
            # Filter by type if specified
            if type:
                models = [m for m in models if m.workflow_type == type]
            # Print header
            click.echo(f"{'Name':<30} {'Package':<10} {'Type':<10} {'Status':<12} {'Flow ID':<12} {'Deployed':<10} {'Serve':<10}")
            click.echo("-" * 104)
            # Print workflows
            for model in models:
                deployed = "YES" if model.is_deployed else "NO"
                flow_id_short = model.flow_id[:8] if model.flow_id else "N/A"
                # Determine serve status - check if process is actually running
                if model.serve_status == "running" and model.serve_pid:
                    if is_process_running(model.serve_pid):
                        serve_status = f"RUN({model.serve_pid})"
                    else:
                        serve_status = "DIED"
                elif model.serve_status == "stopped":
                    serve_status = "STOPPED"
                elif model.serve_status == "failed":
                    serve_status = "FAILED"
                else:
                    serve_status = "N/A" if not model.is_deployed else "NOT RUN"
                click.echo(
                    f"{model.workflow_name:<30} "
                    f"{model.package_name:<10} "
                    f"{model.workflow_type:<10} "
                    f"{model.status:<12} "
                    f"{flow_id_short:<12} "
                    f"{deployed:<10} "
                    f"{serve_status:<10}"
                )
            click.echo(f"\nTotal: {len(models)} workflows")
    except Exception as e:
        click.echo(f"Error listing workflows: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: show
### ------Show detailed information about a workflow
###
######################################################################################################



# Show Command
@cli.command()
@click.argument("workflow_name")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
def show(workflow_name: str, workspace: Optional[str]):
    '''
    Class Introduction
    ------------------
    Show detailed information about a workflow

    - Design mode:
        (1) Command pattern for workflow details
        (2) Repository pattern for data access

    - Key points:
        (1) Workflow metadata display
        (2) Execution history retrieval
        (3) Formatted output

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Name of the workflow to display
        - workspace: Optional path to the workspace directory
    :return:
        - None: Displays detailed workflow information

    Method Function
    ---------------
    - Retrieves workflow details from database
    - Fetches recent execution logs
    - Displays formatted workflow information and history
    '''

    # Get workspace
    manager = get_workspace(workspace)
    # Initialize database
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            log_repo = ExecutionLogRepository(session)
            # Find workflow by name
            model = repo.get_by_name(workflow_name)
            if not model:
                click.echo(f"Error: Workflow not found: {workflow_name}", err=True)
                sys.exit(1)
            # Print workflow details
            click.echo(f"Workflow: {model.workflow_name}")
            click.echo(f"{'=' * 50}")
            click.echo(f"ID: {model.id}")
            click.echo(f"Package: {model.package_name}")
            click.echo(f"Type: {model.workflow_type}")
            click.echo(f"Status: {model.status}")
            click.echo(f"Deployed: {'Yes' if model.is_deployed else 'No'}")
            if model.deployment_id:
                click.echo(f"Deployment ID: {model.deployment_id}")
            if model.flow_id:
                click.echo(f"Flow ID: {model.flow_id}")
            click.echo(f"Command: {model.command}")
            if model.working_dir:
                click.echo(f"Working Directory: {model.working_dir}")
            click.echo(f"Created: {model.created_at}")
            click.echo(f"Updated: {model.updated_at}")
            # Get recent execution logs
            logs = log_repo.list_by_workflow(model.id, limit=5)
            if logs:
                click.echo(f"\nRecent Executions:")
                click.echo(f"{'Run ID':<20} {'Status':<10} {'Started':<20}")
                click.echo("-" * 60)
                for log in logs:
                    started = log.started_at.strftime("%Y-%m-%d %H:%M") if log.started_at else "N/A"
                    click.echo(f"{log.run_id[:18]:<20} {log.status:<10} {started:<20}")
    except Exception as e:
        click.echo(f"Error showing workflow: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: trigger
### ------Trigger a workflow execution
###
######################################################################################################



# Trigger Command
@cli.command()
@click.argument("workflow_name")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
@click.option(
    "--wait",
    is_flag=True,
    help="Wait for execution to complete",
)
def trigger(workflow_name: str, workspace: Optional[str], wait: bool):
    '''
    Class Introduction
    ------------------
    Trigger a workflow execution

    - Design mode:
        (1) Command pattern for workflow execution
        (2) Factory pattern for runner creation
        (3) Repository pattern for logging

    - Key points:
        (1) Workflow lookup and validation
        (2) Execution context creation
        (3) Runner execution
        (4) Log recording
        (5) Output display

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Name of the workflow to trigger
        - workspace: Optional path to the workspace directory
        - wait: Flag to wait for execution completion
    :return:
        - None: Executes workflow and displays results

    Method Function
    ---------------
    - Retrieves workflow from database
    - Creates execution context and log entry
    - Runs workflow using appropriate runner
    - Updates log with execution results
    - Displays execution output and status
    '''

    # Get workspace
    manager = get_workspace(workspace)
    # Initialize database
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            log_repo = ExecutionLogRepository(session)
            # Find workflow by name
            model = repo.get_by_name(workflow_name)
            if not model:
                click.echo(f"Error: Workflow not found: {workflow_name}", err=True)
                sys.exit(1)
            # Create execution context
            context = ExecutionContext(
                workflow_id=model.id,
                workflow_name=model.workflow_name,
                workflow_type=model.workflow_type,
                command=model.command,
                working_dir=model.working_dir,
            )
            # Create execution log
            import uuid
            run_id = str(uuid.uuid4())
            log_entry = log_repo.create(
                workflow_id=model.id,
                run_id=run_id,
                execution_type=model.workflow_type,
                status="running",
                triggered_by="user",
            )
            click.echo(f"[RUN] Triggering workflow: {workflow_name}")
            click.echo(f"   Run ID: {run_id}")
            # Execute the workflow
            runner = RunnerFactory.create("local")
            result = runner.run(context)
            # Update execution log
            log_repo.complete(
                log_id=log_entry.id,
                status=result.status.value,
                stdout=result.stdout,
                stderr=result.stderr,
                return_code=result.return_code,
            )
            # Print results
            if result.status.value == "success":
                click.echo(f"[OK] Workflow completed successfully")
            else:
                click.echo(f"[ERROR] Workflow failed with status: {result.status.value}")
            if result.stdout:
                click.echo(f"\nOutput:\n{result.stdout}")
            if result.stderr:
                click.echo(f"\nErrors:\n{result.stderr}", err=True)
            # Update workflow status
            new_status = WorkflowStatus.COMPLETED if result.status.value == "success" else WorkflowStatus.FAILED
            repo.update_status(model.id, new_status)
    except Exception as e:
        click.echo(f"Error triggering workflow: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: create
### ------Create a new workflow TOML file
###
######################################################################################################



# Create Command
@cli.command()
@click.argument("workflow_name")
@click.option(
    "--type",
    "-t",
    type=click.Choice(["trigger", "scheduled"]),
    default="trigger",
    help="Type of workflow",
)
@click.option(
    "--package",
    "-p",
    type=click.Choice(["PangTS", "PangFT"]),
    default="PangTS",
    help="Algorithm package",
)
@click.option(
    "--command",
    "-c",
    help="Command to execute (default: echo command for demo)",
)
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
def create(workflow_name: str, type: str, package: str, command: Optional[str], workspace: Optional[str]):
    '''
    Class Introduction
    ------------------
    Create a new workflow TOML file

    - Design mode:
        (1) Command pattern for file creation
        (2) Template pattern for workflow generation

    - Key points:
        (1) Workflow type selection
        (2) Package selection
        (3) Custom command support
        (4) File existence validation

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Name for the new workflow
        - type: Workflow type (trigger or scheduled)
        - package: Algorithm package (PangTS or PangFT)
        - command: Optional custom command to execute
        - workspace: Optional path to the workspace directory
    :return:
        - None: Creates workflow file and prints confirmation

    Method Function
    ---------------
    - Generates workflow TOML file from template
    - Validates file doesn't already exist
    - Saves file to workspace workflows directory
    - Provides next steps guidance
    '''

    # Get workspace
    manager = get_workspace(workspace)
    try:
        workflow_path = manager.create_workflow_file(
            workflow_name=workflow_name,
            workflow_type=type,
            package=package,
            command=command
        )
        click.echo(f"[OK] Created workflow file: {workflow_path}")
        click.echo(f"\nEdit the file to customize your workflow, then run:")
        click.echo(f"  pangflow register {workflow_path}")
    except FileExistsError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error creating workflow: {e}", err=True)
        sys.exit(1)



####### CLI Commands ################################################################################
#####################################################################################################
###
### command: logs
### ------Show execution logs for workflows
###
######################################################################################################


# Logs Command
@cli.command()
@click.argument("workflow_name", required=False)
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
@click.option(
    "--limit",
    "-n",
    type=int,
    default=10,
    help="Number of logs to show",
)
def logs(workflow_name: Optional[str], workspace: Optional[str], limit: int):
    '''
    Class Introduction
    ------------------
    Show execution logs for workflows

    - Design mode:
        (1) Command pattern for log retrieval
        (2) Repository pattern for data access

    - Key points:
        (1) Optional workflow filtering
        (2) Configurable result limit
        (3) Duration calculation
        (4) Tabular output formatting

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Optional name of specific workflow to show logs for
        - workspace: Optional path to the workspace directory
        - limit: Maximum number of log entries to display
    :return:
        - None: Displays execution logs in tabular format

    Method Function
    ---------------
    - Retrieves logs for specific workflow or all workflows
    - Formats log data with timestamps and durations
    - Displays formatted log entries in a table
    '''

    # Get workspace
    manager = get_workspace(workspace)
    # Initialize database
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            log_repo = ExecutionLogRepository(session)
            if workflow_name:
                # Get logs for specific workflow
                repo = WorkflowRepository(session)
                model = repo.get_by_name(workflow_name)
                if not model:
                    click.echo(f"Error: Workflow not found: {workflow_name}", err=True)
                    sys.exit(1)
                logs = log_repo.list_by_workflow(model.id, limit=limit)
                click.echo(f"Execution logs for: {workflow_name}")
            else:
                # Get all logs
                logs = log_repo.list_all(limit=limit)
                click.echo("Recent execution logs:")
            if not logs:
                click.echo("No logs found.")
                return
            click.echo(f"\n{'Run ID':<20} {'Status':<10} {'Triggered':<12} {'Started':<20} {'Duration':<10}")
            click.echo("-" * 80)
            for log in logs:
                run_id = log.run_id[:18] if len(log.run_id) > 18 else log.run_id
                started = log.started_at.strftime("%Y-%m-%d %H:%M") if log.started_at else "N/A"
                duration = f"{log.duration_seconds:.1f}s" if log.duration_seconds else "N/A"
                click.echo(
                    f"{run_id:<20} "
                    f"{log.status:<10} "
                    f"{log.triggered_by:<12} "
                    f"{started:<20} "
                    f"{duration:<10}"
                )
    except Exception as e:
        click.echo(f"Error retrieving logs: {e}", err=True)
        sys.exit(1)



####### CLI Command Groups ##########################################################################
#####################################################################################################
###
### group: serve
### ------Manage serve processes for deployed workflows
###
######################################################################################################



# Serve Command Group
@cli.group()
def serve():
    '''
    Class Introduction
    ------------------
    Manage serve processes for deployed workflows

    - Design mode:
        (1) Command group pattern for related commands
        (2) Process management pattern

    - Key points:
        (1) Serve process lifecycle management
        (2) Background/foreground execution modes
        (3) Process status monitoring
        (4) Cross-platform process control

    Attribute Function
    ------------------
    :parameters:
        - None: Command group with no direct parameters
    :return:
        - None: Provides subcommand access (start, stop, status)

    Method Function
    ---------------
    - Groups serve-related commands
    - Provides unified interface for process management
    '''
    pass



####### CLI Subcommands #############################################################################
#####################################################################################################
###
### subcommand: serve_start
### ------Start the serve process for a deployed workflow
###
######################################################################################################



@serve.command("start")
@click.argument("workflow_name")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
@click.option(
    "--detach",
    "-d",
    is_flag=True,
    help="Run serve in detached mode (background)",
)
def serve_start(workflow_name: str, workspace: Optional[str], detach: bool):
    '''
    Class Introduction
    ------------------
    Start the serve process for a deployed workflow

    - Design mode:
        (1) Command pattern for process startup
        (2) Factory pattern for subprocess creation

    - Key points:
        (1) Workflow validation
        (2) Flow file discovery
        (3) Background/foreground execution modes
        (4) Cross-platform subprocess handling
        (5) Process status tracking

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Name of the deployed workflow
        - workspace: Optional path to the workspace directory
        - detach: Flag to run in background/detached mode
    :return:
        - None: Starts serve process and prints status

    Method Function
    ---------------
    - Validates workflow is deployed
    - Locates generated flow file
    - Starts subprocess in background or foreground
    - Updates database with serve process info
    '''

    manager = get_workspace(workspace)
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            # Find workflow by name
            model = repo.get_by_name(workflow_name)
            if not model:
                click.echo(f"Error: Workflow not found: {workflow_name}", err=True)
                sys.exit(1)
            # Check if already deployed
            if not model.is_deployed:
                click.echo(f"Error: Workflow '{workflow_name}' is not deployed.", err=True)
                click.echo("Run 'pangflow deploy <workflow_name>' first.")
                sys.exit(1)
            # Check if serve is already running
            if model.serve_status == "running" and model.serve_pid:
                if is_process_running(model.serve_pid):
                    click.echo(f"Serve is already running for '{workflow_name}' (PID: {model.serve_pid})")
                    return
            # Find the flow file
            flow_file_pattern = f"pangflow_flow_{model.id[:8]}.py"
            flow_file_path = Path.cwd() / flow_file_pattern
            if not flow_file_path.exists():
                # Search in workspace directory
                flow_files = list(Path.cwd().glob("pangflow_flow_*.py"))
                if not flow_files:
                    click.echo(f"Error: Flow file not found for workflow '{workflow_name}'.", err=True)
                    click.echo("The flow file may have been deleted. Please redeploy.")
                    sys.exit(1)
                # Use the most recently modified flow file as fallback
                flow_file_path = max(flow_files, key=lambda p: p.stat().st_mtime)
            # Start the serve process
            click.echo(f"Starting serve for workflow: {workflow_name}")
            click.echo(f"Flow file: {flow_file_path}")
            if detach:
                # Start in background (detached)
                import subprocess
                import sys
                if sys.platform == "win32":
                    process = subprocess.Popen(
                        [sys.executable, str(flow_file_path)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                    )
                else:
                    process = subprocess.Popen(
                        [sys.executable, str(flow_file_path)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True
                    )
                # Update database
                repo.update_serve_info(
                    workflow_id=model.id,
                    serve_pid=process.pid,
                    serve_status="running"
                )
                click.echo(f"[OK] Serve started in background (PID: {process.pid})")
                click.echo(f"Run 'pangflow serve status' to check the status.")
            else:
                # Start in foreground
                click.echo("\nStarting serve process (Ctrl+C to stop)...\n")
                import subprocess
                import sys
                try:
                    # Update status before starting
                    repo.update_serve_info(
                        workflow_id=model.id,
                        serve_pid=os.getpid(),  # Use current process as placeholder
                        serve_status="running"
                    )
                    # Run serve process
                    result = subprocess.run([sys.executable, str(flow_file_path)])
                    # Update status after stopping
                    if result.returncode == 0:
                        repo.update_serve_info(
                            workflow_id=model.id,
                            serve_pid=None,
                            serve_status="stopped"
                        )
                        click.echo(f"\nServe stopped normally.")
                    else:
                        repo.update_serve_info(
                            workflow_id=model.id,
                            serve_pid=None,
                            serve_status="failed"
                        )
                        click.echo(f"\nServe exited with code: {result.returncode}", err=True)
                except KeyboardInterrupt:
                    repo.update_serve_info(
                        workflow_id=model.id,
                        serve_pid=None,
                        serve_status="stopped"
                    )
                    click.echo(f"\nServe stopped by user.")
    except Exception as e:
        click.echo(f"Error starting serve: {e}", err=True)
        sys.exit(1)



####### CLI Subcommands #############################################################################
#####################################################################################################
###
### subcommand: serve_stop
### ------Stop the serve process for a workflow
###
######################################################################################################



@serve.command("stop")
@click.argument("workflow_name")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
def serve_stop(workflow_name: str, workspace: Optional[str]):
    '''
    Class Introduction
    ------------------
    Stop the serve process for a workflow

    - Design mode:
        (1) Command pattern for process termination
        (2) Cross-platform process control

    - Key points:
        (1) Process existence validation
        (2) Graceful termination (SIGTERM on Unix)
        (3) Force kill on Windows (taskkill)
        (4) Database status update

    Attribute Function
    ------------------
    :parameters:
        - workflow_name: Name of the workflow to stop serving
        - workspace: Optional path to the workspace directory
    :return:
        - None: Stops serve process and prints confirmation

    Method Function
    ---------------
    - Validates serve process is running
    - Terminates process using platform-specific method
    - Updates database to reflect stopped state
    '''

    manager = get_workspace(workspace)
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            # Find workflow by name
            model = repo.get_by_name(workflow_name)
            if not model:
                click.echo(f"Error: Workflow not found: {workflow_name}", err=True)
                sys.exit(1)
            # Check if serve is running
            if not model.serve_pid:
                click.echo(f"No serve process found for '{workflow_name}'.")
                return
            if not is_process_running(model.serve_pid):
                click.echo(f"Serve process for '{workflow_name}' is not running (PID: {model.serve_pid})")
                # Update database to reflect stopped state
                repo.update_serve_info(
                    workflow_id=model.id,
                    serve_pid=None,
                    serve_status="stopped"
                )
                return
            # Stop the process
            try:
                if sys.platform == "win32":
                    import subprocess
                    subprocess.run(["taskkill", "/PID", str(model.serve_pid), "/F"], 
                                 capture_output=True, check=False)
                else:
                    import os
                    os.kill(model.serve_pid, 15)  # SIGTERM
                # Update database
                repo.update_serve_info(
                    workflow_id=model.id,
                    serve_pid=None,
                    serve_status="stopped"
                )
                click.echo(f"[OK] Stopped serve for '{workflow_name}' (PID: {model.serve_pid})")
            except Exception as e:
                click.echo(f"Error stopping serve: {e}", err=True)
                sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)



####### CLI Subcommands #############################################################################
#####################################################################################################
###
### subcommand: serve_status
### ------Show status of all serve processes
###
######################################################################################################



@serve.command("status")
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    help="Path to the workspace",
)
def serve_status(workspace: Optional[str]):
    '''
    Class Introduction
    ------------------
    Show status of all serve processes

    - Design mode:
        (1) Command pattern for status reporting
        (2) Process monitoring pattern

    - Key points:
        (1) Lists only deployed workflows
        (2) Real-time process status checking
        (3) Multiple status states (RUNNING, DIED, STOPPED, FAILED)
        (4) Tabular output formatting

    Attribute Function
    ------------------
    :parameters:
        - workspace: Optional path to the workspace directory
    :return:
        - None: Displays serve status for all deployed workflows

    Method Function
    ---------------
    - Retrieves all deployed workflows from database
    - Checks actual process status for each serve process
    - Displays formatted status table
    '''

    manager = get_workspace(workspace)
    db_manager = initialize_database(str(manager.database_path))
    try:
        with db_manager.get_session() as session:
            repo = WorkflowRepository(session)
            models = repo.list_all()
            # Filter to only deployed workflows
            deployed_models = [m for m in models if m.is_deployed]
            if not deployed_models:
                click.echo("No deployed workflows found.")
                return
            click.echo(f"{'Workflow Name':<30} {'Status':<12} {'PID':<10} {'Running':<10}")
            click.echo("-" * 62)
            for model in deployed_models:
                if model.serve_status == "running" and model.serve_pid:
                    actually_running = is_process_running(model.serve_pid)
                    status = "RUNNING" if actually_running else "DIED"
                    pid_str = str(model.serve_pid)
                    running_str = "YES" if actually_running else "NO"
                elif model.serve_status == "stopped":
                    status = "STOPPED"
                    pid_str = "N/A"
                    running_str = "NO"
                elif model.serve_status == "failed":
                    status = "FAILED"
                    pid_str = str(model.serve_pid) if model.serve_pid else "N/A"
                    running_str = "NO"
                else:
                    status = "NOT STARTED"
                    pid_str = "N/A"
                    running_str = "N/A"
                
                click.echo(
                    f"{model.workflow_name:<30} "
                    f"{status:<12} "
                    f"{pid_str:<10} "
                    f"{running_str:<10}"
                )
    except Exception as e:
        click.echo(f"Error checking serve status: {e}", err=True)
        sys.exit(1)



####### Utility Functions ###########################################################################
#####################################################################################################
###
### function: generate_uuid
### ------Generate a unique identifier for workflow state
###
######################################################################################################



# Add generate-uuid method to WorkflowState
@staticmethod
def generate_uuid() -> str:
    '''
    Function Introduction
    ---------------------
    Generate a unique identifier for workflow state

    Attribute Function
    ------------------
    :parameters:
        - None
    :return:
        - str: UUID string for unique identification

    Method Function
    ---------------
    - Generates RFC 4122 compliant UUID
    - Used for workflow ID generation
    '''
    import uuid
    return str(uuid.uuid4())



WorkflowState.generate_uuid = generate_uuid



####### Entry Point #################################################################################
#####################################################################################################



if __name__ == "__main__":
    cli()



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
