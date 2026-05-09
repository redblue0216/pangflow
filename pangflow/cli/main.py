# -*- coding: utf-8 -*-
"""PangFlow v0.2.12 CLI – pangflowctl.

A modern typer-based CLI for workflow orchestration, environment management,
serve lifecycle, model promotion and lineage inspection.
"""

import json
import logging
import os
import signal
import shutil
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from pangflow.core.runner import ExecutionContext, RunnerFactory
from pangflow.core.state import WorkflowState, WorkflowStatus, WorkflowType
from pangflow.database.connection import initialize_database
from pangflow.database.models import (
    ArtifactModel,
    ArtifactVersionModel,
    EnvironmentModel,
    ExecutionLogModel,
    LineageEdgeModel,
    NodeLogModel,
    ServiceModel,
    WorkflowModel,
)
from pangflow.database.repository import ExecutionLogRepository, WorkflowRepository
from pangflow.env.manager import EnvManager
from pangflow.env.spec import CondaSpec, EnvSpec, PipSpec
from pangflow.orchestration.registry import NodeRegistry
from pangflow.orchestration.serve_compiler import ServeCompiler
from pangflow.prefect_integration.deployment import DeploymentManager
from pangflow.serve.manager import ServeManager
from pangflow.storage.backend import LocalFileBackend
from pangflow.storage.meta_store import MetaStore
from pangflow.storage.model_store import ModelStore
from pangflow.utils.toml_parser import parse_workflow_toml
from pangflow.utils.workspace import WorkspaceManager, require_workspace

# --------------------------------------------------------------------------- #
# Global state
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
console = Console()
app = typer.Typer(
    name="pangflowctl",
    help="PangFlow v0.2.12 – Algorithm OPS orchestration CLI",
    no_args_is_help=True,
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


@contextmanager
def _db_session(db_manager):
    """Provide a SQLAlchemy session that auto-commits on success, rolls back on error, and closes cleanly.

    Sets expire_on_commit=False so model attributes remain accessible after the block exits.
    """
    session = db_manager._session_factory()
    session.expire_on_commit = False
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _get_workspace(path: Optional[str] = None) -> WorkspaceManager:
    try:
        if path:
            manager = WorkspaceManager(Path(path))
            if not manager.is_initialized:
                raise FileNotFoundError(
                    f"Workspace not initialized: {path}\n"
                    "Run 'pangflowctl init' first."
                )
            return manager
        return require_workspace()
    except FileNotFoundError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1)


def _init_db(workspace: WorkspaceManager):
    db_url = f"sqlite:///{workspace.database_path}"
    return initialize_database(db_url)


def _is_process_running(pid: Optional[int]) -> bool:
    if pid is None:
        return False
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (ProcessLookupError, OSError, ValueError):
        return False


def _model_store(workspace: WorkspaceManager) -> ModelStore:
    backend = LocalFileBackend(str(workspace.workspace_path / "data" / "models"))
    return ModelStore(backend, MetaStore())


def _env_spec_from_toml(toml_path: Path, workflow_name: str) -> EnvSpec:
    """Read [workflow.env] from a TOML file and build an EnvSpec."""
    from pangflow.core.config import ConfigLoader

    loader = ConfigLoader()
    cfg = loader.load(str(toml_path))
    env = cfg.env

    # Use workflow.env.name if set, else auto-generate
    env_name = env.name or f"pangflow-{workflow_name}"
    python_ver = env.python or "3.10"

    return EnvSpec(
        name=env_name,
        python=python_ver,
        conda=CondaSpec(
            channels=list(env.conda.channels),
            dependencies=list(env.conda.dependencies),
        ),
        pip=PipSpec(
            dependencies=list(env.pip.dependencies),
        ),
    )


def _lookup_workflow(session, name: str) -> WorkflowModel:
    from pangflow.database.models import WorkflowModel
    model = (
        session.query(WorkflowModel)
        .filter_by(workflow_name=name)
        .order_by(WorkflowModel.created_at.desc())
        .first()
    )
    if model is None:
        console.print(f"[red]Error:[/red] Workflow not found: {name}")
        raise typer.Exit(1)
    return model


# --------------------------------------------------------------------------- #
# init
# --------------------------------------------------------------------------- #


@app.command()
def init(
    path: str = typer.Argument(".", help="Directory path for the new workspace"),
    force: bool = typer.Option(False, "--force", "-f", help="Force reinitialization"),
):
    """Initialize a new pangflow workspace."""
    workspace_path = Path(path).resolve()
    manager = WorkspaceManager(workspace_path)

    if manager.is_initialized and not force:
        console.print(f"[yellow]Workspace already initialized:[/yellow] {workspace_path}")
        console.print("Use --force to reinitialize.")
        raise typer.Exit(0)

    try:
        manager.initialize(force=force)
    except Exception as exc:
        console.print(f"[red]Error initializing workspace:[/red] {exc}")
        raise typer.Exit(1)

    # Copy example project files into the new workspace
    import pangflow as _pf
    pkg_dir = Path(_pf.__file__).parent
    example_dir = pkg_dir / "examples" / "demo_project"
    # Database files that should NOT be copied – the new workspace gets a fresh DB
    DB_FILES = {"pangflow.db", "pangflow.db-shm", "pangflow.db-wal"}
    if example_dir.exists():
        copied = 0
        for src_file in example_dir.iterdir():
            if src_file.is_file() and src_file.name not in DB_FILES:
                dst = workspace_path / src_file.name
                if not dst.exists():
                    shutil.copy2(src_file, dst)
                    copied += 1
        if copied:
            console.print(f"  [dim]示例文件已复制到 {workspace_path}/ ({copied} 个)[/dim]")

    tree = (
        f"[bold]{workspace_path.name}/[/bold]\n"
        "├── pangflow.toml\n"
        "├── pangflow.db\n"
        "├── workflows/\n"
        "├── logs/\n"
        "├── data/\n"
        "├── temp/\n"
        "├── models/\n"
        "├── features/\n"
        "├── nodes.py\n"
        "├── workflow.py\n"
        "├── service.py\n"
        "└── workflow.toml"
    )
    console.print(Panel(tree, title="[green]Workspace initialized[/green]", expand=False))
    console.print("\n[bold]Next steps:[/bold]")
    console.print(f"  1. Edit workflows in {workspace_path}/workflows/")
    console.print("  2. Register: pangflowctl register <workflow.toml>")
    console.print("  3. Deploy:   pangflowctl deploy <workflow_name>")
    console.print("")
    console.print(f"[dim]Tip: export PANGFLOW_WORKSPACE={workspace_path}[/dim]")
    console.print("      [dim]in your shell profile to make workspace discovery CWD-independent.[/dim]")


# --------------------------------------------------------------------------- #
# workflow
# --------------------------------------------------------------------------- #

workflow_app = typer.Typer(help="Manage registered workflows")
app.add_typer(workflow_app, name="workflow")


@workflow_app.command("list")
def workflow_list():
    """List all registered workflows."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        repo = WorkflowRepository(session)
        models = repo.list_all()

    if not models:
        console.print("No workflows found.")
        return

    table = Table(title="Workflows")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="magenta")
    table.add_column("Type", style="yellow")
    table.add_column("Status", style="green")
    table.add_column("Deployed", style="blue")

    for m in models:
        table.add_row(
            m.workflow_name,
            m.version or "N/A",
            m.workflow_type,
            m.status,
            "Yes" if m.is_deployed else "No",
        )
    console.print(table)


@workflow_app.command("get")
def workflow_get(
    name: str = typer.Argument(..., help="Workflow name"),
):
    """Show details of a registered workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        model = _lookup_workflow(session, name)

    console.print(f"[bold]Workflow:[/bold] {model.workflow_name}")
    console.print(f"  ID:          {model.id}")
    console.print(f"  Version:     {model.version}")
    console.print(f"  Type:        {model.workflow_type}")
    console.print(f"  Status:      {model.status}")
    console.print(f"  Deployed:    {'Yes' if model.is_deployed else 'No'}")
    console.print(f"  Deployment:  {model.deployment_id or 'N/A'}")
    console.print(f"  Flow ID:     {model.flow_id or 'N/A'}")
    console.print(f"  Serve PID:   {model.prefect_serve_pid or 'N/A'}")
    console.print(f"  Created:     {model.created_at}")
    console.print(f"  Updated:     {model.updated_at}")


@workflow_app.command("delete")
def workflow_delete(
    name: str = typer.Argument(..., help="Workflow name"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete a registered workflow and its associated data."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        model = _lookup_workflow(session, name)
        workflow_id = model.id

        if not force:
            confirm = input(f"Delete workflow '{name}' and all associated data? [y/N]: ")
            if confirm.lower() != "y":
                console.print("Cancelled.")
                raise typer.Exit(0)

        repo = WorkflowRepository(session)
        repo.delete(workflow_id)

    console.print(f"[green]Deleted workflow[/green] {name}")


# --------------------------------------------------------------------------- #
# env
# --------------------------------------------------------------------------- #

env_app = typer.Typer(help="Manage conda environments for workflows")
app.add_typer(env_app, name="env")


@env_app.command("create")
def env_create(
    workflow: str = typer.Option(..., "--workflow", help="Workflow name"),
    file: Optional[str] = typer.Option(None, "--file", "-f", help="Path to workflow TOML config (default: workflow.toml in workspace)"),
):
    """Create a conda environment for a workflow from its TOML config."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    # Locate TOML file
    if file:
        toml_path = Path(file)
    else:
        toml_path = workspace.workspace_path / "workflow.toml"

    if not toml_path.exists():
        console.print(f"[red]Error:[/red] TOML config not found: {toml_path}")
        console.print("Use --file to specify the workflow TOML file.")
        raise typer.Exit(1)

    with _db_session(db_manager) as session:
        wf = _lookup_workflow(session, workflow)
        env_spec = _env_spec_from_toml(toml_path, wf.workflow_name)
        env_manager = EnvManager()
        env = env_manager.create_env(env_spec, workflow_id=wf.id)

    if env:
        console.print(f"[green]Created environment[/green] {env.name} (python={env_spec.python}) for workflow {workflow}")
    else:
        console.print(f"[red]Failed to create environment for {workflow}[/red]")
        raise typer.Exit(1)


@env_app.command("update")
def env_update(
    workflow: str = typer.Option(..., "--workflow", help="Workflow name"),
    file: Optional[str] = typer.Option(None, "--file", "-f", help="Path to workflow TOML config (default: workflow.toml in workspace)"),
):
    """Update the conda environment linked to a workflow from its TOML config."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    # Locate TOML file
    if file:
        toml_path = Path(file)
    else:
        toml_path = workspace.workspace_path / "workflow.toml"

    if not toml_path.exists():
        console.print(f"[red]Error:[/red] TOML config not found: {toml_path}")
        console.print("Use --file to specify the workflow TOML file.")
        raise typer.Exit(1)

    with _db_session(db_manager) as session:
        wf = _lookup_workflow(session, workflow)
        env_spec = _env_spec_from_toml(toml_path, wf.workflow_name)
        env_manager = EnvManager()
        env = env_manager.update_env(wf.id, env_spec)

    if env:
        console.print(f"[green]Updated environment[/green] {env.name} (python={env_spec.python}) for workflow {workflow}")
    else:
        console.print(f"[red]Failed to update environment for {workflow}[/red]")
        raise typer.Exit(1)


@env_app.command("remove")
def env_remove(
    workflow: str = typer.Option(..., "--workflow", help="Workflow name"),
):
    """Remove the conda environment linked to a workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        wf = _lookup_workflow(session, workflow)
        env_manager = EnvManager()
        ok = env_manager.remove_env(wf.id)

    if ok:
        console.print(f"[green]Removed environment[/green] for workflow {workflow}")
    else:
        console.print(f"[red]Failed to remove environment for {workflow}[/red]")
        raise typer.Exit(1)


@env_app.command("list")
def env_list():
    """List all persisted environments."""
    workspace = _get_workspace()
    _init_db(workspace)

    env_manager = EnvManager()
    envs = env_manager.list_envs()

    if not envs:
        console.print("No environments found.")
        return

    table = Table(title="Environments")
    table.add_column("Name", style="cyan")
    table.add_column("Python", style="magenta")
    table.add_column("Conda Prefix", style="green")

    for env in envs:
        table.add_row(env.name, env.python_version, env.conda_prefix or "N/A")

    console.print(table)


# --------------------------------------------------------------------------- #
# register
# --------------------------------------------------------------------------- #


@app.command()
def register(
    toml_file: str = typer.Argument(..., help="Path to workflow TOML file"),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Override workflow name from TOML"),
):
    """Register a workflow from a TOML file."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)
    toml_path = Path(toml_file)

    if not toml_path.exists():
        console.print(f"[red]Error:[/red] File not found: {toml_path}")
        raise typer.Exit(1)

    try:
        from pangflow.core.config import ConfigLoader
        loader = ConfigLoader()
        cfg = loader.load(str(toml_path))
    except Exception as exc:
        console.print(f"[red]Error parsing TOML:[/red] {exc}")
        raise typer.Exit(1)

    def _serialize(v):
        if hasattr(v, "model_dump"):
            return v.model_dump()
        if isinstance(v, dict):
            return {k: _serialize(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_serialize(item) for item in v]
        return v

    wf_name = name or cfg.name or toml_path.stem
    # Auto-infer command if not explicitly provided in TOML
    command = ""
    if toml_path.with_suffix(".py").exists():
        command = f"python {toml_path.with_suffix('.py').name}"
    schedule_data = {}
    if hasattr(cfg, "schedule") and cfg.schedule and getattr(cfg.schedule, "expression", None):
        schedule_data = {"type": cfg.schedule.type, "expression": cfg.schedule.expression}
    workflow_state = WorkflowState(
        workflow_id=str(uuid.uuid4()),
        workflow_name=wf_name,
        package_name="default",
        command=command,
        workflow_type=WorkflowType.TRIGGER,
        status=WorkflowStatus.REGISTERED,
        is_deployed=False,
        metadata={
            "description": cfg.description,
            "schedule": schedule_data,
            "env": _serialize(cfg.env),
            "storage": _serialize(cfg.storage),
            "log": _serialize(cfg.log),
            "serve": _serialize(cfg.serve),
            "nodes": _serialize(cfg.nodes),
        },
    )

    with _db_session(db_manager) as session:
        repo = WorkflowRepository(session)
        existing = (
            session.query(WorkflowModel)
            .filter_by(workflow_name=wf_name)
            .order_by(WorkflowModel.created_at.desc())
            .first()
        )
        if existing is not None:
            # Update existing workflow record
            existing.command = command
            existing.schedule_config = json.dumps(workflow_state.metadata)
            existing.status = WorkflowStatus.REGISTERED.value
            existing.is_deployed = False
            existing.updated_at = datetime.now()
            console.print(f"[green]Updated workflow[/green] {wf_name}")
            console.print(f"  ID:   {existing.id}")
            if name and name != (cfg.name or toml_path.stem):
                console.print(f"  [yellow]Note:[/yellow] Name overridden by --name from '{cfg.name or toml_path.stem}' to '{name}'")
        else:
            model = repo.create(workflow_state)
            console.print(f"[green]Registered workflow[/green] {wf_name}")
            console.print(f"  ID:   {model.id}")
        console.print(f"  Type: {workflow_state.workflow_type.value}")


# --------------------------------------------------------------------------- #
# deploy
# --------------------------------------------------------------------------- #


@app.command()
def deploy(
    workflow_name: str = typer.Argument(..., help="Name of the registered workflow"),
    cron: Optional[str] = typer.Option(None, "--cron", help="Cron schedule expression"),
    no_serve: bool = typer.Option(False, "--no-serve", help="Do not auto-start the Prefect serve process"),
):
    """Deploy a registered workflow to Prefect and auto-start serve."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        repo = WorkflowRepository(session)
        model = _lookup_workflow(session, workflow_name)
        workflow_state = repo.to_workflow_state(model)

        # If --cron not provided, read from TOML config stored in schedule_config
        if cron is None and model.schedule_config:
            import json
            meta = json.loads(model.schedule_config) if isinstance(model.schedule_config, str) else model.schedule_config
            schedule_cfg = meta.get("schedule", {}) if isinstance(meta, dict) else {}
            if schedule_cfg and schedule_cfg.get("expression"):
                cron = schedule_cfg["expression"]
                console.print(f"[blue]Using schedule from config:[/blue] {cron}")

        deployment_manager = DeploymentManager()
        result = deployment_manager.deploy_to_server(
            workflow_state=workflow_state,
            schedule_cron=cron,
            auto_start=not no_serve,
        )

        if not result["success"]:
            console.print(f"[red]Deployment failed:[/red] {result.get('error')}")
            raise typer.Exit(1)

        deployment_info = result.get("deployment", {})
        repo.update_deployment_info(
            workflow_id=model.id,
            deployment_id=result.get("deployment_id"),
            flow_id=result.get("flow_id"),
            flow_file_path=deployment_info.get("flow_file"),
            is_deployed=True,
        )
        # Persist Prefect serve PID and status
        prefect_serve_pid = result.get("serve_pid")
        model.prefect_serve_pid = prefect_serve_pid
        model.prefect_serve_status = "running" if prefect_serve_pid else "stopped"
        model.updated_at = datetime.now()


        repo.update_status(model.id, WorkflowStatus.DEPLOYED)

    console.print(f"[green]Deployed workflow[/green] {workflow_name}")
    console.print(f"  Deployment ID: {result.get('deployment_id')}")
    console.print(f"  Flow ID:       {result.get('flow_id')}")
    if result.get("serve_pid"):
        console.print(f"  Serve PID:     {result['serve_pid']} (auto-started)")
    else:
        console.print(f"  Serve:         not started (use --no-serve or run 'pangflowctl deployment serve {workflow_name}')")


# --------------------------------------------------------------------------- #
# deployment
# --------------------------------------------------------------------------- #

deployment_app = typer.Typer(help="Manage Prefect deployment serve processes")
app.add_typer(deployment_app, name="deployment")


def _is_uuid(value: str) -> bool:
    """Return True if *value* looks like a UUID."""
    import re
    return bool(
        re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            value,
            re.I,
        )
    )


def _is_process_running(pid: int) -> bool:
    """Check if a process with the given PID is still running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


@deployment_app.command("serve")
def deployment_serve(
    workflow_name: str = typer.Argument(..., help="Name of the deployed workflow"),
):
    """Start (or restart) the Prefect serve process for a deployed workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        model = _lookup_workflow(session, workflow_name)
        if not model.flow_file_path:
            console.print(f"[red]Error:[/red] Workflow '{workflow_name}' has no flow file. Run 'pangflowctl deploy {workflow_name}' first.")
            raise typer.Exit(1)

        flow_file = Path(model.flow_file_path)
        if not flow_file.exists():
            console.print(f"[red]Error:[/red] Flow file not found: {flow_file}")
            raise typer.Exit(1)

        # Check if already running
        if model.prefect_serve_pid and _is_process_running(model.prefect_serve_pid):
            console.print(f"[yellow]Prefect serve already running[/yellow] for {workflow_name} (PID: {model.prefect_serve_pid})")
            return

        # Start serve process
        try:
            import subprocess, sys
            from datetime import datetime
            log_dir = workspace.workspace_path / "logs"
            log_dir.mkdir(exist_ok=True)
            log_file = log_dir / f"{workflow_name}_serve.log"
            log_fp = open(log_file, "a", encoding="utf-8")
            log_fp.write(f"\n--- Prefect serve started at {datetime.now().isoformat()} ---\n")
            log_fp.flush()

            if sys.platform == "win32":
                process = subprocess.Popen(
                    [sys.executable, str(flow_file)],
                    stdout=log_fp,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                process = subprocess.Popen(
                    [sys.executable, str(flow_file)],
                    stdout=log_fp,
                    stderr=subprocess.STDOUT,
                    start_new_session=True
                )
            import time
            time.sleep(2)
            if process.poll() is not None:
                log_fp.write(f"Serve process exited early with code {process.returncode}\n")
                log_fp.flush()
                log_fp.close()
                console.print(f"[red]Serve process failed to start; see {log_file}[/red]")
                raise typer.Exit(1)

            model.prefect_serve_pid = process.pid
            model.prefect_serve_status = "running"
            model.updated_at = datetime.now()

            console.print(f"[green]Started Prefect serve[/green] for {workflow_name} (PID: {process.pid}, log: {log_file})")
        except Exception as exc:
            console.print(f"[red]Failed to start serve:[/red] {exc}")
            raise typer.Exit(1)


@deployment_app.command("stop")
def deployment_stop(
    workflow_name: str = typer.Argument(..., help="Name of the deployed workflow"),
):
    """Stop the Prefect serve process for a deployed workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        model = _lookup_workflow(session, workflow_name)
        pid = model.prefect_serve_pid
        if not pid:
            console.print(f"[yellow]No Prefect serve process found[/yellow] for {workflow_name}")
            return

        if not _is_process_running(pid):
            console.print(f"[yellow]Serve process (PID: {pid}) is not running[/yellow]")
            model.prefect_serve_pid = None
            model.prefect_serve_status = "stopped"

            return

        try:
            if sys.platform == "win32":
                os.kill(pid, signal.CTRL_BREAK_EVENT)
            else:
                os.kill(pid, signal.SIGTERM)
            console.print(f"[green]Stopped Prefect serve[/green] for {workflow_name} (PID: {pid})")
            model.prefect_serve_pid = None
            model.prefect_serve_status = "stopped"
            model.updated_at = datetime.now()

        except Exception as exc:
            console.print(f"[red]Failed to stop serve process:[/red] {exc}")
            raise typer.Exit(1)


@deployment_app.command("status")
def deployment_status():
    """Show status of all Prefect deployments and serve processes."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        models = session.query(WorkflowModel).filter(WorkflowModel.is_deployed == True).all()

    if not models:
        console.print("No deployed workflows found.")
        return

    table = Table(title="Prefect Deployments")
    table.add_column("Workflow", style="cyan")
    table.add_column("Deployment ID", style="magenta")
    table.add_column("Flow ID", style="green")
    table.add_column("Serve PID", style="yellow")
    table.add_column("Serve Status", style="blue")

    for model in models:
        pid = model.prefect_serve_pid
        if pid and not _is_process_running(pid):
            status = "crashed"
        else:
            status = model.prefect_serve_status or "unknown"
        table.add_row(
            model.workflow_name,
            model.deployment_id or "N/A",
            model.flow_id or "N/A",
            str(pid) if pid else "N/A",
            status,
        )

    console.print(table)


# --------------------------------------------------------------------------- #
# serve
# --------------------------------------------------------------------------- #

serve_app = typer.Typer(help="Manage HTTP serve processes for workflows")
app.add_typer(serve_app, name="serve")

webui_app = typer.Typer(help="Manage the PangFlow WebUI dashboard")
app.add_typer(webui_app, name="webui")


@webui_app.command("start")
def webui_start(
    port: int = typer.Option(8080, "--port", "-p", help="Port to run the WebUI on"),
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host to bind the WebUI to"),
):
    """Start the PangFlow WebUI dashboard server."""
    from pangflow.web.server import create_web_app
    import uvicorn

    workspace = _get_workspace()
    _init_db(workspace)

    web_app = create_web_app()
    console.print(f"[green]Starting PangFlow WebUI[/green] at http://{host}:{port}/ui")
    console.print("Press Ctrl+C to stop.")
    uvicorn.run(web_app, host=host, port=port, log_level="info")


@webui_app.command("status")
def webui_status():
    """Show WebUI status."""
    console.print("[blue]PangFlow WebUI[/blue] is bundled with the Python package.")
    console.print("Run [bold]pangflowctl webui start[/bold] to launch it.")
    console.print("Once started, open [bold]http://127.0.0.1:8080/ui[/bold] in your browser.")


def _build_trigger_app(command: str, working_dir: Optional[str] = None):
    """Build a minimal FastAPI app that triggers a workflow command."""
    try:
        from fastapi import FastAPI
    except ImportError:
        raise RuntimeError("FastAPI is required for serve operations.")

    app = FastAPI(title="PangFlow Serve")

    @app.post("/trigger")
    def trigger():
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=working_dir,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    return app


@serve_app.command("start")
def serve_start(
    workflow_name: str = typer.Argument(..., help="Workflow name to serve"),
    port: int = typer.Option(8000, "--port", help="Server port"),
    host: str = typer.Option("127.0.0.1", "--host", help="Server host"),
):
    """Start an HTTP serve process for a workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        model = _lookup_workflow(session, workflow_name)
        command = model.command
        working_dir = model.working_dir
        model_id = model.id

        # Clean stale service records for this workflow before starting a new one
        stale = (
            session.query(ServiceModel)
            .filter_by(service_name=workflow_name)
            .all()
        )
        for svc in stale:
            if svc.pid and not _is_process_running(svc.pid):
                session.delete(svc)

    workspace_path = workspace.workspace_path

    # Check for conda environment
    env_manager = EnvManager()
    env = env_manager.get_env(model_id)

    def _discover_service_files(wp: Path, wf_name: str):
        import glob
        all_files = glob.glob(str(wp / "*service.py"))
        if not all_files:
            return []
        exact = str(wp / f"{wf_name}_service.py")
        under = str(wp / f"{wf_name.replace('-', '_')}_service.py")
        matches = [f for f in all_files if f == exact or f == under]
        if matches:
            return matches
        words = set(wf_name.replace('-', '_').lower().split('_'))
        matches = [f for f in all_files if any(w in Path(f).stem.lower() for w in words)]
        if matches:
            return matches
        plain = str(wp / "service.py")
        if plain in all_files:
            return [plain]
        if len(all_files) == 1:
            return all_files
        return all_files

    svc_files = _discover_service_files(workspace_path, workflow_name)

    if env is not None:
        # Conda isolation: generate a temporary script and run it via conda run
        import tempfile
        # Resolve pangflow root so the conda subprocess can import it
        import importlib.util
        pangflow_spec = importlib.util.find_spec("pangflow")
        pangflow_pkg_root = (
            str(Path(pangflow_spec.origin).parent.parent)
            if (pangflow_spec and pangflow_spec.origin)
            else ""
        )
        script_lines = [
            "import sys",
            "import os",
            "from pathlib import Path",
            "os.chdir(str(Path(__file__).parent))",
            f"sys.path.insert(0, {str(workspace_path)!r})",
        ]
        if pangflow_pkg_root:
            script_lines += [
                f"_PANGFLOW_ROOT = {pangflow_pkg_root!r}",
                "if _PANGFLOW_ROOT not in sys.path:",
                "    sys.path.insert(0, _PANGFLOW_ROOT)",
            ]
        script_lines += [
            "",
            "# Ensure runtime deps are available in the conda env",
            "_MISSING = []",
            "for _pkg in ('pydantic', 'fastapi', 'uvicorn'):",
            "    try:",
            "        __import__(_pkg)",
            "    except ImportError:",
            "        _MISSING.append(_pkg)",
            "if _MISSING:",
            "    import subprocess as _sp, sys as _sys",
            "    _sp.check_call([_sys.executable, '-m', 'pip', 'install', '--quiet'] + _MISSING)",
            "",
            "import importlib.util",
        ]
        for svc_path in svc_files:
            script_lines.append(f"spec = importlib.util.spec_from_file_location('_svc_', {svc_path!r})")
            script_lines.append("if spec and spec.loader:")
            script_lines.append("    mod = importlib.util.module_from_spec(spec)")
            script_lines.append("    spec.loader.exec_module(mod)")
        script_lines += [
            "",
            "from pangflow.orchestration.serve_compiler import ServeCompiler",
            "from pangflow.orchestration.registry import NodeRegistry",
            "from fastapi import FastAPI",
            "import os",
            "import subprocess",
            "",
            "serve_compiler = ServeCompiler()",
            "endpoints = NodeRegistry().list_serve()",
            "if endpoints:",
            "    app = serve_compiler.compile(endpoints)",
            "else:",
            "    app = FastAPI(title='PangFlow Serve')",
            "",
            f"command = {command!r}",
            f"working_dir = {working_dir!r}",
            f"os.environ.setdefault('PANGFLOW_WORKFLOW_ID', {model_id!r})",
            "os.environ.setdefault('PANGFLOW_DEFAULT_STAGE', 'production')",
            "",
            "@app.post('/trigger')",
            "def trigger():",
            "    result = subprocess.run(command, shell=True, capture_output=True, text=True, cwd=working_dir, env=os.environ.copy())",
            "    return {'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}",
            "",
            "if __name__ == '__main__':",
            f"    import uvicorn",
            f"    uvicorn.run(app, host={host!r}, port={port})",
        ]
        script_content = "\n".join(script_lines) + "\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix="_serve.py", delete=False, dir=str(workspace_path)) as fh:
            fh.write(script_content)
            script_path = fh.name

        console.print(f"[blue]Starting serve in conda env:[/blue] {env.name}")
        log_path = workspace_path / "logs" / f"{workflow_name}_serve.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = open(str(log_path), "w", encoding="utf-8")
            log_file.write(f"--- Serve started at {datetime.now().isoformat()} ---\n")
            log_file.flush()
            if sys.platform == "win32":
                proc = subprocess.Popen(
                    ["conda", "run", "-n", env.name, "python", script_path],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )
            else:
                proc = subprocess.Popen(
                    ["conda", "run", "-n", env.name, "python", script_path],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            time.sleep(3)
            if proc.poll() is not None:
                log_file.flush()
                with open(str(log_path), "r", encoding="utf-8") as fh:
                    stderr = fh.read()
                console.print(f"[red]Serve process failed:[/red] {stderr}")
                raise typer.Exit(1)

            with _db_session(db_manager) as session:
                service = ServiceModel(
                    service_name=workflow_name,
                    status="running",
                    host=host,
                    port=port,
                    pid=proc.pid,
                    started_at=datetime.now(),
                )
                session.add(service)


            console.print(f"[green]Serve started[/green] {workflow_name} at http://{host}:{port} (conda: {env.name})")
            console.print(f"[dim]Logs: {log_path}[/dim]")
            console.print("Press Ctrl+C to stop.")
            try:
                proc.wait()
            except KeyboardInterrupt:
                proc.terminate()
                console.print(f"\n[yellow]Serve stopped[/yellow] {workflow_name}")
        finally:
            try:
                os.unlink(script_path)
            except OSError:
                pass
            try:
                log_file.close()
            except Exception:
                pass
        return

    # No conda env: run in-process via ServeManager
    import importlib.util
    for svc_path in svc_files:
        spec = importlib.util.spec_from_file_location("_pangflow_svc_", svc_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

    serve_compiler = ServeCompiler()
    endpoints = NodeRegistry().list_serve()
    if endpoints:
        app_obj = serve_compiler.compile(endpoints)
    else:
        from fastapi import FastAPI
        app_obj = FastAPI(title="PangFlow Serve")

    @app_obj.post("/trigger")
    def trigger():
        env = os.environ.copy()
        env.setdefault("PANGFLOW_WORKFLOW_ID", model.id)
        env.setdefault("PANGFLOW_DEFAULT_STAGE", "production")
        result = subprocess.run(command, shell=True, capture_output=True, text=True, cwd=working_dir, env=env)
        return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}

    serve_manager = ServeManager()
    serve_manager.start(app_obj, host=host, port=port)

    time.sleep(1)
    with _db_session(db_manager) as session:
        service = ServiceModel(
            service_name=workflow_name,
            status="running",
            host=host,
            port=port,
            pid=os.getpid(),
            started_at=datetime.now(),
        )
        session.add(service)


    console.print(f"[green]Serve started[/green] {workflow_name} at http://{host}:{port}")
    console.print("Press Ctrl+C to stop.")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        serve_manager.stop()
        console.print(f"\n[yellow]Serve stopped[/yellow] {workflow_name}")


@serve_app.command("stop")
def serve_stop(
    workflow_name: str = typer.Argument(..., help="Workflow name to stop serving"),
):
    """Stop the HTTP serve process for a workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        service = (
            session.query(ServiceModel)
            .filter_by(service_name=workflow_name)
            .order_by(ServiceModel.started_at.desc())
            .first()
        )
        if service is None or service.pid is None:
            console.print(f"[yellow]No serve process found for {workflow_name}[/yellow]")
            raise typer.Exit(0)

        if _is_process_running(service.pid):
            try:
                if sys.platform == "win32":
                    subprocess.run(
                        ["taskkill", "/PID", str(service.pid), "/F"],
                        capture_output=True,
                        check=False,
                    )
                else:
                    try:
                        os.killpg(os.getpgid(service.pid), signal.SIGTERM)
                    except (ProcessLookupError, OSError):
                        os.kill(service.pid, signal.SIGTERM)
            except Exception as exc:
                console.print(f"[red]Error stopping process:[/red] {exc}")
                raise typer.Exit(1)

        service.status = "stopped"
        service.stopped_at = datetime.now()

        pid = service.pid

    console.print(f"[green]Stopped serve[/green] for {workflow_name} (PID {pid})")


@serve_app.command("status")
def serve_status():
    """Show status of all serve processes."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        services = session.query(ServiceModel).all()

        if not services:
            console.print("No serve processes found.")
            return

        table = Table(title="Serve Status")
        table.add_column("Workflow", style="cyan")
        table.add_column("Host:Port", style="magenta")
        table.add_column("PID", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Running", style="blue")

        for svc in services:
            running = _is_process_running(svc.pid)
            if not running and svc.status != "stopped":
                svc.status = "stopped"
                svc.stopped_at = datetime.now()
            status = svc.status or "unknown"
            table.add_row(
                svc.service_name,
                f"{svc.host}:{svc.port}",
                str(svc.pid) if svc.pid else "N/A",
                status,
                "YES" if running else "NO",
            )


    console.print(table)


# --------------------------------------------------------------------------- #
# trigger
# --------------------------------------------------------------------------- #


@app.command()
def trigger(
    workflow_name: str = typer.Argument(..., help="Workflow name to trigger"),
    wait: bool = typer.Option(False, "--wait", help="Wait for execution to complete"),
):
    """Trigger a workflow execution."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        repo = WorkflowRepository(session)
        log_repo = ExecutionLogRepository(session)
        model = _lookup_workflow(session, workflow_name)

        run_id = str(uuid.uuid4())
        log_entry = log_repo.create(
            workflow_id=model.id,
            run_id=run_id,
            execution_type=model.workflow_type,
            status="running",
            triggered_by="user",
        )

        env_manager = EnvManager()
        env = env_manager.get_env(model.id)
        if env is not None:
            command = f"conda run -n {env.name} {model.command}"
        else:
            command = model.command

        # Build env vars; inject PYTHONPATH so conda subprocesses can find pangflow
        env_vars = {"PANGFLOW_RUN_ID": run_id, "PANGFLOW_WORKFLOW_ID": model.id}
        import importlib.util
        pangflow_spec = importlib.util.find_spec("pangflow")
        if pangflow_spec and pangflow_spec.origin:
            pangflow_pkg_root = str(Path(pangflow_spec.origin).parent.parent)
            existing_pp = os.environ.get("PYTHONPATH", "")
            if existing_pp:
                env_vars["PYTHONPATH"] = f"{pangflow_pkg_root}{os.pathsep}{existing_pp}"
            else:
                env_vars["PYTHONPATH"] = pangflow_pkg_root

        context = ExecutionContext(
            workflow_id=model.id,
            workflow_name=model.workflow_name,
            workflow_type=model.workflow_type,
            command=command,
            working_dir=model.working_dir,
            env_vars=env_vars,
        )

        console.print(f"[blue]Triggering[/blue] {workflow_name}  Run ID: {run_id}")
        runner = RunnerFactory.create("local")
        result = runner.run(context)

        log_repo.complete(
            log_id=log_entry.id,
            status=result.status.value,
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.return_code,
        )

        new_status = (
            WorkflowStatus.COMPLETED
            if result.status.value == "success"
            else WorkflowStatus.FAILED
        )
        repo.update_status(model.id, new_status)

    if result.status.value == "success":
        console.print(f"[green]Workflow completed successfully[/green]")
    else:
        console.print(f"[red]Workflow failed:[/red] {result.status.value}")

    if result.stdout:
        console.print(result.stdout)
    if result.stderr:
        console.print(f"[red]{result.stderr}[/red]")


# --------------------------------------------------------------------------- #
# logs
# --------------------------------------------------------------------------- #


@app.command()
def logs(
    workflow_name: str = typer.Argument(..., help="Workflow name"),
    node: Optional[str] = typer.Option(None, "--node", help="Filter by node name"),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum log entries"),
):
    """Show execution logs and node-level logs for a workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        wf = _lookup_workflow(session, workflow_name)

        if node:
            entries = (
                session.query(NodeLogModel)
                .filter(
                    ((NodeLogModel.workflow_id == wf.id) | (NodeLogModel.workflow_id == wf.workflow_name) | (NodeLogModel.workflow_name == wf.workflow_name)),
                    NodeLogModel.node_name == node
                )
                .order_by(NodeLogModel.timestamp.desc())
                .limit(limit)
                .all()
            )
            if not entries:
                console.print(f"No node logs found for {workflow_name}/{node}")
                return

            table = Table(title=f"Node Logs – {workflow_name}/{node}")
            table.add_column("Time", style="cyan")
            table.add_column("Level", style="magenta")
            table.add_column("Message", style="green")
            for entry in entries:
                ts = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S") if entry.timestamp else "N/A"
                table.add_row(ts, entry.level or "-", entry.message or "-")
            console.print(table)
            return

        # Show both execution logs and node-level logs
        log_repo = ExecutionLogRepository(session)
        exec_entries = log_repo.list_by_workflow(wf.id, limit=limit)
        node_entries = (
            session.query(NodeLogModel)
            .filter(
                (NodeLogModel.workflow_id == wf.id)
                | (NodeLogModel.workflow_id == wf.workflow_name)
                | (NodeLogModel.workflow_name == wf.workflow_name)
            )
            .order_by(NodeLogModel.timestamp.desc())
            .limit(limit)
            .all()
        )

        if exec_entries:
            console.print(f"\n[bold]Execution Logs – {workflow_name}[/bold]")
            table = Table()
            table.add_column("Run ID", style="cyan")
            table.add_column("Status", style="magenta")
            table.add_column("Started", style="green")
            table.add_column("Duration", style="yellow")
            for entry in exec_entries:
                started = (
                    entry.started_at.strftime("%Y-%m-%d %H:%M")
                    if entry.started_at else "N/A"
                )
                if entry.completed_at and entry.started_at:
                    duration = f"{(entry.completed_at - entry.started_at).total_seconds():.1f}s"
                else:
                    duration = "N/A"
                table.add_row(entry.run_id[:18], entry.status, started, duration)
            console.print(table)

        if node_entries:
            console.print(f"\n[bold]Node Logs – {workflow_name}[/bold]")
            table = Table()
            table.add_column("Time", style="cyan")
            table.add_column("Node", style="magenta")
            table.add_column("Level", style="green")
            table.add_column("Message", style="yellow")
            for entry in node_entries:
                ts = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S") if entry.timestamp else "N/A"
                table.add_row(
                    ts,
                    entry.node_name or "-",
                    entry.level or "-",
                    (entry.message or "-")[:80],
                )
            console.print(table)

        if not exec_entries and not node_entries:
            console.print(f"No logs found for {workflow_name}")


# --------------------------------------------------------------------------- #
# metrics
# --------------------------------------------------------------------------- #


@app.command()
def metrics(
    workflow_name: str = typer.Argument(..., help="Workflow name"),
):
    """Show recorded metrics for a workflow."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        wf = _lookup_workflow(session, workflow_name)
        rows = (
            session.query(NodeLogModel)
            .filter(
                NodeLogModel.log_type == "metric",
                (
                    (NodeLogModel.workflow_id == wf.id)
                    | (NodeLogModel.workflow_id == wf.workflow_name)
                    | (NodeLogModel.workflow_name == wf.workflow_name)
                ),
            )
            .order_by(NodeLogModel.timestamp.desc())
            .all()
        )

    if not rows:
        console.print(f"No metrics found for {workflow_name}")
        return

    table = Table(title=f"Metrics – {workflow_name}")
    table.add_column("Timestamp", style="cyan")
    table.add_column("Metric", style="magenta")
    table.add_column("Value", style="green")
    table.add_column("Node", style="yellow")

    for row in rows:
        ts = row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "N/A"
        table.add_row(
            ts,
            row.metric_name or "-",
            str(row.metric_value) if row.metric_value is not None else "-",
            row.node_name or "-",
        )

    console.print(table)


# --------------------------------------------------------------------------- #
# lineage
# --------------------------------------------------------------------------- #


@app.command()
def lineage(
    workflow_name: str = typer.Argument(..., help="Workflow name"),
):
    """Show data lineage for a workflow with human-readable artifact names."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        wf = _lookup_workflow(session, workflow_name)
        artifacts = (
            session.query(ArtifactModel)
            .filter(
                (ArtifactModel.workflow_id == wf.id)
                | (ArtifactModel.workflow_id == wf.workflow_name)
                | (ArtifactModel.workflow_id == "")
            )
            .all()
        )
        artifact_ids = {a.artifact_id for a in artifacts}
        edges = (
            session.query(LineageEdgeModel)
            .filter(
                LineageEdgeModel.from_artifact_id.in_(artifact_ids)
                | LineageEdgeModel.to_artifact_id.in_(artifact_ids)
            )
            .all()
        )

        # Build an id -> name lookup for all related artifacts
        all_related_ids = {e.from_artifact_id for e in edges} | {e.to_artifact_id for e in edges}
        name_lookup = {}
        for aid in all_related_ids:
            art = session.query(ArtifactModel).filter_by(artifact_id=aid).first()
            name_lookup[aid] = art.name if art else aid[:8]

    if not edges:
        console.print(f"No lineage edges found for {workflow_name}")
        return

    table = Table(title=f"Lineage – {workflow_name}")
    table.add_column("From", style="cyan")
    table.add_column("To", style="magenta")
    table.add_column("Type", style="green")

    for edge in edges:
        from_label = f"{name_lookup.get(edge.from_artifact_id, edge.from_artifact_id[:8])} ({edge.from_artifact_id[:8]})"
        to_label = f"{name_lookup.get(edge.to_artifact_id, edge.to_artifact_id[:8])} ({edge.to_artifact_id[:8]})"
        table.add_row(from_label, to_label, edge.edge_type)

    console.print(table)


# --------------------------------------------------------------------------- #
# artifact (replaces model, with backward compatibility)
# --------------------------------------------------------------------------- #

artifact_app = typer.Typer(help="Manage artifacts (models, data, features)")
app.add_typer(artifact_app, name="artifact")
# Backward-compatible alias
model_app = artifact_app
app.add_typer(model_app, name="model")


@artifact_app.command("list")
def artifact_list(
    artifact_type: Optional[str] = typer.Option(None, "--type", "-t", help="Filter by type: model, data, feature"),
    workflow: Optional[str] = typer.Option(None, "--workflow", help="Filter by workflow name"),
):
    """List registered artifacts (latest version per name)."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    rows: list = []
    with _db_session(db_manager) as session:
        query = session.query(ArtifactModel)
        if artifact_type:
            query = query.filter_by(artifact_type=artifact_type)
        if workflow:
            wf = _lookup_workflow(session, workflow)
            query = query.filter_by(workflow_id=wf.id)
        artifacts = query.order_by(ArtifactModel.created_at.desc()).all()

        from collections import OrderedDict
        seen = OrderedDict()
        for a in artifacts:
            if a.name not in seen:
                seen[a.name] = a
        artifacts = list(seen.values())

        for art in artifacts:
            tags = json.loads(art.tags_json or "{}")
            wf_name = "-"
            if art.workflow_id:
                wf = session.query(WorkflowModel).filter_by(id=art.workflow_id).first()
                wf_name = wf.workflow_name if wf else "-"
            rows.append(
                (
                    art.name,
                    art.version,
                    art.artifact_type,
                    wf_name,
                    tags.get("stage", "-"),
                    art.created_at.strftime("%Y-%m-%d %H:%M") if art.created_at else "-",
                )
            )

    if not rows:
        console.print("No artifacts found.")
        return

    table = Table(title="Artifacts")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="magenta")
    table.add_column("Type", style="white")
    table.add_column("Workflow", style="green")
    table.add_column("Stage", style="yellow")
    table.add_column("Created", style="blue")

    for row in rows:
        table.add_row(*row)

    console.print(table)


@artifact_app.command("versions")
def artifact_versions(
    name: str = typer.Argument(..., help="Artifact name"),
):
    """List all versions of a named artifact."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    rows: list = []
    with _db_session(db_manager) as session:
        artifacts = (
            session.query(ArtifactModel)
            .filter_by(name=name)
            .order_by(ArtifactModel.created_at.desc())
            .all()
        )
        if not artifacts:
            console.print(f"[yellow]No artifacts found with name:[/yellow] {name}")
            raise typer.Exit(0)

        for art in artifacts:
            tags = json.loads(art.tags_json or "{}")
            rows.append(
                (
                    art.name,
                    art.version,
                    art.artifact_type,
                    art.artifact_id[:8],
                    tags.get("stage", "-"),
                    art.created_at.strftime("%Y-%m-%d %H:%M") if art.created_at else "-",
                )
            )

    table = Table(title=f"Artifact Versions – {name}")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="magenta")
    table.add_column("Type", style="white")
    table.add_column("ID", style="green")
    table.add_column("Stage", style="yellow")
    table.add_column("Created", style="blue")

    for row in rows:
        table.add_row(*row)

    console.print(table)


@artifact_app.command("promote")
def artifact_promote(
    version: str = typer.Argument(..., help="Artifact ID or name:version"),
    to: str = typer.Option(..., "--to", help="Target stage (e.g. staging, production)"),
):
    """Promote an artifact to a new stage."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)
    store = _model_store(workspace)

    artifact_id = version
    if ":" in version and not _is_uuid(version):
        name, ver = version.split(":", 1)
        with _db_session(db_manager) as session:
            art = (
                session.query(ArtifactModel)
                .filter_by(name=name, version=ver)
                .order_by(ArtifactModel.created_at.desc())
                .first()
            )
            if art is None:
                console.print(f"[red]Artifact not found:[/red] {version}")
                raise typer.Exit(1)
            artifact_id = art.artifact_id

    try:
        store.promote_version(artifact_id, stage=to)
        console.print(f"[green]Promoted[/green] {artifact_id} → {to}")
        console.print(f"  [dim]Load with:[/dim] pf.load_model(\"{artifact_id}\", stage=\"{to}\")")
    except Exception as exc:
        console.print(f"[red]Promotion failed:[/red] {exc}")
        raise typer.Exit(1)


@artifact_app.command("rollback")
def artifact_rollback(
    version: str = typer.Argument(..., help="Artifact ID or name:version"),
):
    """Rollback an artifact to the previous stage."""
    workspace = _get_workspace()
    db_manager = _init_db(workspace)

    artifact_id = version
    if ":" in version and not _is_uuid(version):
        name, ver = version.split(":", 1)
        with _db_session(db_manager) as session:
            art = (
                session.query(ArtifactModel)
                .filter_by(name=name, version=ver)
                .order_by(ArtifactModel.created_at.desc())
                .first()
            )
            if art is None:
                console.print(f"[red]Artifact not found:[/red] {version}")
                raise typer.Exit(1)
            artifact_id = art.artifact_id

    with _db_session(db_manager) as session:
        artifact = session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()
        if artifact is None:
            console.print(f"[red]Artifact not found:[/red] {artifact_id}")
            raise typer.Exit(1)

        prev = (
            session.query(ArtifactVersionModel)
            .filter_by(artifact_id=artifact_id)
            .order_by(ArtifactVersionModel.created_at.desc())
            .offset(1)
            .first()
        )
        if prev is None:
            prev_stage = "development"
        else:
            prev_stage = prev.stage
        tags = json.loads(artifact.tags_json or "{}")
        tags["stage"] = prev_stage
        artifact.tags_json = json.dumps(tags)

    console.print(f"[green]Rolled back[/green] {artifact_id} to stage '{prev_stage}'")
    console.print(f"  [dim]Load with:[/dim] pf.load_model(\"{artifact_id}\", stage=\"{prev_stage}\")")


# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #


@app.command()
def status():
    """Show overall pangflow status."""
    try:
        workspace = require_workspace()
    except FileNotFoundError:
        console.print("[yellow]No workspace initialized.[/yellow] Run 'pangflowctl init'.")
        raise typer.Exit(0)

    db_manager = _init_db(workspace)

    with _db_session(db_manager) as session:
        wf_count = session.query(WorkflowModel).count()
        deployed_count = session.query(WorkflowModel).filter_by(is_deployed=True).count()
        service_count = session.query(ServiceModel).count()
        env_count = session.query(EnvironmentModel).count()
        artifact_count = session.query(ArtifactModel).count()

    grid = Table.grid(padding=1)
    grid.add_column(style="cyan", justify="right")
    grid.add_column(style="white")

    grid.add_row("Workspace:", str(workspace.workspace_path))
    grid.add_row("Database:", str(workspace.database_path))
    grid.add_row("Workflows:", f"{wf_count} ({deployed_count} deployed)")
    grid.add_row("Services:", str(service_count))
    grid.add_row("Environments:", str(env_count))
    grid.add_row("Artifacts:", str(artifact_count))

    console.print(Panel(grid, title="[bold green]pangflowctl status[/bold green]", expand=False))


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def cli():
    """CLI entry point referenced by pyproject.toml console script."""
    app()


if __name__ == "__main__":
    cli()
