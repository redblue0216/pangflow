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

This is a workspace management module for pangflow.

- Design mode:

    (1) Workspace initialization and management

    (2) Directory structure creation

    (3) Workflow file discovery and management

- Key points:

    (1) Handles workspace initialization with required directory structure

    (2) Manages workspace configuration files

    (3) Provides workflow file discovery and creation

    (4) Supports workspace auto-discovery by walking up directory tree

- Main functions:

    (1) WorkspaceManager - Class for managing pangflow workspaces

    (2) find_workspace - Function to find a pangflow workspace by walking up the directory tree

    (3) require_workspace - Function to get a workspace manager, ensuring workspace is initialized

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.utils import WorkspaceManager, find_workspace, require_workspace
    
    ### Initialize a new workspace
    manager = WorkspaceManager("/path/to/workspace")
    manager.initialize()
    
    ### Find workspace from current directory
    workspace_path = find_workspace()
    
    ### Require initialized workspace
    manager = require_workspace()
    
    ### Create a workflow file
    workflow_path = manager.create_workflow_file("my-workflow", "trigger", "PangTS")
    
    ### Find all workflow files
    workflows = manager.find_workflow_files()

Description of Class and Function
---------------------------------
(1)WorkspaceManager: Class for managing pangflow workspaces including initialization,
    structure creation, workflow file discovery, and workflow file creation.

(2)find_workspace: Function to find a pangflow workspace by walking up the directory tree
    from a starting path.

(3)require_workspace: Function to get a WorkspaceManager instance, ensuring the workspace
    is properly initialized.

References
----------
pangflow Documentation
Python pathlib Module
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
import os
import logging
from pathlib import Path
from typing import Optional, List
### Internal packages
from pangflow.utils.toml_parser import TomlParser
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: WorkspaceManager
### ------Define a class for managing pangflow workspaces
###
### function: find_workspace
### ------Define a function to find a pangflow workspace
###
### function: require_workspace
### ------Define a function to get a workspace manager with initialization check
###
######################################################################################################



class WorkspaceManager:
    '''
    Class Introduction
    ------------------
    
    Manager for pangflow workspaces.
    
    - Design mode:
    
        (1) Workspace lifecycle management

        (2) Directory structure management
        
        (3) Configuration management
    
    - Key points:
    
        (1) Handles workspace initialization with required directory structure

        (2) Creates and manages workspace configuration files

        (3) Provides workflow file discovery and creation

        (4) Supports checking workspace initialization status
    
    - Main class attributes:
    
        (1) DIRECTORIES - List of directories to create in workspace

        (2) CONFIG_FILE - Name of the workspace configuration file
    
    - Main methods:
    
        (1) initialize - Initialize a new workspace

        (2) find_workflow_files - Find all workflow TOML files

        (3) create_workflow_file - Create a new workflow file

        (4) read_config - Read workspace configuration
    
    Attribute Function
    ------------------
    :parameters:
        - workspace_path (str | Path): Path to the workspace directory
    
    :return: WorkspaceManager instance
    
    Method Function
    ---------------
    (1)__init__: Initialize the workspace manager with a path

    (2)is_initialized: Property to check if workspace is initialized

    (3)config_path: Property to get path to workspace configuration file

    (4)database_path: Property to get path to SQLite database file

    (5)workflows_dir: Property to get path to workflows directory

    (6)logs_dir: Property to get path to logs directory

    (7)initialize: Initialize a new pangflow workspace

    (8)_create_config: Create the workspace configuration file

    (9)_create_example_workflows: Create example workflow files

    (10)read_config: Read the workspace configuration

    (11)find_workflow_files: Find all workflow TOML files in the workspace

    (12)get_workflow_file: Get the path to a specific workflow file

    (13)create_workflow_file: Create a new workflow file in the workspace
    
    References
    ----------
    pangflow Documentation
    TomlParser class
    '''
    

    # Directory structure within a workspace
    DIRECTORIES = [
        "workflows",      # Workflow TOML files
        "logs",          # Execution logs
        "data",          # Workflow data files
        "temp",          # Temporary files
        "models",        # Model artifacts
        "features",      # Feature artifacts
    ]
    # Configuration file name
    CONFIG_FILE = "pangflow.toml"
    

    def __init__(self, workspace_path: str | Path):
        '''Method Function:
        
        Initialize the workspace manager.
        
        - Design mode:
        
            (1) Path resolution

            (2) State initialization
        
        - Key points:
        
            (1) Resolves the workspace path to absolute path

            (2) Sets initialization flag to False initially
        
        :parameters:
            - workspace_path (str | Path) - Path to the workspace directory
        
        :return: None
        '''

        self.workspace_path = Path(workspace_path).resolve()
        self._is_initialized = False
    

    @property
    def is_initialized(self) -> bool:
        '''Method Function:
        
        Check if the workspace is initialized.
        
        - Design mode:
        
            (1) State checking
        
        - Key points:
        
            (1) Checks for existence of config file

            (2) Checks for existence of workflows directory

            (3) Returns boolean indicating initialization status
        
        :parameters: 
            None
        
        :return: 
            - bool - True if workspace is initialized, False otherwise
        '''

        # Check for config file and key directories
        config_file = self.workspace_path / self.CONFIG_FILE
        workflows_dir = self.workspace_path / "workflows"
        return config_file.exists() and workflows_dir.exists()
    

    @property
    def config_path(self) -> Path:
        '''Method Function:
        
        Get the path to the workspace configuration file.
        
        - Design mode:
        
            (1) Path construction
        
        - Key points:
        
            (1) Constructs path by joining workspace_path with CONFIG_FILE
        
        :parameters:
            None
        
        :return: 
            Path to the workspace configuration file
        '''

        return self.workspace_path / self.CONFIG_FILE
    

    @property
    def database_path(self) -> Path:
        '''Method Function:
        
        Get the path to the SQLite database file.
        
        - Design mode:
        
            (1) Path construction
        
        - Key points:
        
            (1) Constructs path by joining workspace_path with "pangflow.db"
        
        :parameters: 
            None
        
        :return: 
            Path to the SQLite database file
        '''

        return self.workspace_path / "pangflow.db"
    

    @property
    def workflows_dir(self) -> Path:
        '''Method Function:
        
        Get the path to the workflows directory.
        
        - Design mode:
        
            (1) Path construction
        
        - Key points:
        
            (1) Constructs path by joining workspace_path with "workflows"
        
        :parameters: 
            None
        
        :return: 
            Path to the workflows directory
        '''

        return self.workspace_path / "workflows"
    

    @property
    def logs_dir(self) -> Path:
        '''Method Function:
        
        Get the path to the logs directory.
        
        - Design mode:
        
            (1) Path construction
        
        - Key points:
        
            (1) Constructs path by joining workspace_path with "logs"
        
        :parameters: 
            None
        
        :return: 
            Path to the logs directory
        '''

        return self.workspace_path / "logs"
    

    def initialize(self, force: bool = False) -> bool:
        '''Method Function:
        
        Initialize a new pangflow workspace.
        
        - Design mode:
        
            (1) Directory creation

            (2) Configuration setup

            (3) Example generation
        
        - Key points:
        
            (1) Creates workspace directory and all subdirectories

            (2) Creates workspace configuration file

            (3) Creates example workflow files

            (4) Skips if already initialized unless force=True
        
        :parameters:
            - force (bool) - If True, reinitialize even if already initialized
        
        :return: 
            - bool - True if initialization was successful
        '''

        if self.is_initialized and not force:
            logger.info(f"Workspace already initialized: {self.workspace_path}")
            return True
        logger.info(f"Initializing pangflow workspace: {self.workspace_path}")
        # Create workspace directory
        self.workspace_path.mkdir(parents=True, exist_ok=True)
        # Create subdirectories
        for dir_name in self.DIRECTORIES:
            dir_path = self.workspace_path / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {dir_path}")
        # Create workspace configuration file
        self._create_config()
        # Create example workflow files
        self._create_example_workflows()
        self._is_initialized = True
        logger.info(f"Workspace initialized successfully: {self.workspace_path}")
        return True
    

    def _create_config(self) -> None:
        '''Method Function:
        
        Create the workspace configuration file.
        
        - Design mode:
        
            (1) Configuration file generation
        
        - Key points:
        
            (1) Defines default configuration values

            (2) Writes configuration to TOML file using TomlParser
        
        :parameters: 
            None
        
        :return: 
            None
        '''

        config_data = {
            "pangflow": {
                "version": "0.2.11",
                "workspace": str(self.workspace_path.name),
            },
            "database": {
                "path": "pangflow.db",
            },
            "prefect": {
                "work_pool": "default-process",
                "api_url": "http://127.0.0.1:4200/api",
            },
            "logging": {
                "level": "INFO",
                "file": "logs/pangflow.log",
            },
        }
        parser = TomlParser()
        parser.write_dict(config_data, self.config_path)
        logger.debug(f"Created workspace config: {self.config_path}")

    
    def _create_example_workflows(self) -> None:
        '''Method Function:
        
        Create example workflow files in the workflows directory.
        
        - Design mode:
        
            (1) Example generation

            (2) User guidance
        
        - Key points:
        
            (1) Creates example trigger workflow for training

            (2) Creates example scheduled workflow for inference

            (3) Uses TomlParser to generate templates
        
        :parameters: 
            None
        
        :return: 
            None
        '''

        parser = TomlParser()
        # Create example trigger workflow (training)
        trigger_example = self.workflows_dir / "example-training.toml"
        trigger_template = parser.generate_template(workflow_type="trigger", package="PangTS")
        with open(trigger_example, "w") as f:
            f.write(trigger_template)
        logger.debug(f"Created example trigger workflow: {trigger_example}")
        # Create example scheduled workflow (inference)
        scheduled_example = self.workflows_dir / "example-inference.toml"
        scheduled_template = parser.generate_template(workflow_type="scheduled", package="PangFT")
        with open(scheduled_example, "w") as f:
            f.write(scheduled_template)
        logger.debug(f"Created example scheduled workflow: {scheduled_example}")
    

    def read_config(self) -> dict:
        '''Method Function:
        
        Read the workspace configuration.
        
        - Design mode:
        
            (1) Configuration reading
        
        - Key points:
        
            (1) Checks if workspace is initialized before reading

            (2) Parses TOML configuration file

            (3) Returns configuration as dictionary
        
        :parameters: 
            None
        
        :return: 
            dict containing the configuration data
        '''

        if not self.is_initialized:
            raise FileNotFoundError(
                f"Workspace not initialized: {self.workspace_path}\n"
                "Run 'pangflow init' to initialize the workspace."
            )
        parser = TomlParser()
        with open(self.config_path, "rb") as f:
            import tomli
            return tomli.load(f)
    

    def find_workflow_files(self) -> List[Path]:
        '''Method Function:
        
        Find all workflow TOML files in the workspace.
        
        - Design mode:
        
            (1) File discovery

            (2) Pattern matching
        
        - Key points:
        
            (1) Searches workflows directory for .toml files

            (2) Returns empty list if workspace not initialized

            (3) Returns sorted list of file paths
        
        :parameters: 
            None
        
        :return: 
            - path (List) - List of paths to workflow TOML files
        '''

        if not self.is_initialized:
            return []
        workflow_files = []
        if self.workflows_dir.exists():
            for file_path in self.workflows_dir.glob("*.toml"):
                workflow_files.append(file_path)
        return sorted(workflow_files)
    

    def get_workflow_file(self, workflow_name: str) -> Optional[Path]:
        '''Method Function:
        
        Get the path to a specific workflow file.
        
        - Design mode:
        
            (1) File lookup

            (2) Name normalization
        
        - Key points:
        
            (1) Appends .toml extension if not present

            (2) Returns None if file not found

            (3) Returns Path if file exists
        
        :parameters:
            - workflow_name (str): Name of the workflow (with or without .toml extension)
        
        :return: 
            - path (Optional) - Path to the workflow file, or None if not found
        '''

        if not workflow_name.endswith(".toml"):
            workflow_name += ".toml"
        workflow_path = self.workflows_dir / workflow_name
        if workflow_path.exists():
            return workflow_path
        return None
    

    def create_workflow_file(
        self,
        workflow_name: str,
        workflow_type: str = "trigger",
        package: str = "PangTS",
        command: Optional[str] = None
    ) -> Path:
        '''Method Function:
        
        Create a new workflow file in the workspace.
        
        - Design mode:
        
            (1) File creation

            (2) Configuration generation
        
        - Key points:
        
            (1) Generates default command if not provided

            (2) Adds .toml extension if not present

            (3) Raises FileExistsError if file already exists

            (4) Adds schedule configuration for scheduled workflows
        
        :parameters:
            - workflow_name (str): Name for the workflow
            - workflow_type (str): Type of workflow ('trigger' or 'scheduled')
            - package (str): Algorithm package name
            - command (Optional[str]): Command to execute (optional, uses default if not provided)
        
        :return: 
            - path (Path) - Path to the created workflow file
        '''

        if not workflow_name.endswith(".toml"):
            workflow_name += ".toml"
        workflow_path = self.workflows_dir / workflow_name
        if workflow_path.exists():
            raise FileExistsError(f"Workflow file already exists: {workflow_path}")
        # Generate default command for Windows demo
        if command is None:
            if workflow_type == "trigger":
                command = f'echo "[{package}] Training workflow: {workflow_name.replace(".toml", "")}"'
            else:
                command = f'echo "[{package}] Inference workflow: {workflow_name.replace(".toml", "")}"'
        # Create the workflow file
        parser = TomlParser()
        config_data = {
            "name": workflow_name.replace(".toml", ""),
            "package": package,
            "type": workflow_type,
            "description": f"Auto-generated {workflow_type} workflow for {package}",
            "command": command,
        }
        if workflow_type == "scheduled":
            config_data["schedule"] = {"cron": "0 * * * *"}  # Hourly
        parser.write_dict(config_data, workflow_path)
        logger.info(f"Created workflow file: {workflow_path}")
        return workflow_path



def find_workspace(current_path: Optional[Path] = None) -> Optional[Path]:
    '''Function Introduction:
    
    Find a pangflow workspace by walking up the directory tree.
    
    - Design mode:
    
        (1) Directory traversal

        (2) Auto-discovery
    
    - Key points:
    
        (1) Starts from current_path or current working directory

        (2) Walks up the directory tree checking for pangflow.toml

        (3) Returns None if no workspace found
    
    :parameters:
        - current_path (Optional[Path]): Starting path (default: current working directory)
    
    :return: 
        - path (Optional[Path]) - Path to the workspace root, or None if not found
    '''
    if current_path is None:
        current_path = Path.cwd()
    current_path = current_path.resolve()
    # Walk up the directory tree
    for path in [current_path] + list(current_path.parents):
        config_file = path / WorkspaceManager.CONFIG_FILE
        if config_file.exists():
            return path
    return None



def require_workspace(path: Optional[str] = None) -> WorkspaceManager:
    '''Function Introduction:
    
    Get a workspace manager, ensuring the workspace is initialized.
    
    - Design mode:
        
        (1) Workspace retrieval with validation

        (2) Error handling
    
    - Key points:
    
        (1) Finds workspace from current directory if path not provided

        (2) Raises FileNotFoundError if no workspace found

        (3) Raises FileNotFoundError if workspace not initialized

        (4) Returns WorkspaceManager instance if valid
    
    :parameters:
        - path (Optional[str]) - Path to the workspace (default: find from current directory)
    
    :return: 
        - manager (WorkspaceManager) - WorkspaceManager instance
    '''

    if path:
        workspace_path = Path(path)
    else:
        workspace_path = find_workspace()
        if workspace_path is None:
            raise FileNotFoundError(
                "No pangflow workspace found.\n"
                "Run 'pangflow init' to create a workspace."
            )
    manager = WorkspaceManager(workspace_path)
    if not manager.is_initialized:
        raise FileNotFoundError(
            f"Workspace not initialized: {workspace_path}\n"
            "Run 'pangflow init' to initialize the workspace."
        )
    return manager



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
