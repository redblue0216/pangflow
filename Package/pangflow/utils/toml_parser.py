# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: PangTS
# Copyright (c) 2025 The PangTS Authors. All rights reserved.



'''
Module Introduction
-------------------

This is a TOML parser utilities module for pangflow workflow configuration.

- Design mode:

    (1) Parser pattern
    (2) Data class pattern

- Key points:

    (1) TOML file parsing and validation
    (2) Workflow configuration management
    (3) Template generation
    (4) Type hints and data validation

- Main functions:

    (1) Parse workflow TOML configuration files
    (2) Validate workflow configuration data
    (3) Generate workflow TOML templates
    (4) Convert between config objects and dictionaries

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.utils.toml_parser import TomlParser, WorkflowConfig, parse_workflow_toml



    ### test
    # Create a parser
    parser = TomlParser()
    
    # Parse a TOML file
    config = parser.parse("workflows/my-workflow.toml")
    
    # Access configuration properties
    print(config.name)
    print(config.package)
    print(config.command)
    
    # Convert to dictionary
    config_dict = config.to_dict()
    
    # Create from dictionary
    new_config = WorkflowConfig.from_dict(config_dict)
    
    # Generate a template
    template = parser.generate_template(workflow_type="trigger", package="PangTS")
    
    # Write config to file
    parser.write(new_config, "workflows/new-workflow.toml")
    
    # Convenience function
    config = parse_workflow_toml("workflows/my-workflow.toml")

    ### end of file

Description of Class and Function
-----------------
(1)WorkflowConfig: This is a data class representing a workflow configuration

(2)TomlParser: This is a parser class for workflow TOML configuration files

(3)parse_workflow_toml: Define a convenience function to parse a workflow TOML file

(4)create_workflow_toml: Define a convenience function to create a workflow TOML file

References
----------
TOML "TOML Documentation"<https://toml.io/en/>
Python "tomli library"<https://github.com/hukkin/tomli>
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
### Parser package
try:
    import tomli
    import tomli_w
    TOMLI_AVAILABLE = True
except ImportError:
    TOMLI_AVAILABLE = False
    tomli = None
    tomli_w = None



####### Logger Configuration #######################################################################
####################################################################################################



logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### dataclass: WorkflowConfig
### ------This is a data class representing a workflow configuration
###
### class: TomlParser
### ------This is a parser class for workflow TOML configuration files
###
### function: parse_workflow_toml
### ------Define a convenience function to parse a workflow TOML file
###
### function: create_workflow_toml
### ------Define a convenience function to create a workflow TOML file
###
######################################################################################################



@dataclass
class WorkflowConfig:
    '''Class Introduction:

        This is a data class representing a workflow configuration

        - Holds all configuration for a workflow as parsed from TOML

        - Provides serialization to/from dictionary

        - Supports optional fields with default values
    '''


    name: str
    package: str
    command: str
    workflow_type: str  # 'trigger' or 'scheduled'
    description: Optional[str] = None
    working_dir: Optional[str] = None
    env_vars: Optional[Dict[str, str]] = None
    timeout: Optional[int] = None
    schedule: Optional[Dict[str, Any]] = None  # For scheduled workflows
    metadata: Optional[Dict[str, Any]] = None


    def to_dict(self) -> Dict[str, Any]:
        '''Method Function:

            Define a method to convert the config to a dictionary

        :parameters:
            - nothing

        :return:
            - data_dict (dict) - Dictionary representation of the configuration
        '''

        result = {
            "name": self.name,
            "package": self.package,
            "command": self.command,
            "type": self.workflow_type,
        }
        if self.description:
            result["description"] = self.description
        if self.working_dir:
            result["working_dir"] = self.working_dir
        if self.env_vars:
            result["env"] = self.env_vars
        if self.timeout:
            result["timeout"] = self.timeout
        if self.schedule:
            result["schedule"] = self.schedule
        if self.metadata:
            result["metadata"] = self.metadata
        return result


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowConfig':
        '''Class Method Function:

            Define a class method to create a WorkflowConfig from a dictionary

        :parameters:
            - data (dict) - Dictionary containing configuration data

        :return:
            - config (WorkflowConfig) - The created WorkflowConfig instance
        '''

        return cls(
            name=data["name"],
            package=data["package"],
            command=data["command"],
            workflow_type=data["type"],
            description=data.get("description"),
            working_dir=data.get("working_dir"),
            env_vars=data.get("env"),
            timeout=data.get("timeout"),
            schedule=data.get("schedule"),
            metadata=data.get("metadata"),
        )



class TomlParser:
    '''Class Introduction:

        This is a parser class for workflow TOML configuration files

        - Parses TOML files into WorkflowConfig objects

        - Validates required fields and workflow types

        - Generates workflow TOML templates

        - Writes configurations to TOML files
    '''


    # Required fields in a workflow TOML file
    REQUIRED_FIELDS = ["name", "package", "command", "type"]
    # Valid workflow types
    VALID_TYPES = ["trigger", "scheduled"]


    def __init__(self):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

            - Checks if tomli and tomli-w are available

            - Raises RuntimeError if dependencies are missing

        :parameters:
            - nothing

        :return:
            - nothing
        '''

        if not TOMLI_AVAILABLE:
            raise RuntimeError(
                "tomli and tomli-w are not installed. "
                "Please install them with: pip install tomli tomli-w"
            )


    def parse(self, file_path: str | Path) -> WorkflowConfig:
        '''Method Function:

            Define a method to parse a TOML file and return a WorkflowConfig

            - Validates file existence

            - Parses TOML content

            - Validates required fields

        :parameters:
            - file_path (str | Path) - Path to the TOML file

        :return:
            - config (WorkflowConfig) - The parsed configuration

        :raises:
            - FileNotFoundError - If the file doesn't exist
            - ValueError - If the TOML is invalid or missing required fields
        '''

        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"TOML file not found: {file_path}")
        try:
            with open(file_path, "rb") as f:
                data = tomli.load(f)
        except Exception as e:
            raise ValueError(f"Failed to parse TOML file: {e}")
        # Validate required fields
        self._validate(data)
        logger.debug(f"Successfully parsed TOML file: {file_path}")
        return WorkflowConfig.from_dict(data)


    def parse_string(self, toml_string: str) -> WorkflowConfig:
        '''Method Function:

            Define a method to parse a TOML string and return a WorkflowConfig

        :parameters:
            - toml_string (str) - TOML content as a string

        :return:
            - config (WorkflowConfig) - The parsed configuration
        '''

        try:
            data = tomli.loads(toml_string)
        except Exception as e:
            raise ValueError(f"Failed to parse TOML string: {e}")
        self._validate(data)
        return WorkflowConfig.from_dict(data)


    def _validate(self, data: Dict[str, Any]) -> None:
        '''Method Function:

            Define a method to validate the parsed TOML data

            - Checks required fields

            - Validates workflow type

            - Warns about missing schedule configuration for scheduled workflows

        :parameters:
            - data (dict) - The parsed TOML data

        :return:
            - nothing

        :raises:
            - ValueError: If validation fails
        '''

        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        # Validate workflow type
        if data["type"] not in self.VALID_TYPES:
            raise ValueError(
                f"Invalid workflow type: {data['type']}. "
                f"Must be one of: {', '.join(self.VALID_TYPES)}"
            )
        # Validate scheduled workflows have schedule config
        if data["type"] == "scheduled" and "schedule" not in data:
            logger.warning(
                f"Scheduled workflow '{data['name']}' has no schedule configuration. "
                "Using default hourly schedule."
            )


    def write(self, config: WorkflowConfig, file_path: str | Path) -> None:
        '''Method Function:

            Define a method to write a WorkflowConfig to a TOML file

        :parameters:
            - config (WorkflowConfig) - The configuration to write
            - file_path (str | Path) - Path to the output TOML file

        :return:
            - nothing
        '''

        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        data = config.to_dict()
        with open(file_path, "wb") as f:
            tomli_w.dump(data, f)
        logger.debug(f"Wrote TOML file: {file_path}")


    def write_dict(self, data: Dict[str, Any], file_path: str | Path) -> None:
        '''Method Function:

            Define a method to write a dictionary to a TOML file

        :parameters:
            - data (dict) - The data to write
            - file_path (str | Path) - Path to the output TOML file

        :return:
            - nothing
        '''
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "wb") as f:
            tomli_w.dump(data, f)

        logger.debug(f"Wrote TOML file: {file_path}")


    def generate_template(
        self,
        workflow_type: str = "trigger",
        package: str = "PangTS"
    ) -> str:
        '''Method Function:

            Define a method to generate a TOML template for a workflow

            - Generates trigger workflow template

            - Generates scheduled workflow template with schedule configuration

        :parameters:
            - workflow_type (str) - Type of workflow ('trigger' or 'scheduled')
            - package (str) - Algorithm package name

        :return:
            - template (str) - TOML template as a string
        '''

        if workflow_type == "trigger":
            template = f'''# pangflow Workflow Configuration
# This is a trigger-based workflow template (for training)

# Workflow name (required)
name = "my-training-workflow"

# Algorithm package name (required)
# Options: PangTS, PangFT
package = "{package}"

# Workflow type (required)
# Options: trigger, scheduled
type = "trigger"

# Description (optional)
description = "My training workflow description"

# Command to execute (required)
# For Windows demo, use echo commands
command = "echo Training workflow: my-training-workflow"

# Working directory (optional)
# working_dir = "."

# Environment variables (optional)
# [env]
# MY_VAR = "value"

# Timeout in seconds (optional)
# timeout = 3600
'''
        else:  # scheduled
            template = f'''# pangflow Workflow Configuration
# This is a scheduled workflow template (for inference)

# Workflow name (required)
name = "my-inference-workflow"

# Algorithm package name (required)
# Options: PangTS, PangFT
package = "{package}"

# Workflow type (required)
# Options: trigger, scheduled
type = "scheduled"

# Description (optional)
description = "My inference workflow description"

# Command to execute (required)
# For Windows demo, use echo commands
command = "echo Inference workflow: my-inference-workflow"

# Working directory (optional)
# working_dir = "."

# Environment variables (optional)
# [env]
# MY_VAR = "value"

# Timeout in seconds (optional)
# timeout = 3600

# Schedule configuration (required for scheduled workflows)
[schedule]
# Cron schedule (either cron or interval must be specified)
cron = "0 * * * *"  # Run every hour

# Alternative: interval in seconds
# interval = 3600
'''
        return template



def parse_workflow_toml(file_path: str | Path) -> WorkflowConfig:
    '''Function Function:

        Define a convenience function to parse a workflow TOML file

    :parameters:
        - file_path (str | Path) - Path to the TOML file

    :return:
        - config (WorkflowConfig) - The parsed configuration
    '''

    parser = TomlParser()
    return parser.parse(file_path)



def create_workflow_toml(
    name: str,
    package: str,
    command: str,
    workflow_type: str,
    file_path: str | Path,
    **kwargs
) -> None:
    '''Function Function:

        Define a convenience function to create a workflow TOML file

    :parameters:
        - name (str) - Workflow name
        - package (str) - Algorithm package name
        - command (str) - Command to execute
        - workflow_type (str) - Type of workflow
        - file_path (str | Path) - Path to the output file
        - **kwargs - Additional configuration options

    :return:
        - nothing
    '''

    config = WorkflowConfig(
        name=name,
        package=package,
        command=command,
        workflow_type=workflow_type,
        **kwargs
    )
    parser = TomlParser()
    parser.write(config, file_path)



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
