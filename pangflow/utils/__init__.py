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

This is the utilities module initialization file for pangflow.

- Design mode:

    (1) Module exports

    (2) Clean API interface

- Key points:

    (1) Exports core utility classes and functions

    (2) Simplifies imports for users of the utils module

- Main functions:

    (1) WorkspaceManager - Workspace management class

    (2) TomlParser - TOML configuration parser class

    (3) setup_logging - Logging configuration function

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Import utilities from the package
    from pangflow.utils import WorkspaceManager
    from pangflow.utils import TomlParser
    from pangflow.utils import setup_logging
    
    ### Use the imported utilities
    manager = WorkspaceManager("/path/to/workspace")
    parser = TomlParser()
    setup_logging(level=logging.INFO)

Description of Class and Function
---------------------------------
(1) WorkspaceManager: Class for managing pangflow workspaces including initialization,
    structure creation, and workflow file discovery.

(2) TomlParser: Class for parsing and validating workflow TOML configuration files.

(3) setup_logging: Function to configure logging for pangflow with optional file output
    and custom format strings.

References
----------
pangflow Documentation
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
from pangflow.utils.workspace import WorkspaceManager
from pangflow.utils.toml_parser import TomlParser
from pangflow.utils.logger import setup_logging



####### Public API ##################################################################################
###
### Expose the following classes and functions for public use
###
######################################################################################################



__all__ = [
    "WorkspaceManager",
    "TomlParser",
    "setup_logging",
]



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
