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

This is a logging utilities module for pangflow.

- Design mode:

    (1) Centralized logging configuration

    (2) Flexible log handlers

- Key points:

    (1) Provides consistent logging setup across the application

    (2) Supports both console and file output

    (3) Reduces noise from external libraries

- Main functions:

    (1) setup_logging - Configure logging with custom level, format, and file output

    (2) get_logger - Get a logger instance with the specified name

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    import logging
    from pangflow.utils import setup_logging, get_logger
    
    ### Setup logging with INFO level
    setup_logging(level=logging.INFO)
    
    ### Setup logging with file output
    setup_logging(level=logging.DEBUG, log_file="app.log")
    
    ### Get a logger and use it
    logger = get_logger("my_module")
    logger.info("This is an info message")

Description of Class and Function
---------------------------------
(1)setup_logging: Function to configure logging handlers, format, and level for pangflow.

(2)get_logger: Function to retrieve a logger instance with the specified name.

References
----------
Python Logging Documentation
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
import logging
import sys
from pathlib import Path
from typing import Optional



####### Classes and Functions #######################################################################
###
### function: setup_logging
### ------Define a function to configure logging for pangflow
###
### function: get_logger
### ------Define a function to get a logger with the given name
###
######################################################################################################



def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> None:
    '''Function Introduction:
    
    Configure logging for pangflow with custom level, format, and optional file output.
    
    - Design mode:
    
        (1) Flexible configuration

        (2) Multiple handlers support
    
    - Key points:
    
        (1) Sets up StreamHandler for console output by default

        (2) Optionally adds FileHandler for file output

        (3) Applies format string to all handlers

        (4) Reduces noise from urllib3 and prefect libraries
    
    :parameters:
        - level (int) - Logging level, default is logging.INFO
        - log_file (Optional[str]) - Optional file path for logging to file
        - format_string (Optional[str]) - Optional custom format string for log messages
    
    :return: 
        None
    '''

    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=handlers,
        force=True
    )
    # Reduce noise from external libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("prefect").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    '''Function Introduction: 
    
    Get a logger with the given name.
    
    - Design mode:
    
        (1) Simple wrapper around logging.getLogger
    
    - Key points:
    
        (1) Returns a standard Python logger instance

        (2) Logger inherits configuration from setup_logging
    
    :parameters:
        - name (str) - Name for the logger, typically __name__ of the module
    
    :return: 
        - logger (object) - logging.Logger instance with the specified name
    '''

    return logging.getLogger(name)



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
