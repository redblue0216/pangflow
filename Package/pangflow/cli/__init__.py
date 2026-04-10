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

This is the CLI module for pangflow

- Design mode:

    (1) Facade pattern - Provides a simplified interface to the CLI subsystem
    (2) Module organization pattern - Exports main CLI entry point

- Key points:

    (1) Single entry point export
    (2) Clean module interface

- Main functions:

    (1) CLI module initialization
    (2) Export main CLI interface

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.cli import cli

    ### use CLI
    cli()

Description of Class and Function
-----------------
(1)cli: The main CLI entry point for pangflow workflow management

References
----------
Click Documentation - https://click.palletsprojects.com/
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
from pangflow.cli.main import cli



####### Export Definitions ##########################################################################
#####################################################################################################



__all__ = ["cli"]



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
