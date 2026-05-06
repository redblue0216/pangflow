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

Prefect integration module for pangflow.

This module provides integration between pangflow and Prefect, enabling
workflow orchestration and deployment management through Prefect's platform.

- Design mode:

    (1) Module-level exports pattern

    (2) Factory function pattern for client instantiation

- Key points:

    (1) Simplified access to Prefect integration components

    (2) Centralized exports for client, deployment, and flow building

    (3) Lazy loading of Prefect dependencies

- Main functions:

    (1) Export PrefectClient for server interaction

    (2) Export DeploymentManager for deployment lifecycle

    (3) Export FlowBuilder for flow construction

    (4) Export get_prefect_client factory function

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.prefect_integration import PrefectClient, DeploymentManager, FlowBuilder
    from pangflow.prefect_integration import get_prefect_client

    ### Get Prefect client instance
    client = get_prefect_client()

    ### Create deployment manager
    deploy_manager = DeploymentManager(work_pool="default-process")

    ### Build a flow
    builder = FlowBuilder(workflow_id="wf-001", workflow_name="my-flow", command="echo hello")

Description of Class and Function
---------------------------------
(1)PrefectClient: Client class for interacting with Prefect server API.
    - Provides connection management and API operations
    - Handles deployment CRUD operations and health checks

(2)get_prefect_client: Factory function to get or create global PrefectClient instance.
    - Returns singleton client instance for reuse across application

(3)DeploymentManager: Manager class for creating and managing Prefect deployments.
    - Handles deployment lifecycle from creation to deletion
    - Supports scheduled deployments with cron or interval scheduling

(4)FlowBuilder: Builder class for constructing Prefect flows from workflow definitions.
    - Uses builder pattern for flexible flow configuration
    - Wraps CLI commands as Prefect tasks and flows

References
----------
Prefect Documentation: https://docs.prefect.io/
'''

####### Load Packages ##############################################################################
####################################################################################################

### Basic package
from pangflow.prefect_integration.client import PrefectClient, get_prefect_client
from pangflow.prefect_integration.deployment import DeploymentManager
from pangflow.prefect_integration.flow_builder import FlowBuilder

__all__ = [
    "PrefectClient",
    "get_prefect_client",
    "DeploymentManager",
    "FlowBuilder",
]

####### Classes and Functions #######################################################################
###
### class: PrefectClient
### ------Client class for interacting with Prefect server API
###
### function: get_prefect_client
### ---------Factory function to get or create global PrefectClient instance
###
### class: DeploymentManager
### ------Manager class for creating and managing Prefect deployments
###
### class: FlowBuilder
### ------Builder class for constructing Prefect flows from workflow definitions
###
######################################################################################################



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
