# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: pangflow
# Copyright (c) 2026 The pangflow Authors. All rights reserved.



'''
Module Introduction
-------------------

This is the database module for pangflow, providing a unified interface for 
database operations related to workflow management and execution logging.

- Design mode:

    (1) Facade pattern - Provides a simplified interface to the database subsystem

    (2) Repository pattern - Implemented in repository.py for data access abstraction

- Key points:

    (1) SQLAlchemy ORM for database operations

    (2) SQLite backend for local data persistence

    (3) Centralized database connection management

- Main functions:

    (1) Database connection management via DatabaseManager

    (2) Workflow data persistence via WorkflowModel and WorkflowRepository

    (3) Execution logging via ExecutionLogModel and ExecutionLogRepository

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.database import (
        DatabaseManager,
        WorkflowRepository,
        ExecutionLogRepository,
        WorkflowModel,
        ExecutionLogModel,
        get_db_manager,
    )
    
    ### Initialize database
    db_manager = get_db_manager("/path/to/database.db")
    
    ### Create a session for database operations
    session = db_manager.get_session()
    
    ### Use repositories for CRUD operations
    workflow_repo = WorkflowRepository(session)
    log_repo = ExecutionLogRepository(session)

Description of Class and Function
-----------------
(1) WorkflowModel: SQLAlchemy model for workflow metadata storage

(2) ExecutionLogModel: SQLAlchemy model for workflow execution logs

(3) WorkflowRepository: Repository class for workflow CRUD operations

(4) ExecutionLogRepository: Repository class for execution log operations

(5) DatabaseManager: Manager class for database connections and sessions

(6) get_db_manager: Function to get or create the global database manager instance

References
----------
SQLAlchemy Documentation: https://docs.sqlalchemy.org/
'''



####### Load Packages ##############################################################################
####################################################################################################



### Database models
from pangflow.database.models import (
    WorkflowModel,
    ExecutionLogModel,
    ArtifactModel,
    ArtifactVersionModel,
    LineageEdgeModel,
    FeatureModel,
    EnvironmentModel,
    ServiceModel,
    TraceModel,
    NodeLogModel,
)
### Repository classes for database operations
from pangflow.database.repository import (
    WorkflowRepository,
    ExecutionLogRepository,
    ArtifactRepository,
    ArtifactVersionRepository,
    LineageRepository,
    FeatureRepository,
    EnvironmentRepository,
    ServiceRepository,
    TraceRepository,
    NodeLogRepository,
)
### Database connection management
from pangflow.database.connection import DatabaseManager, get_db_manager



####### Classes and Functions #######################################################################
###
### class: WorkflowModel
### ------SQLAlchemy model for workflow metadata storage
###
### class: ExecutionLogModel
### ------SQLAlchemy model for workflow execution logs
###
### class: WorkflowRepository
### ------Repository class for workflow CRUD operations
###
### class: ExecutionLogRepository
### ------Repository class for execution log operations
###
### class: DatabaseManager
### ------Manager class for database connections and sessions
###
### function: get_db_manager
### ------Function to get or create the global database manager instance
###
######################################################################################################



__all__ = [
    "WorkflowModel",
    "ExecutionLogModel",
    "ArtifactModel",
    "ArtifactVersionModel",
    "LineageEdgeModel",
    "FeatureModel",
    "EnvironmentModel",
    "ServiceModel",
    "TraceModel",
    "NodeLogModel",
    "WorkflowRepository",
    "ExecutionLogRepository",
    "ArtifactRepository",
    "ArtifactVersionRepository",
    "LineageRepository",
    "FeatureRepository",
    "EnvironmentRepository",
    "ServiceRepository",
    "TraceRepository",
    "NodeLogRepository",
    "DatabaseManager",
    "get_db_manager",
]



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
