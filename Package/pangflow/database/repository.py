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

This is a repository module for pangflow database operations.

- Design mode:

    (1) Repository pattern

- Key points:

    (1) Abstraction of database operations

    (2) CRUD operations for workflow metadata

    (3) CRUD operations for execution logs

    (4) Data conversion between models and domain objects

- Main functions:

    (1) Workflow metadata CRUD operations

    (2) Execution log CRUD operations

    (3) Workflow state conversion

    (4) Status and deployment information updates

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.database.repository import WorkflowRepository, ExecutionLogRepository
    from pangflow.database.connection import initialize_database
    from pangflow.core.state import WorkflowState, WorkflowType, WorkflowStatus



    ### test
    # Initialize database
    db_manager = initialize_database("pangflow.db")
    
    with db_manager.get_session() as session:
        # Create workflow repository
        workflow_repo = WorkflowRepository(session)
        
        # Create a workflow
        workflow_state = WorkflowState(
            workflow_id="uuid-123",
            workflow_name="my-workflow",
            package_name="PangTS",
            command="python train.py",
            workflow_type=WorkflowType.TRIGGER,
            status=WorkflowStatus.CREATED
        )
        model = workflow_repo.create(workflow_state)
        
        # Get workflow by ID
        workflow = workflow_repo.get_by_id("uuid-123")
        
        # Update status
        workflow_repo.update_status("uuid-123", WorkflowStatus.RUNNING)
        
        # Create execution log repository
        log_repo = ExecutionLogRepository(session)
        
        # Create execution log
        log = log_repo.create(
            workflow_id="uuid-123",
            run_id="run-456",
            execution_type="trigger",
            status="running"
        )

    ### end of file

Description of Class and Function
-----------------
(1)WorkflowRepository: This is a repository class for workflow metadata operations

(2)ExecutionLogRepository: This is a repository class for execution log operations

References
----------
SQLAlchemy "SQLAlchemy Documentation"<https://docs.sqlalchemy.org/>
Martin Fowler "Repository Pattern"<https://martinfowler.com/eaaCatalog/repository.html>
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
### Database package
from sqlalchemy.orm import Session
from sqlalchemy import desc
### Project Package
from pangflow.database.models import WorkflowModel, ExecutionLogModel
from pangflow.core.state import WorkflowState, WorkflowStatus, WorkflowType



####### Logger Configuration #######################################################################
####################################################################################################



logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: WorkflowRepository
### ------This is a repository class for workflow metadata operations
###
### class: ExecutionLogRepository
### ------This is a repository class for execution log operations
###
######################################################################################################



class WorkflowRepository:
    '''Class Introduction:

        This is a repository class for workflow metadata operations

        - Provides CRUD operations for workflow metadata
        - Abstracts database operations behind a clean interface
        - Handles data conversion between models and domain objects
        - Manages workflow status and deployment information
    '''


    def __init__(self, session: Session):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - session (Session) - SQLAlchemy session for database operations
        '''

        self._session = session


    def create(self, workflow_state: WorkflowState) -> WorkflowModel:
        '''Method Function:

            Define a method to create a new workflow in the database

            - Converts WorkflowState to WorkflowModel
            - Stores metadata as JSON if present
            - Commits the transaction

        :parameters:
            - workflow_state (WorkflowState) - The workflow state to persist

        :return:
            - model (WorkflowModel) - The created database model
        '''

        model = WorkflowModel(
            id=workflow_state.workflow_id,
            workflow_name=workflow_state.workflow_name,
            package_name=workflow_state.package_name,
            workflow_type=workflow_state.workflow_type.value,
            command=workflow_state.command,
            status=workflow_state.status.value,
            is_deployed=workflow_state.is_deployed,
            created_at=workflow_state.created_at,
            updated_at=workflow_state.updated_at,
        )
        # Store metadata as JSON if present
        if workflow_state.metadata:
            model.schedule_config = json.dumps(workflow_state.metadata)
        self._session.add(model)
        self._session.commit()
        logger.info(f"Created workflow: {workflow_state.workflow_name} (ID: {model.id})")
        return model


    def get_by_id(self, workflow_id: str) -> Optional[WorkflowModel]:
        '''Method Function:

            Define a method to get a workflow by its ID

        :parameters:
            - workflow_id (str) - The workflow identifier

        :return:
            - model (WorkflowModel) - The workflow model, or None if not found
        '''

        return self._session.query(WorkflowModel).filter_by(id=workflow_id).first()


    def get_by_name(self, workflow_name: str) -> Optional[WorkflowModel]:
        '''Method Function:

            Define a method to get a workflow by its name

        :parameters:
            - workflow_name (str) - The workflow name

        :return:
            - model (WorkflowModel) - The workflow model, or None if not found
        '''

        return self._session.query(WorkflowModel).filter_by(workflow_name=workflow_name).first()


    def list_all(self, package_name: Optional[str] = None) -> List[WorkflowModel]:
        '''Method Function:

            Define a method to list all workflows, optionally filtered by package name

        :parameters:
            - package_name (str) - Optional package name filter

        :return:
            - models (List[WorkflowModel]) - List of workflow models
        '''

        query = self._session.query(WorkflowModel)
        if package_name:
            query = query.filter_by(package_name=package_name)
        return query.order_by(desc(WorkflowModel.created_at)).all()


    def update_status(
        self,
        workflow_id: str,
        status: WorkflowStatus,
        is_deployed: Optional[bool] = None
    ) -> bool:
        '''Method Function:

            Define a method to update the status of a workflow

            - Updates the status field
            - Optionally updates the deployment status
            - Updates the updated_at timestamp

        :parameters:
            - workflow_id (str) - The workflow identifier
            - status (WorkflowStatus) - The new status
            - is_deployed (bool) - Optional deployment status update

        :return:
            - success (bool) - True if the workflow was found and updated
        '''

        model = self.get_by_id(workflow_id)
        if not model:
            return False
        model.status = status.value
        model.updated_at = datetime.now()
        if is_deployed is not None:
            model.is_deployed = is_deployed
        self._session.commit()
        logger.debug(f"Updated workflow {workflow_id} status to: {status.value}")
        return True


    def update_deployment_info(
        self,
        workflow_id: str,
        deployment_id: Optional[str] = None,
        flow_id: Optional[str] = None,
        is_deployed: bool = True
    ) -> bool:
        '''Method Function:

            Define a method to update deployment information for a workflow

            - Updates deployment ID and flow ID for Prefect integration
            - Updates the deployment status
            - Updates the updated_at timestamp

        :parameters:
            - workflow_id (str) - The workflow identifier
            - deployment_id (str) - The Prefect deployment ID
            - flow_id (str) - The Prefect flow ID
            - is_deployed (bool) - Whether the workflow is deployed

        :return:
            - success (bool) - True if the workflow was found and updated
        '''

        model = self.get_by_id(workflow_id)
        if not model:
            return False
        if deployment_id:
            model.deployment_id = deployment_id
        if flow_id:
            model.flow_id = flow_id
        model.is_deployed = is_deployed
        model.updated_at = datetime.now()
        self._session.commit()
        logger.info(f"Updated deployment info for workflow: {workflow_id}")
        return True


    def update_serve_info(
        self,
        workflow_id: str,
        serve_pid: Optional[int] = None,
        serve_status: Optional[str] = None
    ) -> bool:
        '''Method Function:

            Define a method to update serve process information for a workflow

            - Updates the serve process PID
            - Updates the serve process status
            - Updates the updated_at timestamp

        :parameters:
            - workflow_id (str) - The workflow identifier
            - serve_pid (int) - The serve process PID
            - serve_status (str) - The serve process status ('running', 'stopped', 'failed')

        :return:
            - success (bool) - True if the workflow was found and updated
        '''

        model = self.get_by_id(workflow_id)
        if not model:
            return False
        if serve_pid is not None:
            model.serve_pid = serve_pid
        if serve_status:
            model.serve_status = serve_status
        model.updated_at = datetime.now()
        self._session.commit()
        logger.info(f"Updated serve info for workflow: {workflow_id} (PID: {serve_pid}, status: {serve_status})")
        return True


    def delete(self, workflow_id: str) -> bool:
        '''Method Function:

            Define a method to delete a workflow from the database

        :parameters:
            - workflow_id (str) - The workflow identifier

        :return:
            - success (bool) - True if the workflow was found and deleted
        '''

        model = self.get_by_id(workflow_id)
        if not model:
            return False
        self._session.delete(model)
        self._session.commit()
        logger.info(f"Deleted workflow: {workflow_id}")
        return True


    def to_workflow_state(self, model: WorkflowModel) -> WorkflowState:
        '''Method Function:

            Define a method to convert a database model to a WorkflowState object

            - Parses schedule_config JSON metadata
            - Creates a WorkflowState domain object

        :parameters:
            - model (WorkflowModel) - The database model

        :return:
            - workflow_state (WorkflowState) - The workflow state object
        '''

        metadata = {}
        if model.schedule_config:
            try:
                metadata = json.loads(model.schedule_config)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse schedule_config for workflow {model.id}")
        return WorkflowState(
            workflow_id=model.id,
            workflow_name=model.workflow_name,
            package_name=model.package_name,
            command=model.command,
            workflow_type=WorkflowType(model.workflow_type),
            status=WorkflowStatus(model.status),
            is_deployed=model.is_deployed,
            created_at=model.created_at,
            updated_at=model.updated_at,
            metadata=metadata,
        )



class ExecutionLogRepository:
    '''Class Introduction:

        This is a repository class for execution log operations

        - Provides CRUD operations for execution logs
        - Manages execution status tracking
        - Handles command output storage
        - Provides query methods for execution history
    '''


    def __init__(self, session: Session):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - session (Session) - SQLAlchemy session for database operations
        '''

        self._session = session


    def create(
        self,
        workflow_id: str,
        run_id: str,
        execution_type: str,
        status: str,
        triggered_by: str = "system",
        trigger_source: Optional[str] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        return_code: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ExecutionLogModel:
        '''Method Function:

            Define a method to create a new execution log entry

            - Creates an ExecutionLogModel with the provided parameters
            - Serializes metadata to JSON if provided
            - Sets the started_at timestamp

        :parameters:
            - workflow_id (str) - The workflow identifier
            - run_id (str) - The unique run identifier
            - execution_type (str) - Type of execution ('trigger' or 'scheduled')
            - status (str) - Execution status
            - triggered_by (str) - Who/what triggered the execution
            - trigger_source (str) - Source information for triggered workflows
            - stdout (str) - Standard output from execution
            - stderr (str) - Standard error from execution
            - return_code (int) - Command return code
            - metadata (dict) - Additional metadata

        :return:
            - model (ExecutionLogModel) - The created log model
        '''

        model = ExecutionLogModel(
            workflow_id=workflow_id,
            run_id=run_id,
            execution_type=execution_type,
            status=status,
            triggered_by=triggered_by,
            trigger_source=trigger_source,
            stdout=stdout,
            stderr=stderr,
            return_code=return_code,
            started_at=datetime.now(),
            metadata_json=json.dumps(metadata) if metadata else None,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created execution log for workflow: {workflow_id}")
        return model


    def complete(
        self,
        log_id: str,
        status: str,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        return_code: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        '''Method Function:

            Define a method to mark an execution log as completed

            - Updates the final status
            - Updates command output and return code
            - Merges additional metadata
            - Sets the completed_at timestamp

        :parameters:
            - log_id (str) - The log entry identifier
            - status (str) - Final execution status
            - stdout (str) - Standard output
            - stderr (str) - Standard error
            - return_code (int) - Command return code
            - metadata (dict) - Additional metadata to merge

        :return:
            - success (bool) - True if the log was found and updated
        '''

        model = self._session.query(ExecutionLogModel).filter_by(id=log_id).first()
        if not model:
            return False
        model.status = status
        model.completed_at = datetime.now()
        if stdout is not None:
            model.stdout = stdout
        if stderr is not None:
            model.stderr = stderr
        if return_code is not None:
            model.return_code = return_code
        if metadata:
            existing_metadata = {}
            if model.metadata_json:
                try:
                    existing_metadata = json.loads(model.metadata_json)
                except json.JSONDecodeError:
                    pass
            existing_metadata.update(metadata)
            model.metadata_json = json.dumps(existing_metadata)
        self._session.commit()
        logger.debug(f"Completed execution log: {log_id} with status: {status}")
        return True


    def get_by_id(self, log_id: str) -> Optional[ExecutionLogModel]:
        '''Method Function:

            Define a method to get an execution log by its ID

        :parameters:
            - log_id (str) - The log entry identifier

        :return:
            - model (ExecutionLogModel) - The log model, or None if not found
        '''

        return self._session.query(ExecutionLogModel).filter_by(id=log_id).first()


    def list_by_workflow(
        self,
        workflow_id: str,
        limit: int = 100
    ) -> List[ExecutionLogModel]:
        '''Method Function:

            Define a method to list execution logs for a specific workflow

            - Orders by started_at in descending order
            - Limits the number of results

        :parameters:
            - workflow_id (str) - The workflow identifier
            - limit (int) - Maximum number of logs to return

        :return:
            - models (List[ExecutionLogModel]) - List of execution logs
        '''

        return (
            self._session.query(ExecutionLogModel)
            .filter_by(workflow_id=workflow_id)
            .order_by(desc(ExecutionLogModel.started_at))
            .limit(limit)
            .all()
        )


    def list_all(self, limit: int = 100) -> List[ExecutionLogModel]:
        '''Method Function:

            Define a method to list all execution logs

            - Orders by started_at in descending order
            - Limits the number of results

        :parameters:
            - limit (int) - Maximum number of logs to return

        :return:
            - models (List[ExecutionLogModel]) - List of execution logs
        '''

        return (
            self._session.query(ExecutionLogModel)
            .order_by(desc(ExecutionLogModel.started_at))
            .limit(limit)
            .all()
        )


    def get_latest_by_workflow(self, workflow_id: str) -> Optional[ExecutionLogModel]:
        '''Method Function:

            Define a method to get the most recent execution log for a workflow

            - Orders by started_at in descending order
            - Returns the first result

        :parameters:
            - workflow_id (str) - The workflow identifier

        :return:
            - model (ExecutionLogModel) - The most recent log, or None if no logs exist
        '''

        return (
            self._session.query(ExecutionLogModel)
            .filter_by(workflow_id=workflow_id)
            .order_by(desc(ExecutionLogModel.started_at))
            .first()
        )


    def get_running_executions(self) -> List[ExecutionLogModel]:
        '''Method Function:

            Define a method to get all currently running executions

            - Filters by status='running'

        :parameters:
            - nothing

        :return:
            - models (List[ExecutionLogModel]) - List of running execution logs
        '''

        return (
            self._session.query(ExecutionLogModel)
            .filter_by(status="running")
            .all()
        )



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
