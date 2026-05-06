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
from pangflow.database.models import (
    WorkflowModel, ExecutionLogModel,
    ArtifactModel, ArtifactVersionModel, LineageEdgeModel,
    FeatureModel, EnvironmentModel, ServiceModel, TraceModel, NodeLogModel,
)
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
        flow_file_path: Optional[str] = None,
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
        if flow_file_path:
            model.flow_file_path = flow_file_path
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


    def get_by_run_id(self, run_id: str) -> Optional[ExecutionLogModel]:
        '''Method Function:

            Define a method to get an execution log by its run ID

        :parameters:
            - run_id (str) - The run identifier

        :return:
            - model (ExecutionLogModel) - The log model, or None if not found
        '''

        return self._session.query(ExecutionLogModel).filter_by(run_id=run_id).first()


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



class ArtifactRepository:
    '''Class Introduction:

        This is a repository class for artifact operations.
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
        node_id: str,
        artifact_type: str,
        name: str,
        storage_key: str,
        version: str = "1.0.0",
        storage_backend: str = "sqlite",
        checksum: Optional[str] = None,
        size_bytes: Optional[int] = None,
        created_by: Optional[str] = None,
        lineage_json: Optional[str] = None,
        tags_json: Optional[str] = None,
        lifecycle: str = "hot",
    ) -> ArtifactModel:
        '''Method Function:

            Define a method to create a new artifact entry.
        '''

        model = ArtifactModel(
            workflow_id=workflow_id,
            node_id=node_id,
            artifact_type=artifact_type,
            name=name,
            version=version,
            storage_backend=storage_backend,
            storage_key=storage_key,
            checksum=checksum,
            size_bytes=size_bytes,
            created_by=created_by,
            lineage_json=lineage_json,
            tags_json=tags_json,
            lifecycle=lifecycle,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created artifact: {name} (ID: {model.artifact_id})")
        return model


    def get_by_id(self, artifact_id: str) -> Optional[ArtifactModel]:
        '''Method Function:

            Define a method to get an artifact by its ID.
        '''

        return self._session.query(ArtifactModel).filter_by(artifact_id=artifact_id).first()


    def get_by_name_version(self, name: str, version: str) -> Optional[ArtifactModel]:
        '''Method Function:

            Define a method to get an artifact by name and version.
        '''

        return self._session.query(ArtifactModel).filter_by(name=name, version=version).first()


    def list_by_workflow(self, workflow_id: str) -> List[ArtifactModel]:
        '''Method Function:

            Define a method to list artifacts for a specific workflow.
        '''

        return self._session.query(ArtifactModel).filter_by(workflow_id=workflow_id).all()


    def list_by_type(self, artifact_type: str) -> List[ArtifactModel]:
        '''Method Function:

            Define a method to list artifacts by type.
        '''

        return self._session.query(ArtifactModel).filter_by(artifact_type=artifact_type).all()


    def update_tags(self, artifact_id: str, tags_json: str) -> bool:
        '''Method Function:

            Define a method to update the tags of an artifact.
        '''

        model = self.get_by_id(artifact_id)
        if not model:
            return False
        model.tags_json = tags_json
        self._session.commit()
        logger.debug(f"Updated tags for artifact: {artifact_id}")
        return True


    def delete(self, artifact_id: str) -> bool:
        '''Method Function:

            Define a method to delete an artifact from the database.
        '''

        model = self.get_by_id(artifact_id)
        if not model:
            return False
        self._session.delete(model)
        self._session.commit()
        logger.info(f"Deleted artifact: {artifact_id}")
        return True



class ArtifactVersionRepository:
    '''Class Introduction:

        This is a repository class for artifact version operations.
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
        artifact_id: str,
        version: str,
        storage_key: str,
        checksum: Optional[str] = None,
        promoted_by: Optional[str] = None,
        promotion_note: Optional[str] = None,
        stage: str = "development",
    ) -> ArtifactVersionModel:
        '''Method Function:

            Define a method to create a new artifact version entry.
        '''

        model = ArtifactVersionModel(
            artifact_id=artifact_id,
            version=version,
            storage_key=storage_key,
            checksum=checksum,
            promoted_by=promoted_by,
            promotion_note=promotion_note,
            stage=stage,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created artifact version: {version} for artifact: {artifact_id}")
        return model


    def get_by_artifact(self, artifact_id: str) -> List[ArtifactVersionModel]:
        '''Method Function:

            Define a method to list versions for a specific artifact.
        '''

        return (
            self._session.query(ArtifactVersionModel)
            .filter_by(artifact_id=artifact_id)
            .order_by(desc(ArtifactVersionModel.created_at))
            .all()
        )


    def promote(
        self,
        version_id: str,
        stage: str,
        promoted_by: Optional[str] = None,
        promotion_note: Optional[str] = None,
    ) -> bool:
        '''Method Function:

            Define a method to promote an artifact version to a new stage.
        '''

        model = self._session.query(ArtifactVersionModel).filter_by(version_id=version_id).first()
        if not model:
            return False
        model.stage = stage
        if promoted_by:
            model.promoted_by = promoted_by
        if promotion_note:
            model.promotion_note = promotion_note
        self._session.commit()
        logger.info(f"Promoted artifact version {version_id} to stage: {stage}")
        return True



class LineageRepository:
    '''Class Introduction:

        This is a repository class for lineage edge operations.
    '''


    def __init__(self, session: Session):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - session (Session) - SQLAlchemy session for database operations
        '''

        self._session = session


    def create_edge(
        self,
        from_artifact_id: str,
        to_artifact_id: str,
        edge_type: str = "data_flow",
    ) -> LineageEdgeModel:
        '''Method Function:

            Define a method to create a new lineage edge.
        '''

        model = LineageEdgeModel(
            from_artifact_id=from_artifact_id,
            to_artifact_id=to_artifact_id,
            edge_type=edge_type,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created lineage edge: {from_artifact_id} -> {to_artifact_id}")
        return model


    def get_upstream(self, artifact_id: str) -> List[ArtifactModel]:
        '''Method Function:

            Define a method to get upstream artifacts for a given artifact.
        '''

        return (
            self._session.query(ArtifactModel)
            .join(LineageEdgeModel, ArtifactModel.artifact_id == LineageEdgeModel.from_artifact_id)
            .filter(LineageEdgeModel.to_artifact_id == artifact_id)
            .all()
        )


    def get_downstream(self, artifact_id: str) -> List[ArtifactModel]:
        '''Method Function:

            Define a method to get downstream artifacts for a given artifact.
        '''

        return (
            self._session.query(ArtifactModel)
            .join(LineageEdgeModel, ArtifactModel.artifact_id == LineageEdgeModel.to_artifact_id)
            .filter(LineageEdgeModel.from_artifact_id == artifact_id)
            .all()
        )


    def get_graph(self, artifact_ids: Optional[List[str]] = None) -> List[LineageEdgeModel]:
        '''Method Function:

            Define a method to get lineage edges, optionally filtered by artifact IDs.
        '''

        query = self._session.query(LineageEdgeModel)
        if artifact_ids:
            query = query.filter(
                (LineageEdgeModel.from_artifact_id.in_(artifact_ids))
                | (LineageEdgeModel.to_artifact_id.in_(artifact_ids))
            )
        return query.all()



class FeatureRepository:
    '''Class Introduction:

        This is a repository class for feature operations.
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
        artifact_id: str,
        feature_name: str,
        schema_json: Optional[str] = None,
        partition_key: Optional[str] = None,
        ttl_expires_at: Optional[datetime] = None,
        upstream_artifacts: Optional[str] = None,
    ) -> FeatureModel:
        '''Method Function:

            Define a method to create a new feature entry.
        '''

        model = FeatureModel(
            artifact_id=artifact_id,
            feature_name=feature_name,
            schema_json=schema_json,
            partition_key=partition_key,
            ttl_expires_at=ttl_expires_at,
            upstream_artifacts=upstream_artifacts,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created feature: {feature_name} (ID: {model.feature_id})")
        return model


    def get_by_name_partition(
        self,
        feature_name: str,
        partition_key: Optional[str] = None,
    ) -> Optional[FeatureModel]:
        '''Method Function:

            Define a method to get a feature by name and optional partition key.
        '''

        query = self._session.query(FeatureModel).filter_by(feature_name=feature_name)
        if partition_key is not None:
            query = query.filter_by(partition_key=partition_key)
        return query.first()


    def list_by_workflow(self, workflow_id: str) -> List[FeatureModel]:
        '''Method Function:

            Define a method to list features for a specific workflow.
        '''

        return (
            self._session.query(FeatureModel)
            .join(ArtifactModel, FeatureModel.artifact_id == ArtifactModel.artifact_id)
            .filter(ArtifactModel.workflow_id == workflow_id)
            .all()
        )


    def invalidate(self, feature_id: str) -> bool:
        '''Method Function:

            Define a method to invalidate a feature by setting its TTL to now.
        '''

        model = self._session.query(FeatureModel).filter_by(feature_id=feature_id).first()
        if not model:
            return False
        model.ttl_expires_at = datetime.now()
        self._session.commit()
        logger.info(f"Invalidated feature: {feature_id}")
        return True



class EnvironmentRepository:
    '''Class Introduction:

        This is a repository class for environment operations.
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
        name: str,
        workflow_id: Optional[str] = None,
        python_version: Optional[str] = None,
        conda_prefix: Optional[str] = None,
        conda_spec_json: Optional[str] = None,
        pip_spec_json: Optional[str] = None,
        status: str = "NotExists",
    ) -> EnvironmentModel:
        '''Method Function:

            Define a method to create a new environment entry.
        '''

        model = EnvironmentModel(
            name=name,
            workflow_id=workflow_id,
            python_version=python_version,
            conda_prefix=conda_prefix,
            conda_spec_json=conda_spec_json,
            pip_spec_json=pip_spec_json,
            status=status,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created environment: {name} (ID: {model.env_id})")
        return model


    def get_by_workflow(self, workflow_id: str) -> List[EnvironmentModel]:
        '''Method Function:

            Define a method to list environments for a specific workflow.
        '''

        return self._session.query(EnvironmentModel).filter_by(workflow_id=workflow_id).all()


    def get_by_name(self, name: str) -> Optional[EnvironmentModel]:
        '''Method Function:

            Define a method to get an environment by its name.
        '''

        return self._session.query(EnvironmentModel).filter_by(name=name).first()


    def list_all(self) -> List[EnvironmentModel]:
        '''Method Function:

            Define a method to list all environments.
        '''

        return self._session.query(EnvironmentModel).order_by(desc(EnvironmentModel.created_at)).all()


    def update_status(self, env_id: str, status: str) -> bool:
        '''Method Function:

            Define a method to update the status of an environment.
        '''

        model = self._session.query(EnvironmentModel).filter_by(env_id=env_id).first()
        if not model:
            return False
        model.status = status
        model.updated_at = datetime.now()
        self._session.commit()
        logger.debug(f"Updated environment {env_id} status to: {status}")
        return True


    def delete(self, env_id: str) -> bool:
        '''Method Function:

            Define a method to delete an environment from the database.
        '''

        model = self._session.query(EnvironmentModel).filter_by(env_id=env_id).first()
        if not model:
            return False
        self._session.delete(model)
        self._session.commit()
        logger.info(f"Deleted environment: {env_id}")
        return True



class ServiceRepository:
    '''Class Introduction:

        This is a repository class for service operations.
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
        service_name: str,
        workflow_id: Optional[str] = None,
        status: str = "stopped",
        host: str = "127.0.0.1",
        port: int = 8000,
        pid: Optional[int] = None,
        started_at: Optional[datetime] = None,
        stopped_at: Optional[datetime] = None,
    ) -> ServiceModel:
        '''Method Function:

            Define a method to create a new service entry.
        '''

        model = ServiceModel(
            service_name=service_name,
            workflow_id=workflow_id,
            status=status,
            host=host,
            port=port,
            pid=pid,
            started_at=started_at,
            stopped_at=stopped_at,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created service: {service_name} (ID: {model.service_id})")
        return model


    def get_by_workflow(self, workflow_id: str) -> List[ServiceModel]:
        '''Method Function:

            Define a method to list services for a specific workflow.
        '''

        return self._session.query(ServiceModel).filter_by(workflow_id=workflow_id).all()


    def update_status(self, service_id: str, status: str) -> bool:
        '''Method Function:

            Define a method to update the status of a service.
        '''

        model = self._session.query(ServiceModel).filter_by(service_id=service_id).first()
        if not model:
            return False
        model.status = status
        self._session.commit()
        logger.debug(f"Updated service {service_id} status to: {status}")
        return True


    def list_all(self) -> List[ServiceModel]:
        '''Method Function:

            Define a method to list all services.
        '''

        return self._session.query(ServiceModel).order_by(desc(ServiceModel.started_at)).all()



class TraceRepository:
    '''Class Introduction:

        This is a repository class for trace operations.
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
        endpoint: str,
        method: str,
        workflow_id: Optional[str] = None,
        service_id: Optional[str] = None,
        status_code: Optional[int] = None,
        latency_ms: Optional[float] = None,
        request_json: Optional[str] = None,
        response_json: Optional[str] = None,
    ) -> TraceModel:
        '''Method Function:

            Define a method to create a new trace entry.
        '''

        model = TraceModel(
            endpoint=endpoint,
            method=method,
            workflow_id=workflow_id,
            service_id=service_id,
            status_code=status_code,
            latency_ms=latency_ms,
            request_json=request_json,
            response_json=response_json,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created trace: {model.trace_id}")
        return model


    def list_by_service(self, service_id: str, limit: int = 100) -> List[TraceModel]:
        '''Method Function:

            Define a method to list traces for a specific service.
        '''

        return (
            self._session.query(TraceModel)
            .filter_by(service_id=service_id)
            .order_by(desc(TraceModel.created_at))
            .limit(limit)
            .all()
        )


    def get_by_trace_id(self, trace_id: str) -> Optional[TraceModel]:
        '''Method Function:

            Define a method to get a trace by its trace ID.
        '''

        return self._session.query(TraceModel).filter_by(trace_id=trace_id).first()



class NodeLogRepository:
    '''Class Introduction:

        This is a repository class for node-level log operations.
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
        workflow_id: Optional[str] = None,
        workflow_name: Optional[str] = None,
        node_id: Optional[str] = None,
        node_name: Optional[str] = None,
        log_type: str = "auto",
        level: Optional[str] = None,
        message: Optional[str] = None,
        extra_json: Optional[str] = None,
        inputs_hash: Optional[str] = None,
        outputs_hash: Optional[str] = None,
        duration_ms: Optional[float] = None,
        exception: Optional[str] = None,
        metric_name: Optional[str] = None,
        metric_value: Optional[float] = None,
        trace_id: Optional[str] = None,
        storage_backend: str = "sqlite",
    ) -> NodeLogModel:
        '''Method Function:

            Define a method to create a new node log entry.
        '''

        model = NodeLogModel(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            node_id=node_id,
            node_name=node_name,
            log_type=log_type,
            level=level,
            message=message,
            extra_json=extra_json,
            inputs_hash=inputs_hash,
            outputs_hash=outputs_hash,
            duration_ms=duration_ms,
            exception=exception,
            metric_name=metric_name,
            metric_value=metric_value,
            trace_id=trace_id,
            storage_backend=storage_backend,
        )
        self._session.add(model)
        self._session.commit()
        logger.debug(f"Created node log for workflow: {workflow_id}, node: {node_id}")
        return model


    def list_by_workflow(self, workflow_id: str, limit: int = 100) -> List[NodeLogModel]:
        '''Method Function:

            Define a method to list node logs for a specific workflow.
        '''

        return (
            self._session.query(NodeLogModel)
            .filter_by(workflow_id=workflow_id)
            .order_by(desc(NodeLogModel.timestamp))
            .limit(limit)
            .all()
        )


    def list_by_node(self, node_id: str, limit: int = 100) -> List[NodeLogModel]:
        '''Method Function:

            Define a method to list node logs for a specific node.
        '''

        return (
            self._session.query(NodeLogModel)
            .filter_by(node_id=node_id)
            .order_by(desc(NodeLogModel.timestamp))
            .limit(limit)
            .all()
        )


    def query(
        self,
        level: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[NodeLogModel]:
        '''Method Function:

            Define a method to query node logs with optional filters.
        '''

        query = self._session.query(NodeLogModel)
        if level:
            query = query.filter_by(level=level)
        if since:
            query = query.filter(NodeLogModel.timestamp >= since)
        if until:
            query = query.filter(NodeLogModel.timestamp <= until)
        return query.order_by(desc(NodeLogModel.timestamp)).limit(limit).all()



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
