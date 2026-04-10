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

This is a database models module for pangflow workflow management.

- Design mode:

    (1) SQLAlchemy ORM

- Key points:

    (1) Database persistence

    (2) Model relationships

    (3) Data serialization

- Main functions:

    (1) Define SQLAlchemy models for workflow metadata

    (2) Define SQLAlchemy models for execution logs

    (3) Provide model serialization methods

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.database.models import WorkflowModel, ExecutionLogModel, generate_uuid
    from datetime import datetime



    ### test
    # Create a workflow model
    workflow = WorkflowModel(
        id=generate_uuid(),
        workflow_name="my-workflow",
        package_name="PangTS",
        workflow_type="trigger",
        command="python train.py",
        status="created"
    )

    # Create an execution log model
    log = ExecutionLogModel(
        id=generate_uuid(),
        workflow_id=workflow.id,
        run_id=generate_uuid(),
        execution_type="trigger",
        status="running",
        started_at=datetime.now()
    )

    # Convert to dictionary
    workflow_dict = workflow.to_dict()
    log_dict = log.to_dict()

    ### end of file

Description of Class and Function
-----------------
(1)generate_uuid: Define a function to generate unique identifier strings

(2)WorkflowModel: This is a SQLAlchemy model class for workflow metadata storage

(3)ExecutionLogModel: This is a SQLAlchemy model class for execution logs storage

References
----------
SQLAlchemy "SQLAlchemy Documentation"<https://docs.sqlalchemy.org/>
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import uuid
from datetime import datetime
from typing import Optional
### Database package
from sqlalchemy import Column, String, DateTime, Boolean, Text, ForeignKey, Integer
from sqlalchemy.orm import relationship
### Project Package
from pangflow.database.connection import Base



####### Classes and Functions #######################################################################
###
### function: generate_uuid
### ------Define a function to generate unique identifier strings
###
### class: WorkflowModel
### ------This is a SQLAlchemy model class for workflow metadata storage
###
### class: ExecutionLogModel
### ------This is a SQLAlchemy model class for execution logs storage
###
######################################################################################################



def generate_uuid() -> str:
    '''Function Function:

        Define a function to generate unique identifier strings

        - Uses uuid4 for generating random UUIDs
        - Returns string representation of the UUID

    :parameters:
        - nothing

    :return:
        - uuid_str (str) - Unique identifier string
    '''
    return str(uuid.uuid4())



class WorkflowModel(Base):
    '''Class Introduction:

        This is a SQLAlchemy model class for workflow metadata storage

        - Stores workflow configuration, status, and deployment state
        - Manages relationships with execution logs
        - Provides serialization methods

        - Workflow metadata
        - Command configuration
        - Status tracking
        - Deployment information
        - Serve process tracking
        - Scheduling configuration
    '''

    __tablename__ = "workflows"
    # Primary key - unique workflow identifier
    id = Column(String(36), primary_key=True, default=generate_uuid)
    # Workflow metadata
    workflow_name = Column(String(255), nullable=False, index=True)
    package_name = Column(String(100), nullable=False, index=True)
    workflow_type = Column(String(20), nullable=False)  # 'trigger' or 'scheduled'
    # Command configuration
    command = Column(Text, nullable=False)
    working_dir = Column(String(500), nullable=True)
    # Status tracking
    status = Column(String(20), nullable=False, default="created")
    is_deployed = Column(Boolean, nullable=False, default=False)
    # Deployment information (for Prefect integration)
    deployment_id = Column(String(100), nullable=True)
    flow_id = Column(String(100), nullable=True)
    # Serve process tracking
    serve_pid = Column(Integer, nullable=True)  # Process ID of the serve process
    serve_status = Column(String(20), nullable=True, default=None)  # 'running', 'stopped', 'failed'
    # Scheduling configuration (for scheduled workflows)
    schedule_config = Column(Text, nullable=True)  # JSON string for schedule config
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)
    # Relationships
    execution_logs = relationship(
        "ExecutionLogModel",
        back_populates="workflow",
        cascade="all, delete-orphan",
        order_by="desc(ExecutionLogModel.started_at)"
    )


    def __repr__(self) -> str:
        '''Method Function:

            Define a string representation method for the model

        :parameters:
            - nothing

        :return:
            - repr_str (str) - String representation of the workflow model
        '''

        return (
            f"<WorkflowModel(id='{self.id}', name='{self.workflow_name}', "
            f"type='{self.workflow_type}', status='{self.status}')>"
        )


    def to_dict(self) -> dict:
        '''Method Function:

            Define a method to convert the model to a dictionary representation

            - Facilitates JSON serialization
            - Converts datetime objects to ISO format strings

        :parameters:
            - nothing

        :return:
            - data_dict (dict) - Dictionary representation of the workflow model
        '''

        return {
            "id": self.id,
            "workflow_name": self.workflow_name,
            "package_name": self.package_name,
            "workflow_type": self.workflow_type,
            "command": self.command,
            "working_dir": self.working_dir,
            "status": self.status,
            "is_deployed": self.is_deployed,
            "deployment_id": self.deployment_id,
            "flow_id": self.flow_id,
            "serve_pid": self.serve_pid,
            "serve_status": self.serve_status,
            "schedule_config": self.schedule_config,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }



class ExecutionLogModel(Base):
    '''Class Introduction:

        This is a SQLAlchemy model class for execution logs storage

        - Stores execution history of workflows
        - Tracks execution status, output, and timing
        - Manages relationships with workflow models
        - Provides duration calculation property

        - Execution metadata
        - Execution status
        - Command output
        - Execution context
        - Timestamps
        - Additional metadata
    '''

    __tablename__ = "execution_logs"
    # Primary key - unique execution identifier
    id = Column(String(36), primary_key=True, default=generate_uuid)
    # Foreign key to workflow
    workflow_id = Column(String(36), ForeignKey("workflows.id"), nullable=False, index=True)
    # Execution metadata
    run_id = Column(String(36), nullable=False, index=True)
    execution_type = Column(String(20), nullable=False)  # 'trigger' or 'scheduled'
    # Execution status
    status = Column(String(20), nullable=False)  # 'success', 'failed', 'cancelled', 'running'
    return_code = Column(Integer, nullable=True)
    # Command output
    stdout = Column(Text, nullable=True)
    stderr = Column(Text, nullable=True)
    # Execution context
    triggered_by = Column(String(50), nullable=True)  # 'user', 'scheduler', 'system'
    trigger_source = Column(String(100), nullable=True)  # For triggered workflows
    # Timestamps
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    completed_at = Column(DateTime, nullable=True)
    # Additional metadata (JSON string)
    metadata_json = Column(Text, nullable=True)
    # Relationships
    workflow = relationship("WorkflowModel", back_populates="execution_logs")


    def __repr__(self) -> str:
        '''Method Function:

            Define a string representation method for the model

        :parameters:
            - nothing

        :return:
            - repr_str (str) - String representation of the execution log model
        '''

        return (
            f"<ExecutionLogModel(id='{self.id}', workflow_id='{self.workflow_id}', "
            f"status='{self.status}')>"
        )


    def to_dict(self) -> dict:
        '''Method Function:

            Define a method to convert the model to a dictionary representation

            - Facilitates JSON serialization
            - Converts datetime objects to ISO format strings

        :parameters:
            - nothing

        :return:
            - data_dict (dict) - Dictionary representation of the execution log model
        '''

        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "execution_type": self.execution_type,
            "status": self.status,
            "return_code": self.return_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "triggered_by": self.triggered_by,
            "trigger_source": self.trigger_source,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metadata_json": self.metadata_json,
        }


    @property
    def duration_seconds(self) -> Optional[float]:
        '''Property Function:

            Define a property to calculate the execution duration in seconds

            - Returns None if execution has not completed
            - Calculates difference between started_at and completed_at

        :parameters:
            - nothing

        :return:
            - duration (float) - Execution duration in seconds, or None if not completed
        '''

        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
