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

This is the state management module for pangflow workflow engine.

- Design mode:

    (1) State Pattern - Different states for workflow lifecycle

    (2) Factory Pattern - State creation through StateFactory

- Key points:

    (1) Abstract base class State defines the state interface

    (2) Concrete state classes for each workflow status

    (3) StateFactory creates appropriate state instances

    (4) WorkflowStateManager manages state transitions

    (5) WorkflowState data class for persistence

- Main functions:

    (1) Track workflow lifecycle states

    (2) Validate state transitions

    (3) Persist workflow state information

    (4) Manage deployment status

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Create a workflow state
    from pangflow.core.state import WorkflowState, WorkflowType, WorkflowStatus
    
    state = WorkflowState(
        workflow_id="wf-001",
        workflow_name="Training",
        package_name="PangTS",
        command="python train.py",
        workflow_type=WorkflowType.TRIGGER
    )
    
    ### Manage state transitions
    from pangflow.core.state import WorkflowStateManager
    
    manager = WorkflowStateManager(state)
    manager.transition_to(WorkflowStatus.REGISTERED)

Description of Class and Function
-----------------
(1) WorkflowStatus: Enumeration of possible workflow statuses

(2) WorkflowType: Enumeration of workflow types

(3) State: Abstract base class for workflow states (State Pattern)

(4) CreatedState: Workflow is created but not yet registered

(5) RegisteredState: Workflow is registered in the database

(6) DeployedState: Workflow is deployed to the Prefect platform

(7) RunningState: Workflow is currently executing

(8) CompletedState: Workflow execution completed successfully

(9) FailedState: Workflow execution failed

(10) PausedState: Workflow is paused (for scheduled workflows)

(11) StateFactory: Factory for creating state instances (Factory Pattern)

(12) WorkflowState: Data class representing a workflow's state at a point in time

(13) WorkflowStateManager: Manager class for workflow state transitions

References
----------
State Pattern `"State Pattern"<https://refactoring.guru/design-patterns/state>`_
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic packages
from enum import Enum, auto
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any
import logging
### Configure logging
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: WorkflowStatus
### ------Enumeration of possible workflow statuses
###
### class: WorkflowType
### ------Enumeration of workflow types
###
### class: State
### ------Abstract base class for workflow states (State Pattern)
###
### class: CreatedState
### ------Workflow is created but not yet registered
###
### class: RegisteredState
### ------Workflow is registered in the database
###
### class: DeployedState
### ------Workflow is deployed to the Prefect platform
###
### class: RunningState
### ------Workflow is currently executing
###
### class: CompletedState
### ------Workflow execution completed successfully
###
### class: FailedState
### ------Workflow execution failed
###
### class: PausedState
### ------Workflow is paused (for scheduled workflows)
###
### class: StateFactory
### ------Factory for creating state instances (Factory Pattern)
###
### class: WorkflowState
### ------Data class representing a workflow's state at a point in time
###
### class: WorkflowStateManager
### ------Manager class for workflow state transitions
###
######################################################################################################



class WorkflowStatus(Enum):
    '''Class Introduction:

        Enumeration of possible workflow statuses.
        
        Defines all possible states in the workflow lifecycle.
    '''


    CREATED = "created"       # Workflow is created but not yet registered
    REGISTERED = "registered" # Workflow is registered in database
    DEPLOYED = "deployed"     # Workflow is deployed to Prefect platform
    RUNNING = "running"       # Workflow is currently executing
    COMPLETED = "completed"   # Workflow execution completed successfully
    FAILED = "failed"         # Workflow execution failed
    PAUSED = "paused"         # Workflow is paused (for scheduled workflows)



class WorkflowType(Enum):
    '''Class Introduction:

        Enumeration of workflow types.
        
        Defines the available workflow types for execution.
    '''


    TRIGGER = "trigger"       # Trigger-based workflow (e.g., training)
    SCHEDULED = "scheduled"   # Scheduled workflow (e.g., inference)



class State(ABC):
    '''Class Introduction:

        Abstract base class for workflow states (State Pattern).
        
        This class defines the interface for all concrete states and provides
        common functionality for state transitions.
    '''



    @property
    @abstractmethod
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - The workflow status
        '''

        pass
    

    @abstractmethod
    def can_transition_to(self, new_state: 'State') -> bool:
        '''Method Function:

            Check if transition to new_state is allowed

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed, False otherwise
        '''

        pass
    

    def on_enter(self, context: 'WorkflowStateManager') -> None:
        '''Method Function:

            Called when entering this state

        :parameters:
            - context (WorkflowStateManager) - The state manager context

        :return:
            - None
        '''

        logger.debug(f"Entering state: {self.status.value}")
    

    def on_exit(self, context: 'WorkflowStateManager') -> None:
        '''Method Function:

            Called when exiting this state

        :parameters:
            - context (WorkflowStateManager) - The state manager context

        :return:
            - None
        '''

        logger.debug(f"Exiting state: {self.status.value}")



class CreatedState(State):
    '''Class Introduction:

        Workflow is created but not yet registered.
        
        Initial state for all workflows. Can only transition to REGISTERED.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - CREATED status
        '''

        return WorkflowStatus.CREATED
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From CREATED, can only go to REGISTERED.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [WorkflowStatus.REGISTERED]



class RegisteredState(State):
    '''Class Introduction:

        Workflow is registered in the database.
        
        Can transition to DEPLOYED or back to CREATED.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - REGISTERED status
        '''

        return WorkflowStatus.REGISTERED
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From REGISTERED, can go to DEPLOYED or back to CREATED.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [WorkflowStatus.DEPLOYED, WorkflowStatus.CREATED]


class DeployedState(State):
    '''Class Introduction:

        Workflow is deployed to the Prefect platform.
        
        Can transition to RUNNING, PAUSED, or back to REGISTERED.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - DEPLOYED status
        '''

        return WorkflowStatus.DEPLOYED
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From DEPLOYED, can go to RUNNING, PAUSED, or back to REGISTERED.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [
            WorkflowStatus.RUNNING, 
            WorkflowStatus.PAUSED,
            WorkflowStatus.REGISTERED
        ]



class RunningState(State):
    '''Class Introduction:

        Workflow is currently executing.
        
        Can transition to COMPLETED, FAILED, PAUSED, or DEPLOYED.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - RUNNING status
        '''

        return WorkflowStatus.RUNNING
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From RUNNING, can go to COMPLETED, FAILED, or PAUSED.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.PAUSED,
            WorkflowStatus.DEPLOYED
        ]



class CompletedState(State):
    '''Class Introduction:

        Workflow execution completed successfully.
        
        Can transition back to DEPLOYED for re-triggering.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - COMPLETED status
        '''

        return WorkflowStatus.COMPLETED
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From COMPLETED, can go back to DEPLOYED for re-triggering.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [WorkflowStatus.DEPLOYED, WorkflowStatus.RUNNING]



class FailedState(State):
    '''Class Introduction:

        Workflow execution failed.
        
        Can retry (go to RUNNING) or reset to DEPLOYED/REGISTERED.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - FAILED status
        '''

        return WorkflowStatus.FAILED
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From FAILED, can retry (go to RUNNING) or reset.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [
            WorkflowStatus.RUNNING,
            WorkflowStatus.DEPLOYED,
            WorkflowStatus.REGISTERED
        ]



class PausedState(State):
    '''Class Introduction:

        Workflow is paused (for scheduled workflows).
        
        Can resume to RUNNING or DEPLOYED.
    '''


    @property
    def status(self) -> WorkflowStatus:
        '''Attribute Function:

            Return the workflow status associated with this state

        :parameters:
            - None

        :return:
            - WorkflowStatus - PAUSED status
        '''

        return WorkflowStatus.PAUSED
    

    def can_transition_to(self, new_state: State) -> bool:
        '''Method Function:

            Check if transition to new_state is allowed
            
            From PAUSED, can resume to RUNNING or DEPLOYED.

        :parameters:
            - new_state (State) - The target state

        :return:
            - bool - True if transition is allowed
        '''

        return new_state.status in [WorkflowStatus.RUNNING, WorkflowStatus.DEPLOYED]



class StateFactory:
    '''Class Introduction:

        Factory for creating state instances (Factory Pattern).
        
        This factory creates appropriate State objects based on WorkflowStatus.
    '''


    _state_map: Dict[WorkflowStatus, type] = {
        WorkflowStatus.CREATED: CreatedState,
        WorkflowStatus.REGISTERED: RegisteredState,
        WorkflowStatus.DEPLOYED: DeployedState,
        WorkflowStatus.RUNNING: RunningState,
        WorkflowStatus.COMPLETED: CompletedState,
        WorkflowStatus.FAILED: FailedState,
        WorkflowStatus.PAUSED: PausedState,
    }
    

    @classmethod
    def create(cls, status: WorkflowStatus) -> State:
        '''Method Function:

            Create a State instance for the given status

        :parameters:
            - status (WorkflowStatus) - The workflow status to create a state for

        :return:
            - State - The corresponding state instance

        :raises:
            - ValueError - If the status is not recognized
        '''

        if status not in cls._state_map:
            raise ValueError(f"Unknown workflow status: {status}")
        return cls._state_map[status]()
    

    @classmethod
    def create_from_string(cls, status_str: str) -> State:
        '''Method Function:

            Create a State instance from a string representation

        :parameters:
            - status_str (str) - String representation of the status

        :return:
            - State - The corresponding state instance

        :raises:
            - ValueError - If the status string is invalid
        '''

        try:
            status = WorkflowStatus(status_str.lower())
            return cls.create(status)
        except ValueError as e:
            raise ValueError(f"Invalid status string: {status_str}") from e



class WorkflowState:
    '''Class Introduction:

        Data class representing a workflow's state at a point in time.
        
        This is used for persisting state information to the database.
    '''


    def __init__(
        self,
        workflow_id: str,
        workflow_name: str,
        package_name: str,
        command: str,
        workflow_type: WorkflowType,
        status: WorkflowStatus = WorkflowStatus.CREATED,
        is_deployed: bool = False,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - workflow_id (str) - Unique identifier for the workflow
            - workflow_name (str) - Human-readable name of the workflow
            - package_name (str) - Name of the algorithm package (e.g., PangTS, PangFT)
            - command (str) - Command line string to execute
            - workflow_type (WorkflowType) - Type of workflow (TRIGGER or SCHEDULED)
            - status (WorkflowStatus) - Current status of the workflow
            - is_deployed (bool) - Whether the workflow is deployed to Prefect
            - created_at (datetime) - Timestamp when the workflow was created
            - updated_at (datetime) - Timestamp when the workflow was last updated
            - metadata (dict) - Additional metadata as a dictionary
        '''

        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.package_name = package_name
        self.command = command
        self.workflow_type = workflow_type
        self.status = status
        self.is_deployed = is_deployed
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        self.metadata = metadata or {}
    

    def to_dict(self) -> Dict[str, Any]:
        '''Method Function:

            Convert the state to a dictionary for serialization

        :parameters:
            - None

        :return:
            - dict - Dictionary representation of the workflow state
        '''

        return {
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "package_name": self.package_name,
            "command": self.command,
            "workflow_type": self.workflow_type.value,
            "status": self.status.value,
            "is_deployed": self.is_deployed,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
        }
    

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowState':
        '''Method Function:

            Create a WorkflowState from a dictionary

        :parameters:
            - data (dict) - Dictionary containing workflow state data

        :return:
            - WorkflowState - The created WorkflowState instance
        '''

        return cls(
            workflow_id=data["workflow_id"],
            workflow_name=data["workflow_name"],
            package_name=data["package_name"],
            command=data["command"],
            workflow_type=WorkflowType(data["workflow_type"]),
            status=WorkflowStatus(data["status"]),
            is_deployed=data.get("is_deployed", False),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            metadata=data.get("metadata", {}),
        )



class WorkflowStateManager:
    '''Class Introduction:

        Manager class for workflow state transitions (State Pattern context).
        
        This class manages the current state of a workflow and handles
        state transitions according to the defined rules.
    '''


    def __init__(self, workflow_state: WorkflowState):
        '''Attribute Function:

            Define an initialization method to initialize class attributes

        :parameters:
            - _workflow_state (WorkflowState) - The workflow state to manage
            - _current_state (State) - The current state instance
            - _transition_history (list) - History of state transitions
        '''

        self._workflow_state = workflow_state
        self._current_state = StateFactory.create(workflow_state.status)
        self._transition_history: list[tuple[datetime, WorkflowStatus]] = []
        self._record_transition(workflow_state.status)
    

    @property
    def workflow_state(self) -> WorkflowState:
        '''Attribute Function:

            Get the current workflow state

        :parameters:
            - None

        :return:
            - WorkflowState - The current workflow state
        '''

        return self._workflow_state
    
    
    @property
    def current_status(self) -> WorkflowStatus:
        '''Attribute Function:

            Get the current workflow status

        :parameters:
            - None

        :return:
            - WorkflowStatus - The current workflow status
        '''

        return self._current_state.status
    

    @property
    def transition_history(self) -> list:
        '''Attribute Function:

            Get the history of state transitions

        :parameters:
            - None

        :return:
            - list - List of (timestamp, status) tuples
        '''

        return self._transition_history.copy()
    

    def _record_transition(self, new_status: WorkflowStatus) -> None:
        '''Method Function:

            Record a state transition in the history

        :parameters:
            - new_status (WorkflowStatus) - The new status

        :return:
            - None
        '''

        self._transition_history.append((datetime.now(), new_status))
    

    def transition_to(self, new_status: WorkflowStatus) -> bool:
        '''Method Function:

            Transition to a new state

        :parameters:
            - new_status (WorkflowStatus) - The target status to transition to

        :return:
            - bool - True if the transition was successful, False otherwise

        :raises:
            - RuntimeError - If the transition is not allowed
        '''

        new_state = StateFactory.create(new_status)
        if not self._current_state.can_transition_to(new_state):
            raise RuntimeError(
                f"Invalid state transition from {self._current_state.status.value} "
                f"to {new_status.value}"
            )
        # Execute exit action on current state
        self._current_state.on_exit(self)
        # Perform the transition
        old_status = self._current_state.status
        self._current_state = new_state
        self._workflow_state.status = new_status
        self._workflow_state.updated_at = datetime.now()
        # Execute enter action on new state
        self._current_state.on_enter(self)
        # Record the transition
        self._record_transition(new_status)
        logger.info(
            f"Workflow {self._workflow_state.workflow_id} transitioned: "
            f"{old_status.value} -> {new_status.value}"
        )
        return True
    

    def can_transition_to(self, status: WorkflowStatus) -> bool:
        '''Method Function:

            Check if transition to the given status is allowed

        :parameters:
            - status (WorkflowStatus) - The target status

        :return:
            - bool - True if transition is allowed
        '''

        new_state = StateFactory.create(status)
        return self._current_state.can_transition_to(new_state)
    

    def update_deployed_status(self, is_deployed: bool) -> None:
        '''Method Function:

            Update the deployment status of the workflow

        :parameters:
            - is_deployed (bool) - New deployment status

        :return:
            - None
        '''

        self._workflow_state.is_deployed = is_deployed
        self._workflow_state.updated_at = datetime.now()
        # If deployed and currently registered, transition to deployed
        if is_deployed and self._workflow_state.status == WorkflowStatus.REGISTERED:
            self.transition_to(WorkflowStatus.DEPLOYED)
        # If undeployed and currently deployed, transition back to registered
        elif not is_deployed and self._workflow_state.status == WorkflowStatus.DEPLOYED:
            self.transition_to(WorkflowStatus.REGISTERED)



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
