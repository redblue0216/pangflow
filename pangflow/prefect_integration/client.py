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

Prefect client module for pangflow.

This module provides a client for interacting with the Prefect server,
handling connection management and API operations for deployment lifecycle.

- Design mode:

    (1) Client pattern with async/sync bridging

    (2) Context manager pattern for resource management

    (3) Singleton pattern via global instance

- Key points:

    (1) Wraps Prefect's async client API for synchronous usage

    (2) Provides health check and deployment operations

    (3) Handles optional Prefect dependency gracefully

    (4) Uses context managers for client lifecycle

- Main functions:

    (1) Initialize and configure Prefect client

    (2) Check server health status

    (3) Get, list, create, and delete deployments

    (4) Trigger deployment runs with parameters

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.prefect_integration.client import PrefectClient, get_prefect_client

    ### Create client instance
    client = PrefectClient(api_url="http://localhost:4200")

    ### Check server health
    if client.health_check():
        print("Prefect server is healthy")

    ### List deployments
    deployments = client.list_deployments()

    ### Trigger a deployment
    flow_run_id = client.trigger_deployment(deployment_id="dep-123", parameters={"key": "value"})

    ### Use global client instance
    global_client = get_prefect_client()

Description of Class and Function
---------------------------------
(1)PrefectClient: Client class for interacting with Prefect server API.
    - Provides methods for deployment management and server health checks
    - Bridges async Prefect API to synchronous interface

(2)get_prefect_client: Factory function to get or create global PrefectClient instance.
    - Returns singleton client instance for application-wide reuse

References
----------
Prefect Client Documentation: https://docs.prefect.io/latest/api-ref/prefect/client/
Prefect API: https://docs.prefect.io/latest/api-ref/
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
### Prefect imports - these may not be available if Prefect is not installed
try:
    from prefect import get_client
    from prefect.client.schemas.filters import DeploymentFilter, FlowFilter
    from prefect.client.schemas.sorting import DeploymentSort
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False
    get_client = None
logger = logging.getLogger(__name__)



####### Classes and Functions #######################################################################
###
### class: PrefectClient
### ------Client class for interacting with Prefect server API
###
### function: get_prefect_client
### ---------Factory function to get or create global PrefectClient instance
###
######################################################################################################



class PrefectClient:
    '''
    Class Introduction
    ------------------
    
    Client for interacting with Prefect server.
    
    This class wraps Prefect's client API to provide a simplified interface
    for pangflow's workflow operations. It handles connection management,
    deployment operations, and server health checks.
    
    The client supports both synchronous and asynchronous operations by
    using an internal async client bridged through context managers.
    
    Attributes
    ----------
    api_url : Optional[str]
        URL of the Prefect server API
    api_key : Optional[str]
        API key for authentication (if required)
    _client : Optional[Any]
        Internal Prefect client instance
    '''
    

    def __init__(self, api_url: Optional[str] = None, api_key: Optional[str] = None):
        '''Attribute Function:
        
        Initialize the Prefect client.
        
        Creates a new PrefectClient instance with the specified API URL
        and authentication key. Validates that Prefect is installed before
        initialization.
        
        :parameters:
            - api_url (Optional[str]) - URL of the Prefect server API
            - api_key (Optional[str]) - API key for authentication (if required)
        
        :return: 
            None
        '''

        if not PREFECT_AVAILABLE:
            raise RuntimeError(
                "Prefect is not installed. "
                "Please install it with: pip install prefect==3.6.21"
            )
        self.api_url = api_url
        self.api_key = api_key
        self._client = None
    

    async def _get_client(self):
        '''Method Function:
        
        Get or create the Prefect client instance.
        
        Lazily initializes the internal Prefect async client on first call
        and returns the cached instance on subsequent calls.
        
        :parameters: 
            None
        
        :return: 
            The Prefect client instance
        '''

        if self._client is None:
            self._client = get_client()
        return self._client
    

    @contextmanager
    def _sync_client(self):
        '''Method Function:
        
        Context manager for synchronous client operations.
        
        Provides a synchronous interface to the async Prefect client
        by running async operations in an event loop.
        
        :parameters: 
            None
        
        :yield: 
            The Prefect client instance
        '''

        import asyncio
        client = asyncio.run(self._get_client())
        try:
            yield client
        finally:
            # Client is managed by Prefect's context
            pass
    

    def health_check(self) -> bool:
        '''Method Function:
        
        Check if the Prefect server is reachable.
        
        Attempts to read deployments from the server as a health check.
        Logs the result and returns a boolean status.
        
        :parameters: 
            None
        
        :return: 
            True if the server is healthy and reachable, False otherwise
        '''

        try:
            with self._sync_client() as client:
                # Try to read deployments as a health check
                asyncio = __import__('asyncio')
                deployments = asyncio.run(client.read_deployments(limit=1))
                logger.debug("Prefect server health check: OK")
                return True
        except Exception as e:
            logger.warning(f"Prefect server health check failed: {e}")
            return False
    

    def get_deployment(self, deployment_id: str) -> Optional[Dict[str, Any]]:
        '''Method Function:
        
        Get a deployment by its ID.
        
        Retrieves deployment information from the Prefect server
        using the specified deployment ID.
        
        :parameters:
            - deployment_id (str) - The Prefect deployment ID
        
        :return: 
            Deployment information dictionary, or None if not found
        '''

        try:
            with self._sync_client() as client:
                import asyncio
                deployment = asyncio.run(client.read_deployment(deployment_id))
                return {
                    "id": str(deployment.id),
                    "name": deployment.name,
                    "flow_id": str(deployment.flow_id),
                    "is_schedule_active": deployment.is_schedule_active,
                }
        except Exception as e:
            logger.error(f"Failed to get deployment {deployment_id}: {e}")
            return None
    

    def list_deployments(self, flow_name: Optional[str] = None) -> list:
        '''Method Function:
        
        List deployments, optionally filtered by flow name.
        
        Retrieves a list of all deployments from the Prefect server,
        with optional filtering by flow name.
        
        :parameters:
            - flow_name (Optional[str]) - Optional flow name filter
        
        :return: 
            List of deployment information dictionaries
        '''
        try:
            with self._sync_client() as client:
                import asyncio
                # Build filter if flow name provided
                deployment_filter = None
                if flow_name:
                    deployment_filter = DeploymentFilter(name={"like_": flow_name})
                deployments = asyncio.run(
                    client.read_deployments(deployment_filter=deployment_filter)
                )
                return [
                    {
                        "id": str(d.id),
                        "name": d.name,
                        "flow_id": str(d.flow_id),
                        "is_schedule_active": d.is_schedule_active,
                    }
                    for d in deployments
                ]
        except Exception as e:
            logger.error(f"Failed to list deployments: {e}")
            return []
    

    def create_deployment(
        self,
        flow_name: str,
        deployment_name: str,
        schedule: Optional[Any] = None
    ) -> Optional[str]:
        '''Method Function:
        
        Create a deployment for a flow.
        
        Creates a new deployment for the specified flow. Note that
        direct deployment creation via API is not fully implemented;
        use DeploymentManager for complete deployment lifecycle.
        
        :parameters:
            - flow_name (str) - Name of the flow
            - deployment_name (str) - Name for the deployment
            - schedule (Optional[Any]) - Optional schedule configuration
        
        :return: 
            The deployment ID if created, or None if creation failed
        '''

        try:
            # Deployment creation is handled by DeploymentManager
            # This is a placeholder for direct API calls if needed
            logger.warning("Direct deployment creation not implemented - use DeploymentManager")
            return None
        except Exception as e:
            logger.error(f"Failed to create deployment: {e}")
            return None
    

    def delete_deployment(self, deployment_id: str) -> bool:
        '''Method Function:
        
        Delete a deployment.
        
        Deletes the specified deployment from the Prefect server.
        
        :parameters:
            - deployment_id (str) - The deployment ID to delete
        
        :return: 
            True if deletion was successful, False otherwise
        '''

        try:
            with self._sync_client() as client:
                import asyncio
                asyncio.run(client.delete_deployment(deployment_id))
                logger.info(f"Deleted deployment: {deployment_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to delete deployment {deployment_id}: {e}")
            return False
    

    def trigger_deployment(
        self,
        deployment_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        '''Method Function:
        
        Trigger a deployment run.
        
        Creates a new flow run from the specified deployment with
        optional parameters.
        
        :parameters:
            - deployment_id (str) - The deployment ID to trigger
            - parameters (Optional[Dict[str, Any]]) - Optional parameters for the flow run
        
        :return: 
            The flow run ID if triggered successfully, or None if failed
        '''

        try:
            with self._sync_client() as client:
                import asyncio
                flow_run = asyncio.run(
                    client.create_flow_run_from_deployment(
                        deployment_id=deployment_id,
                        parameters=parameters or {}
                    )
                )
                logger.info(f"Triggered deployment {deployment_id}, flow run: {flow_run.id}")
                return str(flow_run.id)
        except Exception as e:
            logger.error(f"Failed to trigger deployment {deployment_id}: {e}")
            return None



# Global Prefect client instance
_prefect_client: Optional[PrefectClient] = None



def get_prefect_client(
    api_url: Optional[str] = None,
    api_key: Optional[str] = None
) -> PrefectClient:
    '''Function Introduction:
    
    Get the global Prefect client instance.
    
    Returns the singleton PrefectClient instance, creating it if necessary.
    This function ensures that the same client instance is reused across
    the application for efficient connection management.
    
    :parameters:
        - api_url (Optional[str]) - Optional API URL for the Prefect server
        - api_key (Optional[str]) - Optional API key for authentication
    
    :return: 
        The global PrefectClient instance
    '''

    global _prefect_client
    if _prefect_client is None:
        _prefect_client = PrefectClient(api_url=api_url, api_key=api_key)
    return _prefect_client



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
