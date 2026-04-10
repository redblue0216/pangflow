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

This is the database connection module for pangflow.

This module provides database connection management using SQLAlchemy
with SQLite as the backend. It handles engine creation, session management,
and database schema initialization.

- Design mode:

    (1) Singleton pattern - Global database manager instance via get_db_manager()

    (2) Factory pattern - Session factory creation for database sessions

- Key points:

    (1) SQLite database with cross-thread support

    (2) Foreign key enforcement enabled via event listeners

    (3) Automatic table creation via SQLAlchemy metadata

- Main functions:

    (1) Database connection management via DatabaseManager class

    (2) Global database manager instance access via get_db_manager()

    (3) Database initialization and reset functionality

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### load packages
    from pangflow.database.connection import DatabaseManager, get_db_manager, initialize_database
    
    ### Method 1: Direct database manager creation
    db_manager = DatabaseManager("/path/to/database.db")
    db_manager.initialize()
    
    ### Method 2: Using global singleton
    db_manager = get_db_manager("/path/to/database.db")
    
    ### Get a session for database operations
    session = db_manager.get_session()
    
    ### Close database when done
    db_manager.close()

Description of Class and Function
-----------------
(1) Base: SQLAlchemy declarative base class for model definitions

(2) DatabaseManager: Manager class for database connections and sessions

(3) get_db_manager: Get the global database manager instance (singleton)

(4) initialize_database: Initialize the global database manager and create tables

(5) reset_database: Reset the database by dropping and recreating all tables

References
----------
SQLAlchemy Documentation: https://docs.sqlalchemy.org/
SQLite Documentation: https://www.sqlite.org/docs.html
'''



####### Load Packages ##############################################################################
####################################################################################################



### Basic package
import os
import logging
from pathlib import Path
from typing import Optional
### Third-party packages
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session, declarative_base



####### Classes and Functions #######################################################################
###
### variable: logger
### ------Module logger for logging database operations
###
### variable: Base
### ------SQLAlchemy declarative base for model definitions
###
### class: DatabaseManager
### ------Manager for database connections and sessions
###
### function: get_db_manager
### ------Get the global database manager instance (singleton pattern)
###
### function: initialize_database
### ------Initialize the global database manager and create tables
###
### function: reset_database
### ------Reset the database (delete all data and recreate tables)
###
######################################################################################################



### Initialize module logger
logger = logging.getLogger(__name__)



### SQLAlchemy declarative base for model definitions
Base = declarative_base()



class DatabaseManager:
    '''
    Class Introduction
    ------------------
    
    Manager for database connections and sessions.
    
    This class provides a centralized way to manage database connections,
    session creation, and schema management. It handles SQLite engine
    creation with proper settings for concurrent access.
    
    Key Features:
        - Lazy engine initialization
        - Session factory management
        - Database schema initialization
        - Foreign key support for SQLite
    
    Attributes:
        db_path (Path): Path to the SQLite database file
        _engine: SQLAlchemy engine instance
        _session_factory: SQLAlchemy sessionmaker factory
        _is_initialized (bool): Flag indicating if database is initialized
    '''
    

    def __init__(self, db_path: str):
        '''Attribute Function:
        
        Initialize the database manager.
        
        :parameters:
            - db_path (str) - Path to the SQLite database file
            
        :return: 
            None
        '''

        self.db_path = Path(db_path)
        self._engine = None
        self._session_factory = None
        self._is_initialized = False
    
    @property
    def engine(self):
        '''Method Function:
        
        Get the SQLAlchemy engine (create if not exists).
        
        :parameters: 
            None
            
        :return: 
            SQLAlchemy engine instance
        '''

        if self._engine is None:
            self._create_engine()
        return self._engine
    

    def _create_engine(self) -> None:
        '''Method Function:
        
        Create the SQLAlchemy engine with SQLite backend.
        
        Configures SQLite with cross-thread support and enables
        foreign key constraints via event listeners.
        
        :parameters: 
            None
            
        :return: 
            None
        '''

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Create SQLite engine with proper settings for concurrent access
        connection_string = f"sqlite:///{self.db_path.absolute()}"
        self._engine = create_engine(
            connection_string,
            echo=False,  # Set to True for SQL debugging
            connect_args={
                "check_same_thread": False,  # Allow cross-thread usage
            }
        )
        # Add foreign key support for SQLite
        @event.listens_for(self._engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            '''Enable foreign key support for SQLite.'''
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
        logger.debug(f"Created database engine for: {self.db_path}")
    

    def initialize(self) -> None:
        '''Method Function:
        
        Initialize the database by creating all tables.
        
        This method should be called once when setting up the database.
        It imports all models to register them with the Base metadata
        and creates all defined tables.
        
        :parameters: 
            None
            
        :return: 
            None
        '''

        if self._is_initialized:
            return
        # Import models to register them with Base
        from pangflow.database import models
        # Create all tables
        Base.metadata.create_all(self.engine)
        self._is_initialized = True
        logger.info(f"Initialized database at: {self.db_path}")
    

    def get_session(self) -> Session:
        '''Method Function:
        
        Get a new database session.
        
        Creates a new session factory if one doesn't exist and
        returns a new SQLAlchemy session bound to the engine.
        
        :parameters: 
            None
            
        :return: 
            A new SQLAlchemy session instance
        '''

        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory()
    

    def close(self) -> None:
        '''Method Function"
        
        Close the database engine and clean up resources.
        
        Disposes of the engine connection pool and resets
        internal state for clean shutdown.
        
        :parameters: 
            None
            
        :return: 
            None
        '''

        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.debug("Closed database engine")
    

    def reset(self) -> None:
        '''Method Function:
        
        Reset the database by dropping all tables and recreating them.
        
        WARNING: This will delete all data! Use with caution.
        
        :parameters: 
            None
            
        :return: 
            None
        '''

        from pangflow.database import models
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        logger.warning("Database has been reset - all data deleted")



# Global database manager instance (singleton pattern)
_db_manager: Optional[DatabaseManager] = None



def get_db_manager(db_path: Optional[str] = None) -> DatabaseManager:
    '''Function Introduction:
    
    Get the global database manager instance.
    
    Implements the singleton pattern to ensure only one database
    manager exists throughout the application lifecycle. Creates
    a new manager if one doesn't exist and db_path is provided.
    
    :parameters:
        - db_path (Optional[str]) - Path to the database file. If provided and no manager exists, creates a new manager with this path.
            
    :return: 
        The database manager instance
    '''

    global _db_manager
    if _db_manager is None:
        if db_path is None:
            raise RuntimeError(
                "Database manager not initialized. "
                "Please provide db_path or call initialize_database first."
            )
        _db_manager = DatabaseManager(db_path)
    return _db_manager



def initialize_database(db_path: str) -> DatabaseManager:
    '''Function Introduction:
    
    Initialize the global database manager.
    
    Creates a new DatabaseManager instance with the given path,
    initializes the database schema, and returns the manager.
    
    :parameters:
        - db_path (str) - Path to the SQLite database file
            
    :return: 
        The initialized database manager instance
    '''

    global _db_manager
    _db_manager = DatabaseManager(db_path)
    _db_manager.initialize()
    return _db_manager



def reset_database() -> None:
    '''Function Introduction:
    
    Reset the database (delete all data and recreate tables).
    
    Calls the reset method on the global database manager instance.
    
    :parameters: 
        None
            
    :return: 
        None
    '''

    global _db_manager
    if _db_manager is not None:
        _db_manager.reset()
    else:
        raise RuntimeError("Database manager not initialized")



##############################################################################################################################################################################
##############################################################################################################################################################################


### End of file
