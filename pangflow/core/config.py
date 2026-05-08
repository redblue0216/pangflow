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

Configuration layer for pangflow workflow engine.

- Design mode:

    (1) Chain of Responsibility - Multi-level config merge

    (2) Strategy Pattern - Variable expansion strategies

- Key points:

    (1) Pydantic models for structured validation

    (2) Multi-level config merge with priority

    (3) Environment variable and path expansion

    (4) Thread-local runtime parameter store

    (5) Include directive support for secrets

Usage examples
--------------
.. code-block:: python
    :linenos:

    ### Load workflow configuration
    from pangflow.core.config import ConfigLoader, WorkflowConfig

    loader = ConfigLoader()
    config = loader.load("workflow.toml")

    ### Get runtime parameter
    from pangflow.core.config import get_param

    value = get_param("learning_rate", default=0.01)

Description of Class and Function
-----------------
(1) WorkflowEnv: Environment variables configuration section

(2) WorkflowStorage: Storage configuration section

(3) WorkflowLog: Logging configuration section

(4) WorkflowServe: Serving configuration section

(5) NodeConfig: Configuration for a workflow node

(6) WorkflowConfig: Root workflow configuration model

(7) ConfigLoader: Loads and merges workflow configuration from multiple sources

(8) get_param: Resolve parameter with multi-level priority

References
----------
pangflow Documentation
'''



####### Load Packages ##############################################################################
####################################################################################################



import os
import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

### Prefer tomli as specified; fall back to stdlib tomllib on Python >= 3.11
try:
    import tomli as _toml
except ImportError:  # pragma: no cover
    import tomllib as _toml

from pydantic import BaseModel, ConfigDict, Field



####### Variable Expansion #########################################################################
####################################################################################################



### Environment variable pattern: ${VAR} or ${VAR:-default}
_ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')


def _expand_env_vars(value: Any) -> Any:
    '''Recursively expand ${ENV_VAR} and ${ENV_VAR:-default} patterns.'''
    if isinstance(value, str):
        def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
            expr = match.group(1)
            if ':-' in expr:
                var_name, default_val = expr.split(':-', 1)
                return os.environ.get(var_name, default_val)
            return os.environ.get(expr, match.group(0))
        return _ENV_VAR_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    return value


def _expand_user_path(value: Any) -> Any:
    '''Recursively expand ~ to user home directory in strings.'''
    if isinstance(value, str):
        if value.startswith('~/') or value == '~':
            return os.path.expanduser(value)
        return value
    if isinstance(value, dict):
        return {k: _expand_user_path(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_user_path(v) for v in value]
    return value


def _expand_value(value: Any) -> Any:
    '''Apply all expansions: env vars and user paths.'''
    return _expand_user_path(_expand_env_vars(value))



####### Include Directive ##########################################################################
####################################################################################################



_INCLUDE_RE = re.compile(r'^!include\s+["\']?([^"\'\s]+)["\']?\s*$', re.MULTILINE)


def _resolve_includes(raw_text: str, base_path: Path) -> str:
    '''Stub for !include directive. Replaces !include <file> with merged content.'''
    def _replace(match: re.Match) -> str:  # type: ignore[type-arg]
        include_path = base_path / match.group(1)
        if not include_path.exists():
            return ''
        included = include_path.read_text(encoding='utf-8')
        return _resolve_includes(included, include_path.parent)

    return _INCLUDE_RE.sub(_replace, raw_text)



####### Pydantic Models ############################################################################
####################################################################################################



class WorkflowEnvConda(BaseModel):
    '''Conda package specification within workflow env.'''
    channels: List[str] = Field(default_factory=lambda: ["conda-forge"])
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra='allow')


class WorkflowEnvPip(BaseModel):
    '''Pip package specification within workflow env.'''
    dependencies: List[str] = Field(default_factory=list)
    model_config = ConfigDict(extra='allow')


class WorkflowEnv(BaseModel):
    '''Environment configuration section: python version, conda/pip deps.'''
    name: Optional[str] = None
    python: str = "3.10"
    conda: WorkflowEnvConda = Field(default_factory=WorkflowEnvConda)
    pip: WorkflowEnvPip = Field(default_factory=WorkflowEnvPip)
    model_config = ConfigDict(extra='allow')


class WorkflowStorage(BaseModel):
    '''Storage configuration section.'''
    backend: str = "local"
    path: str = "./data"
    model_config = ConfigDict(extra='allow')


class WorkflowLog(BaseModel):
    '''Logging configuration section.'''
    level: str = "INFO"
    path: Optional[str] = None
    format: Optional[str] = None
    model_config = ConfigDict(extra='allow')


class WorkflowServe(BaseModel):
    '''Serving configuration section.'''
    host: str = "127.0.0.1"
    port: int = 8000
    model_config = ConfigDict(extra='allow')


class ScheduleConfig(BaseModel):
    '''Schedule configuration section.'''
    type: str = "cron"
    expression: Optional[str] = None
    model_config = ConfigDict(extra='allow')


class NodeConfig(BaseModel):
    '''Configuration for a workflow node.'''
    name: str = ""
    model_config = ConfigDict(extra='allow')


class WorkflowConfig(BaseModel):
    '''Root workflow configuration model.'''
    name: Optional[str] = None
    version: str = "0.2.12"
    description: Optional[str] = None
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    env: WorkflowEnv = Field(default_factory=WorkflowEnv)
    storage: WorkflowStorage = Field(default_factory=WorkflowStorage)
    log: WorkflowLog = Field(default_factory=WorkflowLog)
    serve: WorkflowServe = Field(default_factory=WorkflowServe)
    nodes: Dict[str, NodeConfig] = Field(default_factory=dict)
    model_config = ConfigDict(extra='allow')



####### Runtime Parameter Store ####################################################################
####################################################################################################



class _RuntimeStore:
    '''Thread-local and global runtime parameter store.'''

    def __init__(self) -> None:
        self._local = threading.local()
        self._global: Dict[str, Any] = {}

    @property
    def _local_params(self) -> Dict[str, Any]:
        if not hasattr(self._local, 'params'):
            self._local.params: Dict[str, Any] = {}
        return self._local.params

    def set_global(self, key: str, value: Any) -> None:
        '''Set a global runtime parameter.'''
        self._global[key] = value

    def set_local(self, key: str, value: Any) -> None:
        '''Set a thread-local runtime parameter.'''
        self._local_params[key] = value

    def set_cli_params(self, params: Dict[str, Any]) -> None:
        '''Set multiple CLI parameters (global, highest priority).'''
        self._global.update(params)

    def get(self, key: str, default: Any = None) -> Any:
        '''Get parameter from store. Local takes precedence over global.'''
        if key in self._local_params:
            return self._local_params[key]
        return self._global.get(key, default)

    def clear(self) -> None:
        '''Clear all runtime parameters.'''
        self._global.clear()
        self._local_params.clear()


### Singleton runtime parameter store
_RUNTIME_STORE = _RuntimeStore()


def set_runtime_param(key: str, value: Any, local: bool = False) -> None:
    '''Set a runtime parameter.

    Args:
        key: Parameter name.
        value: Parameter value.
        local: If True, store in thread-local; otherwise global.
    '''
    if local:
        _RUNTIME_STORE.set_local(key, value)
    else:
        _RUNTIME_STORE.set_global(key, value)


def set_cli_params(params: Dict[str, Any]) -> None:
    '''Set CLI parameters (highest priority for runtime store).'''
    _RUNTIME_STORE.set_cli_params(params)


def get_runtime_param(key: str, default: Any = None) -> Any:
    '''Get a runtime parameter from the store.'''
    return _RUNTIME_STORE.get(key, default)


def clear_runtime_params() -> None:
    '''Clear all runtime parameters.'''
    _RUNTIME_STORE.clear()



####### Config Loader ##############################################################################
####################################################################################################



class ConfigLoader:
    '''Loads and merges workflow configuration from multiple sources.'''

    def __init__(self, defaults: Optional[Dict[str, Any]] = None):
        self.defaults = defaults or self._build_defaults()
        self._loaded_config: Optional[WorkflowConfig] = None
        self._current_node: Optional[str] = None

    @staticmethod
    def _build_defaults() -> Dict[str, Any]:
        '''Build global default configuration.'''
        return {
            "name": None,
            "version": "0.2.12",
            "env": {},
            "storage": {"backend": "local", "path": "./data"},
            "log": {"level": "INFO", "path": None, "format": None},
            "serve": {"host": "127.0.0.1", "port": 8000},
            "nodes": {},
        }

    def set_current_node(self, node_name: Optional[str]) -> None:
        '''Set the current node context for parameter resolution.'''
        self._current_node = node_name

    def _flatten_toml(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        '''Flatten nested TOML sections into a unified dict.

        Handles [workflow] and [workflow.*] nested tables.
        '''
        result: Dict[str, Any] = {}

        for key, value in raw.items():
            if key == "workflow":
                result.update(self._flatten_workflow_section(value))
            else:
                result[key] = value

        return result

    def _flatten_workflow_section(self, section: Dict[str, Any]) -> Dict[str, Any]:
        '''Flatten the [workflow] section with nested tables.'''
        result: Dict[str, Any] = {}
        nodes: Dict[str, Any] = {}

        for key, value in section.items():
            if key == "nodes":
                nodes = value
            else:
                result[key] = value

        if nodes:
            result["nodes"] = nodes

        return result

    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        '''Deep merge override dict into base dict.'''
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def load(
        self,
        path: str,
        decorator_params: Optional[Dict[str, Any]] = None,
        cli_params: Optional[Dict[str, Any]] = None,
    ) -> WorkflowConfig:
        '''Load and merge configuration from all sources.

        Merge order (lowest to highest priority):
            1. Global defaults
            2. TOML file
            3. Decorator params
            4. CLI --param

        Args:
            path: Path to TOML configuration file.
            decorator_params: Parameters from workflow decorator.
            cli_params: Parameters from CLI --param flags.

        Returns:
            Merged WorkflowConfig instance.
        '''
        config: Dict[str, Any] = dict(self.defaults)
        file_path = Path(path)

        if file_path.exists():
            raw_text = file_path.read_text(encoding='utf-8')
            raw_text = _resolve_includes(raw_text, file_path.parent)
            parsed = _toml.loads(raw_text)
            flattened = self._flatten_toml(parsed)
            expanded = _expand_value(flattened)
            self._deep_merge(config, expanded)

        if decorator_params:
            expanded = _expand_value(dict(decorator_params))
            self._deep_merge(config, expanded)

        if cli_params:
            expanded = _expand_value(dict(cli_params))
            self._deep_merge(config, expanded)
            set_cli_params(expanded)

        self._loaded_config = WorkflowConfig.model_validate(config)
        return self._loaded_config

    def get_param(self, key: str, default: Any = None) -> Any:
        '''Get parameter with full priority resolution.

        Priority (highest to lowest):
            1. CLI --param key=value (runtime store)
            2. Environment variable PANGFLOW_KEY
            3. TOML node-level (if current node set)
            4. TOML global
            5. function default

        Args:
            key: Parameter key (supports dot notation like 'storage.path').
            default: Default value if not found.

        Returns:
            Resolved parameter value.
        '''
        # 1. CLI --param (runtime store)
        runtime_val = get_runtime_param(key)
        if runtime_val is not None:
            return runtime_val

        # 2. Environment variable PANGFLOW_KEY
        env_key = f"PANGFLOW_{key.upper().replace('.', '_')}"
        env_val = os.environ.get(env_key)
        if env_val is not None:
            return env_val

        # 3 & 4. TOML config (node-level > global)
        if self._loaded_config is not None:
            if self._current_node and self._current_node in self._loaded_config.nodes:
                node = self._loaded_config.nodes[self._current_node]
                val = self._get_nested_attr(node, key)
                if val is not None:
                    return val

            val = self._get_nested_attr(self._loaded_config, key)
            if val is not None:
                return val

        return default

    @staticmethod
    def _get_nested_attr(obj: Any, key: str) -> Any:
        '''Get nested attribute using dot notation.'''
        parts = key.split('.')
        current = obj

        for part in parts:
            if isinstance(current, BaseModel):
                current = getattr(current, part, None)
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                return None

            if current is None:
                return None

        return current



####### Module-level Convenience API ###############################################################
####################################################################################################



_DEFAULT_LOADER: Optional[ConfigLoader] = None


def get_param(key: str, default: Any = None) -> Any:
    '''Get parameter from the default config loader's resolution chain.

    Priority (highest to lowest):
        1. CLI --param key=value (runtime store)
        2. Environment variable PANGFLOW_KEY
        3. TOML node-level (if current node set on loader)
        4. TOML global
        5. function default

    Args:
        key: Parameter key (supports dot notation).
        default: Default value if not found.

    Returns:
        Resolved parameter value.
    '''
    if _DEFAULT_LOADER is not None:
        return _DEFAULT_LOADER.get_param(key, default=default)

    # Fallback without loader
    runtime_val = get_runtime_param(key)
    if runtime_val is not None:
        return runtime_val

    env_key = f"PANGFLOW_{key.upper().replace('.', '_')}"
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return env_val

    return default


def set_default_loader(loader: ConfigLoader) -> None:
    '''Set the default config loader used by module-level get_param().'''
    global _DEFAULT_LOADER
    _DEFAULT_LOADER = loader


def set_current_node(node_name: Optional[str]) -> None:
    '''Set the current node context on the default loader.'''
    if _DEFAULT_LOADER is not None:
        _DEFAULT_LOADER.set_current_node(node_name)



##############################################################################################################################################################################
##############################################################################################################################################################################



### End of file
