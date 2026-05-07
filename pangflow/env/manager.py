# -*- coding: utf-8 -*-
"""
Environment manager singleton backed by SQLite caching.
"""

import importlib.util
import json
import logging
import pathlib
import subprocess
import sys
from typing import Dict, List, Optional

from pangflow.database.connection import get_db_manager
from pangflow.database.models import EnvironmentModel
from pangflow.env.conda_env import CondaEnv
from pangflow.env.spec import CondaSpec, EnvSpec, PipSpec

logger = logging.getLogger(__name__)


class EnvManager:
    """Singleton that manages :class:`CondaEnv` instances and persists metadata."""

    _instance: Optional["EnvManager"] = None

    def __new__(cls) -> "EnvManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._cache: Dict[str, CondaEnv] = {}
        return cls._instance

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    def get_env(self, workflow_id: str) -> Optional[CondaEnv]:
        """Retrieve the environment linked to *workflow_id* (cache → DB).

        Falls back to looking up by workflow_name for backwards compatibility
        with environments created before v0.2.10.
        """
        if workflow_id in self._cache:
            return self._cache[workflow_id]

        db_manager = get_db_manager()
        with db_manager.get_session() as session:
            # Try exact workflow_id match first
            model = (
                session.query(EnvironmentModel)
                .filter_by(workflow_id=workflow_id)
                .first()
            )
            if model is None:
                # Fallback: try matching by workflow_name (old records)
                from pangflow.database.models import WorkflowModel

                wf = (
                    session.query(WorkflowModel)
                    .filter_by(workflow_name=workflow_id)
                    .first()
                )
                if wf:
                    model = (
                        session.query(EnvironmentModel)
                        .filter_by(workflow_id=wf.workflow_name)
                        .first()
                    )
                    if model is None:
                        # Try one more fallback: old envs stored UUID but query came as name
                        model = (
                            session.query(EnvironmentModel)
                            .filter_by(workflow_id=wf.id)
                            .first()
                        )

            if model is None:
                logger.debug("No environment found for workflow %s", workflow_id)
                return None

            env = self._model_to_env(model)
            self._cache[workflow_id] = env
            return env

    def create_env(
        self, env_spec: EnvSpec, workflow_id: Optional[str] = None
    ) -> Optional[CondaEnv]:
        """Create a new conda environment from *env_spec* and persist metadata."""
        db_manager = get_db_manager()
        with db_manager.get_session() as session:
            if workflow_id is not None:
                existing = (
                    session.query(EnvironmentModel)
                    .filter_by(workflow_id=workflow_id)
                    .first()
                )
                if existing is not None:
                    logger.warning(
                        "Environment already exists for workflow %s; updating instead.",
                        workflow_id,
                    )
                    return self.update_env(workflow_id, env_spec)

            env = CondaEnv(
                env_id="",
                name=env_spec.name,
                python_version=env_spec.python,
                conda_channels=list(env_spec.conda.channels),
                conda_dependencies=list(env_spec.conda.dependencies),
                pip_dependencies=list(env_spec.pip.dependencies),
            )

            if not env.create():
                logger.error("Failed to create conda environment '%s'", env_spec.name)
                return None

            # Install pangflow itself into the new environment so workflows can import it
            pangflow_spec = importlib.util.find_spec("pangflow")
            if pangflow_spec and pangflow_spec.origin:
                pangflow_pkg_root = pathlib.Path(pangflow_spec.origin).parent.parent
                # Try editable install first
                pip_install_cmd = [
                    "conda", "run", "-n", env_spec.name,
                    sys.executable, "-m", "pip", "install", "-e", str(pangflow_pkg_root)
                ]
                try:
                    subprocess.run(pip_install_cmd, check=True, capture_output=True)
                except subprocess.CalledProcessError:
                    # Fallback to regular install
                    pip_install_cmd = [
                        "conda", "run", "-n", env_spec.name,
                        sys.executable, "-m", "pip", "install", str(pangflow_pkg_root)
                    ]
                    subprocess.run(pip_install_cmd, check=False, capture_output=True)

            model = EnvironmentModel(
                workflow_id=workflow_id,
                name=env_spec.name,
                python_version=env_spec.python,
                conda_prefix=env.conda_prefix,
                conda_spec_json=json.dumps(
                    {
                        "channels": env_spec.conda.channels,
                        "dependencies": env_spec.conda.dependencies,
                    }
                ),
                pip_spec_json=json.dumps(
                    {"dependencies": env_spec.pip.dependencies}
                ),
                status="Created",
            )
            session.add(model)
            session.commit()
            env.env_id = model.env_id

            if workflow_id is not None:
                self._cache[workflow_id] = env

            logger.info(
                "Created environment %s for workflow %s", env.env_id, workflow_id,
            )
            return env

    def update_env(
        self,
        workflow_id: str,
        env_spec: Optional[EnvSpec] = None,
    ) -> Optional[CondaEnv]:
        """Update the environment linked to *workflow_id*."""
        db_manager = get_db_manager()
        with db_manager.get_session() as session:
            model = (
                session.query(EnvironmentModel)
                .filter_by(workflow_id=workflow_id)
                .first()
            )
            if model is None:
                logger.warning(
                    "No environment record for workflow %s to update", workflow_id,
                )
                return None

            if env_spec is None:
                env_spec = self._model_to_spec(model)

            env = self._model_to_env(model)
            env.name = env_spec.name
            env.python_version = env_spec.python
            env.conda_channels = list(env_spec.conda.channels)
            env.conda_dependencies = list(env_spec.conda.dependencies)
            env.pip_dependencies = list(env_spec.pip.dependencies)

            if not env.update():
                logger.error("Failed to update environment %s", env.env_id)
                return None

            model.name = env_spec.name
            model.python_version = env_spec.python
            model.conda_prefix = env.conda_prefix
            model.conda_spec_json = json.dumps(
                {
                    "channels": env_spec.conda.channels,
                    "dependencies": env_spec.conda.dependencies,
                }
            )
            model.pip_spec_json = json.dumps(
                {"dependencies": env_spec.pip.dependencies}
            )
            model.status = "Updated"
            session.commit()

            self._cache[workflow_id] = env
            logger.info(
                "Updated environment %s for workflow %s", env.env_id, workflow_id,
            )
            return env

    def remove_env(self, workflow_id: str) -> bool:
        """Remove the environment linked to *workflow_id* from disk and DB."""
        env = self.get_env(workflow_id)

        db_manager = get_db_manager()
        with db_manager.get_session() as session:
            model = (
                session.query(EnvironmentModel)
                .filter_by(workflow_id=workflow_id)
                .first()
            )
            if model is None:
                logger.warning(
                    "No environment record for workflow %s", workflow_id,
                )
                return False

            if env is not None:
                env.remove()

            session.delete(model)
            session.commit()
            self._cache.pop(workflow_id, None)
            logger.info("Removed environment for workflow %s", workflow_id)
            return True

    def list_envs(self) -> List[CondaEnv]:
        """Return all persisted environments."""
        db_manager = get_db_manager()
        with db_manager.get_session() as session:
            models = session.query(EnvironmentModel).all()
            return [self._model_to_env(m) for m in models]

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #

    @staticmethod
    def _model_to_env(model: EnvironmentModel) -> CondaEnv:
        conda_spec = (
            json.loads(model.conda_spec_json) if model.conda_spec_json else {}
        )
        pip_spec = json.loads(model.pip_spec_json) if model.pip_spec_json else {}
        return CondaEnv(
            env_id=model.env_id,
            name=model.name,
            python_version=model.python_version or "3.10",
            conda_prefix=model.conda_prefix,
            conda_channels=conda_spec.get("channels", []),
            conda_dependencies=conda_spec.get("dependencies", []),
            pip_dependencies=pip_spec.get("dependencies", []),
        )

    @staticmethod
    def _model_to_spec(model: EnvironmentModel) -> EnvSpec:
        conda_spec = (
            json.loads(model.conda_spec_json) if model.conda_spec_json else {}
        )
        pip_spec = json.loads(model.pip_spec_json) if model.pip_spec_json else {}
        return EnvSpec(
            name=model.name,
            python=model.python_version or "3.10",
            conda=CondaSpec(
                channels=conda_spec.get("channels", []),
                dependencies=conda_spec.get("dependencies", []),
            ),
            pip=PipSpec(
                dependencies=pip_spec.get("dependencies", []),
            ),
        )
