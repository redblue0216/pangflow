# -*- coding: utf-8 -*-
"""
Conda environment wrapper with subprocess-based lifecycle management.
"""

import json
import logging
import os
import shlex
import subprocess
from typing import List, Optional

logger = logging.getLogger(__name__)


class CondaEnv:
    """Represents a single Conda environment and exposes lifecycle methods."""

    def __init__(
        self,
        env_id: str,
        name: str,
        python_version: str,
        conda_prefix: Optional[str] = None,
        conda_channels: Optional[List[str]] = None,
        conda_dependencies: Optional[List[str]] = None,
        pip_dependencies: Optional[List[str]] = None,
    ):
        self.env_id = env_id
        self.name = name
        self.python_version = python_version
        self.conda_prefix = conda_prefix
        self.conda_channels = conda_channels or []
        self.conda_dependencies = conda_dependencies or []
        self.pip_dependencies = pip_dependencies or []

    def exists(self) -> bool:
        """Check whether the conda environment exists on disk."""
        try:
            result = subprocess.run(
                ["conda", "run", "-n", self.name, "python", "--version"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                self._refresh_prefix()
                return True
            return False
        except FileNotFoundError:
            logger.error("conda command not found. Is Conda installed and on PATH?")
            return False

    def create(self) -> bool:
        """Create the conda environment and install dependencies."""
        if self.exists():
            logger.info("Conda env '%s' already exists.", self.name)
            return True

        # 收集所有 conda 包规格，避免 python 版本重复
        packages = list(self.conda_dependencies)
        python_spec = f"python={self.python_version}"
        has_python = any(p.startswith("python") for p in packages)
        if not has_python:
            packages.insert(0, python_spec)

        cmd = ["conda", "create", "-n", self.name, "-y"]
        for ch in self.conda_channels:
            cmd.extend(["-c", ch])
        cmd.extend(packages)

        try:
            logger.info(
                "Creating conda env '%s' with python %s",
                self.name, self.python_version,
            )
            subprocess.run(cmd, check=True)
            self._refresh_prefix()
        except subprocess.CalledProcessError as exc:
            logger.error("Failed to create conda env '%s': %s", self.name, exc)
            return False

        if self.pip_dependencies:
            pip_cmd = [
                "conda", "run", "-n", self.name,
                "pip", "install",
            ] + self.pip_dependencies
            try:
                logger.info("Installing pip dependencies in '%s'", self.name)
                subprocess.run(pip_cmd, check=True)
            except subprocess.CalledProcessError as exc:
                logger.error(
                    "Failed to install pip dependencies in '%s': %s",
                    self.name, exc,
                )
                return False

        return True

    def update(self) -> bool:
        """Update the conda environment with current dependency lists."""
        if not self.exists():
            logger.warning(
                "Conda env '%s' does not exist; creating instead.", self.name,
            )
            return self.create()

        cmd = ["conda", "install", "-n", self.name, "-y"]
        for ch in self.conda_channels:
            cmd.extend(["-c", ch])
        for dep in self.conda_dependencies:
            cmd.append(dep)

        try:
            logger.info("Updating conda dependencies for '%s'", self.name)
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            logger.error("Failed to update conda env '%s': %s", self.name, exc)
            return False

        if self.pip_dependencies:
            pip_cmd = [
                "conda", "run", "-n", self.name,
                "pip", "install", "--upgrade",
            ] + self.pip_dependencies
            try:
                logger.info("Updating pip dependencies in '%s'", self.name)
                subprocess.run(pip_cmd, check=True)
            except subprocess.CalledProcessError as exc:
                logger.error(
                    "Failed to update pip dependencies in '%s': %s",
                    self.name, exc,
                )
                return False

        return True

    def remove(self) -> bool:
        """Remove the conda environment from disk."""
        if not self.exists():
            logger.info("Conda env '%s' does not exist.", self.name)
            return True

        try:
            logger.info("Removing conda env '%s'", self.name)
            subprocess.run(
                ["conda", "env", "remove", "-n", self.name, "-y"],
                check=True,
            )
            self.conda_prefix = None
            return True
        except subprocess.CalledProcessError as exc:
            logger.error("Failed to remove conda env '%s': %s", self.name, exc)
            return False

    def activate_command(self) -> str:
        """Return the shell command to activate this environment."""
        return f"conda activate {self.name}"

    def run_command(self, cmd: str) -> subprocess.CompletedProcess:
        """Run *cmd* inside this environment via ``conda run``."""
        run_cmd = ["conda", "run", "-n", self.name] + shlex.split(cmd)
        logger.debug("Running command in env '%s': %s", self.name, cmd)
        try:
            return subprocess.run(run_cmd, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            logger.error("conda command not found: %s", exc)
            raise

    def _refresh_prefix(self) -> None:
        """Synchronise :attr:`conda_prefix` with Conda's current state."""
        try:
            result = subprocess.run(
                ["conda", "info", "--envs", "--json"],
                capture_output=True,
                text=True,
                check=True,
            )
            data = json.loads(result.stdout)
            for env in data.get("envs", []):
                if isinstance(env, dict):
                    if env.get("name") == self.name:
                        self.conda_prefix = env.get("prefix")
                        return
                elif isinstance(env, str):
                    if os.path.basename(env) == self.name:
                        self.conda_prefix = env
                        return
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            pass
