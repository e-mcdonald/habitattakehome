"""Load per-source YAML configs into ``SourceConfig`` objects."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from habitat_pipeline.sources.base import SourceConfig


class RegistryError(RuntimeError):
    """Raised when a source config can't be located or resolved."""


def load_source(sources_dir: Path, name: str) -> SourceConfig:
    """Load the ``<name>.yaml`` config from ``sources_dir``."""
    path = sources_dir / f"{name}.yaml"
    if not path.exists():
        raise RegistryError(f"source config not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict):
        raise RegistryError(f"source config {path} is not a mapping")

    return SourceConfig.model_validate(raw)


def list_sources(sources_dir: Path) -> list[str]:
    """Return the names of all discoverable YAML sources."""
    if not sources_dir.exists():
        return []
    return sorted(p.stem for p in sources_dir.glob("*.yaml"))


def resolve_schema_model(dotted_path: str) -> type[BaseModel]:
    """Resolve ``pkg.module.ClassName`` to the pydantic model class.

    Kept out of ``SourceConfig`` so import errors only bite when a source is
    actually run (not when its YAML is loaded for e.g. listing).
    """
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise RegistryError(f"schema_model must be a dotted path, got {dotted_path!r}")
    module = importlib.import_module(module_path)
    obj: Any = getattr(module, class_name, None)
    if obj is None:
        raise RegistryError(f"schema_model {dotted_path!r} not found in {module_path}")
    if not (isinstance(obj, type) and issubclass(obj, BaseModel)):
        raise RegistryError(f"schema_model {dotted_path!r} is not a pydantic BaseModel")
    return obj
