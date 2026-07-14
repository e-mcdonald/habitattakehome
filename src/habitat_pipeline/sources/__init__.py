"""Source abstraction: config-driven extractors, one YAML per source.

Import ``register_builtin_extractors()`` before consulting the registry to
guarantee the built-in extractors are discoverable.
"""

from habitat_pipeline.sources.base import (
    EXTRACTORS,
    Extractor,
    ExtractorContext,
    SourceConfig,
    register,
)


def register_builtin_extractors() -> None:
    """Import side-effects register the built-in extractors."""
    # Local imports intentional: pulling these at package init would create a
    # dependency graph that's harder to reason about in tests.
    from habitat_pipeline.sources import ckan, rest  # noqa: F401


__all__ = [
    "EXTRACTORS",
    "Extractor",
    "ExtractorContext",
    "SourceConfig",
    "register",
    "register_builtin_extractors",
]
