"""Extractor protocol, source-config model, and registry.

An ``Extractor`` is stateless: given a source config and a watermark, it yields
records. Everything else (retry, throttling, pagination bookkeeping) lives on
the extractor implementation itself so the pipeline runners stay
source-agnostic.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


class SourceConfig(BaseModel):
    """Parsed representation of a per-source YAML config."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    extractor: str
    extractor_config: dict[str, Any] = Field(default_factory=dict)
    schema_model: str  # dotted path, resolved lazily so a bad model doesn't break import
    raw_table: str
    staging_table: str
    business_key: str = "unit_result_id"
    on_reject_threshold: str = Field(default="fail", pattern="^(fail|quarantine|warn)$")


def split_schema_table(qualified: str) -> tuple[str, str]:
    """Split ``"schema.table"`` into ``(schema, table)``.

    Shared by every runner that builds SQL against ``raw_table``/``staging_table``
    from a source config, so there's one place enforcing the "must be qualified"
    invariant instead of three copies drifting apart.
    """
    schema, _, name = qualified.partition(".")
    if not schema or not name:
        raise ValueError(f"expected schema.table, got {qualified!r}")
    return schema, name


@dataclass(frozen=True)
class ExtractorContext:
    """Read-only context handed to every extractor call.

    Keeps the ``Extractor`` protocol narrow while still letting extractors
    reach the source name for structured logging.
    """

    source_name: str


@runtime_checkable
class Extractor(Protocol):
    """Pull records from a source. Sequential, yields dicts, no side effects."""

    def extract(
        self,
        config: dict[str, Any],
        watermark: int | None,
        context: ExtractorContext,
    ) -> Iterator[dict[str, Any]]:
        """Yield records with ``_id`` > ``watermark`` in ascending order.

        Args:
            config: The ``extractor_config`` block from the source YAML.
            watermark: The largest ``_id`` already ingested, or ``None`` for a cold start.
            context: Read-only context (source name for logging, etc.).

        Yields:
            One dict per record, ready to be written to raw as JSONB.
        """
        ...


EXTRACTORS: dict[str, type[Extractor]] = {}


def register(name: str) -> Callable[[type[Extractor]], type[Extractor]]:
    """Class decorator: register an extractor implementation under ``name``.

    Duplicate names raise at decoration time so registry ambiguity is caught
    at import, not at run.
    """

    def _deco(cls: type[Extractor]) -> type[Extractor]:
        if name in EXTRACTORS:
            raise RuntimeError(f"extractor already registered: {name}")
        EXTRACTORS[name] = cls
        return cls

    return _deco


def get_extractor(name: str) -> Extractor:
    """Instantiate the registered extractor for ``name``.

    Raises ``KeyError`` with a helpful message if not registered.
    """
    try:
        cls = EXTRACTORS[name]
    except KeyError as exc:
        known = ", ".join(sorted(EXTRACTORS)) or "<none>"
        raise KeyError(f"no extractor registered as {name!r}; known: {known}") from exc
    return cls()
