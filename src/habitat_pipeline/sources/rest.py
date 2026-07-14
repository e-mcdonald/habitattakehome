"""REST cursor-pagination extractor — narrated skeleton.

Purpose: prove the ``Extractor`` protocol generalises beyond CKAN. This class
is intentionally NOT implemented; the docstring below is the specification. To
enable it:

    1. Fill in ``_fetch_page`` with the target API's cursor mechanics
       (e.g. ``cursor`` query param, ``Link: rel="next"`` header, etc.).
    2. Adapt the auth: many REST sources require a bearer token; read it from
       ``config`` (which the registry passes through unchanged from YAML).
    3. Keep the retry semantics identical to ``CkanDatastoreSqlExtractor``:
       retry on 5xx / 429 / network errors only. Honor ``Retry-After``.

Shipping the skeleton — rather than nothing — makes the extensibility claim in
NOTES.md concrete: adding a non-CKAN source is a subclass + a decorator + a
YAML, with zero runner changes.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from habitat_pipeline.sources.base import ExtractorContext, register


@register("rest_api_cursor")
class RestApiCursorExtractor:
    """Cursor-paginated REST extractor. Skeleton — not yet implemented.

    Expected YAML shape (illustrative)::

        extractor: rest_api_cursor
        extractor_config:
          base_url: https://api.example.com/v1/results
          auth_env_var: EXAMPLE_API_TOKEN   # bearer token loaded from env
          cursor_param: cursor
          page_size_param: page_size
          page_size: 500
          records_json_path: data
          next_cursor_json_path: pagination.next_cursor
          incremental_field: id
    """

    def extract(
        self,
        config: dict[str, Any],
        watermark: int | None,
        context: ExtractorContext,
    ) -> Iterator[dict[str, Any]]:
        raise NotImplementedError(
            "RestApiCursorExtractor is a skeleton demonstrating extension shape. "
            "See docstring for the fields it would consume."
        )
