"""CKAN ``datastore_search_sql`` extractor.

Why ``datastore_search_sql`` and not plain ``datastore_search``:
    The standard ``datastore_search`` ``filters`` parameter is exact-match only
    (verified against the live NESO API; ``filters={"_id":{">":100}}`` returns
    HTTP 200 with ``success: false``). Range predicates require the SQL
    endpoint.

Retry policy: 5xx, network errors, and 429 (honoring ``Retry-After``). Do NOT
retry other 4xx — those are our bugs, not transient failures. Additionally,
CKAN can return HTTP 200 with ``{"success": false}`` on validation errors, so
the body is checked as well.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import httpx
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from habitat_pipeline.logging import get_logger
from habitat_pipeline.sources.base import ExtractorContext, register

log = get_logger(__name__)


class CkanApiError(RuntimeError):
    """Raised when CKAN responds with ``success: false`` or a non-retryable error."""


class CkanRetryableError(RuntimeError):
    """Raised on 5xx / 429 / network errors so tenacity retries them."""


def _should_retry_status(status: int) -> bool:
    """5xx and 429 are transient; other 4xx are permanent client errors."""
    return status >= 500 or status == 429


def _sleep_before_retry(retry_state: RetryCallState) -> None:
    """Honor a Retry-After header attached to the previous exception, if any."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, int | float) and retry_after > 0:
        time.sleep(float(retry_after))


@register("ckan_datastore_sql")
class CkanDatastoreSqlExtractor:
    """Extract records from a CKAN datastore via the SQL endpoint."""

    def extract(
        self,
        config: dict[str, Any],
        watermark: int | None,
        context: ExtractorContext,
    ) -> Iterator[dict[str, Any]]:
        base_url: str = config["base_url"]
        resource_id: str = config["resource_id"]
        page_size: int = int(config.get("page_size", 25_000))
        incremental_field: str = config.get("incremental_field", "_id")
        inter_page_sleep: float = float(config.get("inter_page_sleep_seconds", 0.2))
        timeout: float = float(config.get("request_timeout_seconds", 30.0))

        current_wm = watermark or 0
        page_num = 0

        with httpx.Client(timeout=timeout) as client:
            while True:
                page_num += 1
                records = self._fetch_page(
                    client=client,
                    base_url=base_url,
                    resource_id=resource_id,
                    incremental_field=incremental_field,
                    watermark=current_wm,
                    limit=page_size,
                    source=context.source_name,
                    page_num=page_num,
                )

                if not records:
                    log.info(
                        "ckan.extract.complete",
                        source=context.source_name,
                        pages=page_num - 1,
                        final_watermark=current_wm,
                    )
                    return

                yield from records

                # Advance watermark by the max _id we just saw.
                new_wm = max(int(r[incremental_field]) for r in records)
                if new_wm <= current_wm:
                    # Defensive: prevents an infinite loop if the source ever returns non-monotonic pages.
                    log.warning(
                        "ckan.extract.watermark_not_advancing",
                        source=context.source_name,
                        watermark=current_wm,
                        new_watermark=new_wm,
                    )
                    return
                current_wm = new_wm

                # If we got a partial page, we've reached the tail.
                if len(records) < page_size:
                    log.info(
                        "ckan.extract.complete",
                        source=context.source_name,
                        pages=page_num,
                        final_watermark=current_wm,
                    )
                    return

                time.sleep(inter_page_sleep)

    @staticmethod
    @retry(
        retry=retry_if_exception_type(CkanRetryableError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=_sleep_before_retry,
        reraise=True,
    )
    def _fetch_page(
        *,
        client: httpx.Client,
        base_url: str,
        resource_id: str,
        incremental_field: str,
        watermark: int,
        limit: int,
        source: str,
        page_num: int,
    ) -> list[dict[str, Any]]:
        # Careful SQL construction:
        #   - Resource IDs are uuids, safe to interpolate but we still quote them.
        #   - watermark and limit are integers already coerced above.
        sql = (
            f'SELECT * FROM "{resource_id}" '
            f'WHERE "{incremental_field}" > {int(watermark)} '
            f'ORDER BY "{incremental_field}" ASC LIMIT {int(limit)}'
        )

        log.debug("ckan.fetch", source=source, page=page_num, watermark=watermark)

        try:
            resp = client.get(base_url, params={"sql": sql})
        except httpx.RequestError as exc:
            # Network-level failure: retryable.
            raise CkanRetryableError(f"network error: {exc}") from exc

        if _should_retry_status(resp.status_code):
            # Named distinctly from the `except ... as exc` above: reusing `exc`
            # here reads as if it's still bound to that (implicitly deleted)
            # except-clause variable, which trips up mypy's flow analysis.
            err = CkanRetryableError(f"transient HTTP {resp.status_code}")
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                err.retry_after = float(retry_after)  # type: ignore[attr-defined]
            raise err

        if resp.status_code >= 400:
            raise CkanApiError(f"HTTP {resp.status_code}: {resp.text[:200]}")

        body = resp.json()
        # CKAN returns HTTP 200 with success:false on validation errors.
        if not body.get("success", False):
            raise CkanApiError(f"CKAN success=false: {body.get('error')!r}")

        result = body.get("result", {})
        records = result.get("records", [])
        if not isinstance(records, list):
            raise CkanApiError(f"CKAN result.records is not a list: {type(records).__name__}")
        return records
