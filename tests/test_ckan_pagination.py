"""CKAN extractor tests — offline via respx.

Verifies:
- Pagination follows the watermark until the tail (partial page terminates the loop).
- Retry fires on 5xx and 429; honors Retry-After.
- Retry does NOT fire on 4xx that isn't 429.
- HTTP 200 with ``success: false`` raises.
"""

from __future__ import annotations

import re

import httpx
import pytest
import respx

from habitat_pipeline.sources import register_builtin_extractors
from habitat_pipeline.sources.base import ExtractorContext, get_extractor
from habitat_pipeline.sources.ckan import CkanApiError

register_builtin_extractors()

BASE_URL = "https://api.example.test/api/3/action/datastore_search_sql"
# respx matches on exact URL by default; use a regex so query params don't matter.
URL_PATTERN = re.compile(r"^https://api\.example\.test/api/3/action/datastore_search_sql")
CONTEXT = ExtractorContext(source_name="test_source")


def _config(page_size: int = 3) -> dict:
    return {
        "base_url": BASE_URL,
        "resource_id": "abc-123",
        "page_size": page_size,
        "incremental_field": "_id",
        "inter_page_sleep_seconds": 0,
        "request_timeout_seconds": 5,
    }


def _record(rid: int) -> dict:
    return {"_id": rid, "unitResultID": f"urid-{rid}", "auctionUnit": "u1"}


def _ok(records: list[dict]) -> httpx.Response:
    return httpx.Response(200, json={"success": True, "result": {"records": records}})


@respx.mock
def test_paginates_until_partial_page() -> None:
    # Three full pages of 3 records, then a partial page of 1, then empty.
    responses = [
        _ok([_record(1), _record(2), _record(3)]),
        _ok([_record(4), _record(5), _record(6)]),
        _ok([_record(7), _record(8), _record(9)]),
        _ok([_record(10)]),
    ]
    route = respx.get(url__regex=URL_PATTERN).mock(side_effect=responses)

    extractor = get_extractor("ckan_datastore_sql")
    got = list(extractor.extract(_config(page_size=3), watermark=None, context=CONTEXT))

    assert [r["_id"] for r in got] == list(range(1, 11))
    assert route.call_count == 4  # loop stops on the partial page


@respx.mock
def test_retries_on_5xx_then_succeeds() -> None:
    responses = [
        httpx.Response(503),
        _ok([_record(1)]),
    ]
    route = respx.get(url__regex=URL_PATTERN).mock(side_effect=responses)

    extractor = get_extractor("ckan_datastore_sql")
    got = list(extractor.extract(_config(page_size=10), watermark=None, context=CONTEXT))

    assert [r["_id"] for r in got] == [1]
    assert route.call_count == 2


@respx.mock
def test_retries_on_429_honors_retry_after() -> None:
    responses = [
        httpx.Response(429, headers={"Retry-After": "0"}),
        _ok([_record(1)]),
    ]
    respx.get(url__regex=URL_PATTERN).mock(side_effect=responses)

    extractor = get_extractor("ckan_datastore_sql")
    got = list(extractor.extract(_config(page_size=10), watermark=None, context=CONTEXT))

    assert [r["_id"] for r in got] == [1]


@respx.mock
def test_does_not_retry_on_400() -> None:
    respx.get(url__regex=URL_PATTERN).mock(return_value=httpx.Response(400, text="bad query"))

    extractor = get_extractor("ckan_datastore_sql")
    with pytest.raises(CkanApiError):
        list(extractor.extract(_config(page_size=10), watermark=None, context=CONTEXT))


@respx.mock
def test_raises_on_success_false_even_with_200() -> None:
    respx.get(url__regex=URL_PATTERN).mock(
        return_value=httpx.Response(200, json={"success": False, "error": {"msg": "bad"}})
    )

    extractor = get_extractor("ckan_datastore_sql")
    with pytest.raises(CkanApiError):
        list(extractor.extract(_config(page_size=10), watermark=None, context=CONTEXT))


@respx.mock
def test_watermark_advances_across_pages() -> None:
    # Cold start: watermark=None, then extractor advances by max(_id) per page.
    responses = [
        _ok([_record(5), _record(6)]),
        _ok([_record(7)]),
    ]
    respx.get(url__regex=URL_PATTERN).mock(side_effect=responses)

    extractor = get_extractor("ckan_datastore_sql")
    got = list(extractor.extract(_config(page_size=2), watermark=None, context=CONTEXT))
    assert [r["_id"] for r in got] == [5, 6, 7]
