"""Pydantic model tests — offline, no DB required."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from habitat_pipeline.validate.models import NesoDfrResult


def _base_record() -> dict:
    return {
        "_id": 1,
        "unitResultID": "2661#||#2710#||#PSR#||#150423",
        "registeredAuctionParticipant": "GRIDBEYOND LIMITED",
        "auctionUnit": "AG-GBL06G",
        "serviceType": "Slow Reserve",
        "auctionProduct": "PSR",
        "executedQuantity": "11.0",
        "clearingPrice": "8.98",
        "deliveryStart": "2026-04-01T07:30:00",
        "deliveryEnd": "2026-04-01T08:00:00",
        "technologyType": "Load Response",
        "postCode": None,
    }


def test_happy_path_parses_and_attaches_utc() -> None:
    m = NesoDfrResult.model_validate(_base_record())
    assert m.executed_quantity_mw == Decimal("11.0")
    assert m.clearing_price_gbp_per_mw_h == Decimal("8.98")
    assert m.delivery_start_utc == datetime(2026, 4, 1, 7, 30, tzinfo=UTC)
    assert m.delivery_end_utc.tzinfo is not None
    assert m.post_code is None


def test_missing_required_field_raises() -> None:
    bad = _base_record()
    del bad["clearingPrice"]
    with pytest.raises(ValidationError) as excinfo:
        NesoDfrResult.model_validate(bad)
    assert "clearingPrice" in str(excinfo.value) or "clearing_price" in str(excinfo.value)


def test_bad_numeric_type_raises() -> None:
    bad = _base_record()
    bad["executedQuantity"] = "not a number"
    with pytest.raises(ValidationError):
        NesoDfrResult.model_validate(bad)


def test_trailing_tabs_are_stripped() -> None:
    # The API's own metadata examples show trailing \t\t on string fields.
    rec = _base_record()
    rec["auctionUnit"] = "AG-GBL06G\t\t"
    rec["technologyType"] = "Load Response\t\t"
    m = NesoDfrResult.model_validate(rec)
    assert m.auction_unit == "AG-GBL06G"
    assert m.technology_type == "Load Response"


def test_empty_required_string_raises() -> None:
    rec = _base_record()
    rec["registeredAuctionParticipant"] = "   \t  "
    with pytest.raises(ValidationError):
        NesoDfrResult.model_validate(rec)


def test_extras_are_ignored() -> None:
    # Silent tolerance for schema drift; raw.payload captures new fields losslessly.
    rec = _base_record()
    rec["unknownFutureField"] = "should not raise"
    m = NesoDfrResult.model_validate(rec)
    assert m.auction_unit == "AG-GBL06G"


def test_aware_datetime_input_is_preserved() -> None:
    rec = _base_record()
    rec["deliveryStart"] = "2026-04-01T07:30:00+00:00"
    m = NesoDfrResult.model_validate(rec)
    assert m.delivery_start_utc == datetime(2026, 4, 1, 7, 30, tzinfo=UTC)
