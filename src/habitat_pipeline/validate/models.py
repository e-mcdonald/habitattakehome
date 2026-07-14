"""Pydantic models — one per source dataset.

Naming: model field names are the *staging* names (snake_case, unit-suffixed).
Field aliases map from the API's camelCase source keys. This way the record
that comes out of a validated model is nearly ready to insert into staging.

Timezone: NESO's naive ISO strings are UTC. Verified empirically — the min
``deliveryStart`` of ``2026-03-31 22:00`` naive aligns with 23:00 BST, which is
the canonical EFA-block start. Under a local-time reading it would be 23:00,
not 22:00. See NOTES §8 for the full argument.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _attach_utc(value: str | datetime) -> datetime:
    """Parse a naive ISO string as UTC. Idempotent on aware datetimes."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _clean_str(value: str | None) -> str | None:
    """Trim whitespace including the ``\\t\\t`` seen in the API metadata examples."""
    if value is None:
        return None
    stripped = value.strip(" \t\r\n")
    return stripped or None


class NesoDfrResult(BaseModel):
    """One row from the NESO DFR auction-results dataset."""

    # extra="ignore" — silently tolerate new upstream fields. Their presence is
    # captured losslessly in raw.payload; caller logs the first extras seen per run.
    model_config = ConfigDict(populate_by_name=True, extra="ignore", str_strip_whitespace=False)

    id_: int = Field(alias="_id")
    unit_result_id: str = Field(alias="unitResultID")
    registered_auction_participant: str = Field(alias="registeredAuctionParticipant")
    auction_unit: str = Field(alias="auctionUnit")
    service_type: str = Field(alias="serviceType")
    auction_product: str = Field(alias="auctionProduct")
    executed_quantity_mw: Decimal = Field(alias="executedQuantity")
    clearing_price_gbp_per_mw_h: Decimal = Field(alias="clearingPrice")
    delivery_start_utc: datetime = Field(alias="deliveryStart")
    delivery_end_utc: datetime = Field(alias="deliveryEnd")
    technology_type: str | None = Field(default=None, alias="technologyType")
    post_code: str | None = Field(default=None, alias="postCode")

    @field_validator("delivery_start_utc", "delivery_end_utc", mode="before")
    @classmethod
    def _attach_utc(cls, v: str | datetime) -> datetime:
        return _attach_utc(v)

    @field_validator(
        "unit_result_id",
        "registered_auction_participant",
        "auction_unit",
        "service_type",
        "auction_product",
        mode="before",
    )
    @classmethod
    def _clean_required_str(cls, v: str) -> str:
        cleaned = _clean_str(v)
        if not cleaned:
            raise ValueError("empty after whitespace strip")
        return cleaned

    @field_validator("technology_type", "post_code", mode="before")
    @classmethod
    def _clean_optional_str(cls, v: str | None) -> str | None:
        return _clean_str(v)


class NesoSecondDatasetPlaceholder(BaseModel):
    """Placeholder for the stubbed second CKAN source.

    Intentionally not fleshed out — the point of the stub is to prove that
    adding a source is a YAML + a model, not a runner change. The CLI refuses
    to run this source until the model is implemented; that refusal is the
    demonstration.
    """

    model_config = ConfigDict(extra="forbid")
