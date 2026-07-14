"""Validate runner.

Streams unvalidated rows out of ``raw``, coerces them through a pydantic
model, and writes typed rows into ``staging``. Failures are captured to
``ops.rejected_records`` with the pydantic error string.

Reject-threshold behaviour:
    ``fail``       — raise if any rows rejected (loud failure, default).
    ``quarantine`` — succeed but return non-zero rejected count for alerting.
    ``warn``       — succeed even with rejections; log at WARNING.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql
from pydantic import BaseModel, ValidationError

from habitat_pipeline.logging import get_logger
from habitat_pipeline.observability.metrics import RunTracker, record_rejected
from habitat_pipeline.sources.base import SourceConfig, split_schema_table
from habitat_pipeline.sources.registry import resolve_schema_model

log = get_logger(__name__)


# Fields expected on the staging model. Kept as a module constant so the
# INSERT column list stays in sync with the model shape.
_STAGING_COLUMNS = (
    "unit_result_id",
    "_id",
    "registered_auction_participant",
    "auction_unit",
    "service_type",
    "auction_product",
    "executed_quantity_mw",
    "clearing_price_gbp_per_mw_h",
    "delivery_start_utc",
    "delivery_end_utc",
    "technology_type",
    "post_code",
    "ingested_at",
    "extract_run_id",
)


@dataclass(frozen=True)
class ValidateResult:
    rows_read: int
    rows_written: int
    rows_rejected: int


class ValidateFailed(RuntimeError):
    """Raised when reject-threshold is ``fail`` and any rows were rejected."""


def _model_to_row(
    m: BaseModel,
    *,
    raw_id: int,
    raw_ingested_at: Any,
    raw_extract_run_id: Any,
) -> tuple[Any, ...]:
    """Map a validated model instance to a tuple matching ``_STAGING_COLUMNS``.

    Derived from ``_STAGING_COLUMNS`` itself (rather than a second hand-written
    tuple) so the two can't drift out of position with each other.
    """
    d = m.model_dump()
    raw_derived = {
        "_id": raw_id,
        "ingested_at": raw_ingested_at,
        "extract_run_id": raw_extract_run_id,
    }
    return tuple(d[col] if col in d else raw_derived[col] for col in _STAGING_COLUMNS)


def run_validate(
    *,
    conn: psycopg.Connection[Any],
    source: SourceConfig,
) -> ValidateResult:
    """Read from ``raw``, write to ``staging``, capture rejects."""
    raw_schema, raw_table = split_schema_table(source.raw_table)
    staging_schema, staging_table = split_schema_table(source.staging_table)

    model_cls = resolve_schema_model(source.schema_model)
    tracker = RunTracker(conn=conn, source=source.name, stage="validate")

    rejects: list[tuple[dict[str, Any], str]] = []
    rows_read = 0
    rows_written = 0
    extras_seen: set[str] = set()

    try:
        tracker.start()

        # Read raw payloads that haven't yet made it into staging.
        with (
            conn.cursor() as read_cur,
            conn.cursor() as write_cur,
        ):
            read_query = sql.SQL(
                """
                SELECT r.unit_result_id, r._id, r.payload, r.ingested_at, r.extract_run_id
                FROM {raw_schema}.{raw_table} AS r
                WHERE NOT EXISTS (
                    SELECT 1 FROM {staging_schema}.{staging_table} AS s
                    WHERE s.unit_result_id = r.unit_result_id
                )
                """
            ).format(
                raw_schema=sql.Identifier(raw_schema),
                raw_table=sql.Identifier(raw_table),
                staging_schema=sql.Identifier(staging_schema),
                staging_table=sql.Identifier(staging_table),
            )
            read_cur.execute(read_query)

            insert_stmt = sql.SQL(
                "INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders}) "
                "ON CONFLICT (unit_result_id) DO NOTHING"
            ).format(
                schema=sql.Identifier(staging_schema),
                table=sql.Identifier(staging_table),
                columns=sql.SQL(", ").join(sql.Identifier(c) for c in _STAGING_COLUMNS),
                placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in _STAGING_COLUMNS),
            )

            for row in read_cur:
                rows_read += 1
                _unit_result_id, raw_id, payload, ingested_at, extract_run_id = row

                # Log any observed extra fields once per validate run.
                if isinstance(payload, dict):
                    unknown = set(payload.keys()) - set(model_cls.model_fields) - _KNOWN_ALIASES
                    new_unknown = unknown - extras_seen
                    if new_unknown:
                        extras_seen.update(new_unknown)
                        log.info(
                            "validate.extra_keys_observed",
                            source=source.name,
                            keys=sorted(new_unknown),
                        )

                try:
                    m = model_cls.model_validate(payload)
                except ValidationError as ve:
                    rejects.append((payload, str(ve)))
                    continue

                try:
                    values = _model_to_row(
                        m,
                        raw_id=raw_id,
                        raw_ingested_at=ingested_at,
                        raw_extract_run_id=extract_run_id,
                    )
                    write_cur.execute(insert_stmt, values)
                    if write_cur.rowcount:
                        rows_written += 1
                except Exception as exc:
                    rejects.append((payload, f"staging insert failed: {exc}"))

        if rejects:
            record_rejected(conn, run_id=tracker.run_id, source=source.name, rejects=rejects)

        conn.commit()

        tracker.rows_read = rows_read
        tracker.rows_written = rows_written
        tracker.rows_rejected = len(rejects)
        tracker.finish_success()

        # Apply reject-threshold policy after we've persisted the numbers.
        if rejects and source.on_reject_threshold == "fail":
            raise ValidateFailed(f"{len(rejects)} rows rejected under on_reject_threshold=fail")
        if rejects and source.on_reject_threshold == "warn":
            log.warning(
                "validate.reject_threshold_warn",
                source=source.name,
                rejected=len(rejects),
            )

        return ValidateResult(
            rows_read=rows_read, rows_written=rows_written, rows_rejected=len(rejects)
        )
    except Exception as exc:
        conn.rollback()
        tracker.finish_failure(str(exc))
        raise


# Aliases the model accepts under ``populate_by_name=True`` — kept here so the
# "extra keys" detector doesn't flag legitimate camelCase aliases.
_KNOWN_ALIASES = {
    "_id",
    "unitResultID",
    "registeredAuctionParticipant",
    "auctionUnit",
    "serviceType",
    "auctionProduct",
    "executedQuantity",
    "clearingPrice",
    "deliveryStart",
    "deliveryEnd",
    "technologyType",
    "postCode",
}
