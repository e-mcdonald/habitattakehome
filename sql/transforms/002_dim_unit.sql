-- 002_dim_unit.sql
--
-- Upserts an auction-unit dimension from staging. One row per auction_unit,
-- carrying the current technology / participant / postcode plus first/last
-- seen delivery windows. Idempotent via ON CONFLICT.
--
-- If an auction_unit's participant or technology ever changes upstream, the
-- latest observed value wins. That's an intentional trade-off appropriate to
-- a first-pass mart; SCD Type 2 would be the next step.

INSERT INTO marts.dim_unit (
    auction_unit,
    technology_type,
    post_code,
    registered_auction_participant,
    first_seen_utc,
    last_seen_utc
)
SELECT
    s.auction_unit,
    -- MAX over text picks a stable value; when only one distinct value exists
    -- (the common case) MAX and MIN agree.
    max(s.technology_type),
    max(s.post_code),
    max(s.registered_auction_participant),
    min(s.delivery_start_utc),
    max(s.delivery_start_utc)
FROM staging.neso_dfr_results AS s
GROUP BY s.auction_unit
ON CONFLICT (auction_unit) DO UPDATE
SET
    technology_type                = EXCLUDED.technology_type,
    post_code                      = EXCLUDED.post_code,
    registered_auction_participant = EXCLUDED.registered_auction_participant,
    first_seen_utc                 = LEAST(marts.dim_unit.first_seen_utc, EXCLUDED.first_seen_utc),
    last_seen_utc                  = GREATEST(marts.dim_unit.last_seen_utc, EXCLUDED.last_seen_utc);
