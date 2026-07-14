-- 003_fact_auction_result.sql
--
-- Materializes the fact table from staging. Grain: one row per
-- (auction_unit, auction_product, delivery_start_utc). Verified unique across
-- the source dataset at plan time (COUNT(*) = COUNT(DISTINCT ...) = 769,077).
--
-- We assert grain uniqueness before the upsert to catch upstream schema drift
-- loudly instead of silently deduping via ON CONFLICT.

DO $$
DECLARE
    v_dupes bigint;
BEGIN
    SELECT count(*) INTO v_dupes
    FROM (
        SELECT 1
        FROM staging.neso_dfr_results
        GROUP BY auction_unit, auction_product, delivery_start_utc
        HAVING count(*) > 1
    ) AS dup;

    IF v_dupes > 0 THEN
        RAISE EXCEPTION
            'fact grain violation: % (auction_unit, auction_product, delivery_start_utc) groups have duplicates',
            v_dupes;
    END IF;
END $$;

INSERT INTO marts.fact_auction_result (
    auction_unit,
    auction_product,
    service_type,
    delivery_start_utc,
    delivery_end_utc,
    executed_quantity_mw,
    clearing_price_gbp_per_mw_h,
    unit_result_id
)
SELECT
    s.auction_unit,
    s.auction_product,
    s.service_type,
    s.delivery_start_utc,
    s.delivery_end_utc,
    s.executed_quantity_mw,
    s.clearing_price_gbp_per_mw_h,
    s.unit_result_id
FROM staging.neso_dfr_results AS s
ON CONFLICT (auction_unit, auction_product, delivery_start_utc) DO UPDATE
SET
    service_type                = EXCLUDED.service_type,
    delivery_end_utc            = EXCLUDED.delivery_end_utc,
    executed_quantity_mw        = EXCLUDED.executed_quantity_mw,
    clearing_price_gbp_per_mw_h = EXCLUDED.clearing_price_gbp_per_mw_h,
    unit_result_id              = EXCLUDED.unit_result_id;
