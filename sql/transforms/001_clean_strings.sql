-- 001_clean_strings.sql
--
-- The NESO API's own metadata examples contain trailing tabs (\t\t) in string
-- fields. The pydantic model strips them at validate time (via `str.strip`),
-- but this transform is a defensive belt-and-braces pass over anything that
-- may have slipped through — e.g. rows loaded through an earlier version of
-- the validator, or from a source with slightly different quirks.
--
-- Idempotent: only rewrites values that actually contain leading/trailing
-- whitespace. Safe to re-run.

UPDATE staging.neso_dfr_results
SET
    registered_auction_participant = btrim(registered_auction_participant, E' \t\r\n'),
    auction_unit                   = btrim(auction_unit,                   E' \t\r\n'),
    service_type                   = btrim(service_type,                   E' \t\r\n'),
    auction_product                = btrim(auction_product,                E' \t\r\n'),
    technology_type                = btrim(technology_type,                E' \t\r\n'),
    post_code                      = btrim(post_code,                      E' \t\r\n')
WHERE
       registered_auction_participant <> btrim(registered_auction_participant, E' \t\r\n')
    OR auction_unit                   <> btrim(auction_unit,                   E' \t\r\n')
    OR service_type                   <> btrim(service_type,                   E' \t\r\n')
    OR auction_product                <> btrim(auction_product,                E' \t\r\n')
    OR technology_type IS DISTINCT FROM btrim(technology_type, E' \t\r\n')
    OR post_code       IS DISTINCT FROM btrim(post_code,       E' \t\r\n');
