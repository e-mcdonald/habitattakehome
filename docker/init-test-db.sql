-- Runs automatically via postgres's docker-entrypoint-initdb.d mechanism on
-- first container init (empty data dir only — never on an existing volume).
--
-- Provisions a second database, separate from POSTGRES_DB, for the `test`
-- compose service's @pytest.mark.db tests (see conftest.py::clean_db). Keeping
-- this in version control — rather than a one-off manual `createdb` — is what
-- makes `make test` reproducible for CI and for a fresh clone alike.
CREATE DATABASE habitat_test;
