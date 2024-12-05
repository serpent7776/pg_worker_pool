CREATE TABLE pg_worker_pool_jobs (
    id SERIAL PRIMARY KEY,
    worker_name TEXT NOT NULL,
    query_text TEXT NOT NULL,
    status TEXT CHECK (status IN ('pending', 'done')) DEFAULT 'pending'
);

CREATE FUNCTION pg_worker_pool_submit(TEXT, TEXT) RETURNS INTEGER
AS 'MODULE_PATHNAME', 'pg_worker_pool_submit' LANGUAGE c STRICT;
