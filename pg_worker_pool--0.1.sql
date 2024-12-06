CREATE TABLE pg_worker_pool_jobs (
    id SERIAL PRIMARY KEY,
    worker_name TEXT NOT NULL,
    query_text TEXT NOT NULL,
    status TEXT CHECK (status IN ('waiting', 'pending', 'done', 'failed')) DEFAULT 'waiting'
);

CREATE PROCEDURE pg_worker_pool_submit(worker TEXT, query TEXT)
AS 'MODULE_PATHNAME', 'pg_worker_pool_submit' LANGUAGE c;
