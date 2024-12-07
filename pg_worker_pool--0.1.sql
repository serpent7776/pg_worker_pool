CREATE SCHEMA worker_pool;

CREATE TABLE worker_pool.jobs (
    id SERIAL PRIMARY KEY,
    worker_name TEXT NOT NULL,
    query_text TEXT NOT NULL,
    status TEXT CHECK (status IN ('waiting', 'pending', 'done', 'failed')) DEFAULT 'waiting'
);

CREATE PROCEDURE worker_pool.submit(worker TEXT, query TEXT)
AS 'MODULE_PATHNAME', 'pg_worker_pool_submit' LANGUAGE c;
