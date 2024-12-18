# pg_worker_pool

A Postgresql extension that creates a pool of named background workers.

This uses [Postgresql background workers](https://www.postgresql.org/docs/current/bgworker.html) functionality.

Keep in mind that Postgresql has limited number of available background workers. This is specified by `max_worker_processes` configuration. Changing that limit requires server restart.

Currently this extension has hard-coded number of workers which is set to `8`.

Background workers are created on-demand and are exited as soon as they aren't needed.

## Usage

Upon creation, this extension creates `worker_pool` schema.

`worker_pool.submit` can be used to add query to be executed on a specified background worker. This also starts the worker at the end of current transaction (if committed). If the current transaction is aborted, the worker is not started.

`worker_pool.launch` can be used to start a specified worker without adding any queries to the queue.

`worker_pool.jobs` stores all queries submitted to be executed.

Worker name can be any string with max length of `NAMEDATALEN` (by default 64 characters).

This extension must be loaded via `shared_preload_libraries` to correctly allocate required shared memory.

## Example

```sql
CREATE EXTENSION pg_worker_pool;
CALL worker_pool.submit('foo', 'create index myindex_1 on my_big_table (id)');
CALL worker_pool.submit('foo', 'create index myindex_1 on my_big_table (name)');
CALL worker_pool.submit('bar', 'create index otherindex_2 on other_big_table (author)');
CALL worker_pool.submit('bar', 'create index otherindex_2 on other_big_table (title)');
```

This will start two background workers, `foo` and `bar`.
`foo` will create an indices on table `my_big_table` and `bar` on table `other_big_table`.
`foo` and `bar` will run independently of each other, but all indices submitted to the same worker will be created in order.
