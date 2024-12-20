# pg_worker_pool

A Postgresql extension that creates a pool of named background workers.

This uses [Postgresql background workers](https://www.postgresql.org/docs/current/bgworker.html) functionality.

Keep in mind that Postgresql has limited number of available background workers. This is specified by `max_worker_processes` configuration. Changing that limit requires server restart.

Currently this extension has hard-coded number of workers which is set to `8`.
If you want to change it, you can set `MAX_WORKERS` variable in `pg_worker_pool.c` to any other number and recompile the extension.
Increasing this number will let you start more workers.
Making this larger than `max_worker_processes` will yield no effect though. This is a Postgresql limitation.
You could also decrease that number, but I'm not sure why would one do that. You can control how many workers actually start by choosing appropriate worker name.

Background workers are created on-demand and exit as soon as they aren't needed.

## Usage

Upon creation, this extension creates `worker_pool` schema.

`worker_pool.submit` can be used to add query to be executed on a specified background worker. This also starts the worker at the end of current transaction (if committed). If the current transaction is aborted, the worker is not started.
If you start more than one worker in the same transaction, the order in which they are started in unspecified. It might be and likely will be different than the order of submission. Do not rely on this ordering.

`worker_pool.launch` can be used to start a specified worker without adding any queries to the queue.

`worker_pool.jobs` stores all queries submitted to be executed.

Worker name can be any string with max length of `NAMEDATALEN` (by default 64 characters).

This extension must be loaded via `shared_preload_libraries` to correctly allocate required shared memory.

## How to install

Regular `git clone` + `make` + `make install` should be enough.

Note that `pg_config` utility needs to be in your `$PATH`.
It is commonly part of `postgresql-common` package on Debian/Ubuntu and `postgresql-libs` on Arch systems.

```
git clone https://github.com/serpent7776/pg_worker_pool.git
cd pg_worker_pool
make
sudo make install
```

## Example

```sql
CREATE EXTENSION pg_worker_pool;
CALL worker_pool.submit('foo', 'create index myindex_1 on my_big_table (id)');
CALL worker_pool.submit('foo', 'create index myindex_2 on my_big_table (name)');
CALL worker_pool.submit('bar', 'create index otherindex_1 on other_big_table (author)');
CALL worker_pool.submit('bar', 'create index otherindex_2 on other_big_table (title)');
```

This will start two background workers, `foo` and `bar`.
`foo` will create an indices on table `my_big_table` and `bar` on table `other_big_table`.
`foo` and `bar` will run independently of each other, but all indices submitted to the same worker will be created in order.

## FAQ

### How many workers should I start?

It's hard to answer that in general case, but you probably don't want to start more than the number of CPUs you have on the machine, especially for long running queries.
