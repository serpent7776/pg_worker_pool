EXTENSION = pg_worker_pool
MODULE_big = pg_worker_pool
OBJS = pg_worker_pool.o
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_CFLAGS = -Wall -Wextra -Wno-declaration-after-statement -ggdb3
DATA = pg_worker_pool--0.1.sql

include $(PGXS)
