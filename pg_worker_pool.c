#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#pragma GCC diagnostic pop

PG_MODULE_MAGIC;

typedef struct WorkerInfo
{
    char name[NAMEDATALEN];
    pid_t pid;
    bool is_active;
} WorkerInfo;

typedef struct WorkerPool
{
	LWLockId lock;
	WorkerInfo worker[];
} WorkerPool;

typedef struct WorkerParams
{
	Oid database;
	Oid user;
} WorkerParams;
_Static_assert (sizeof(WorkerParams) < BGW_EXTRALEN, "WorkerParams too big");

typedef struct XactArgs
{
	char worker_name[NAMEDATALEN];
} XactArgs;

#define MAX_WORKERS 64
static WorkerPool* worker_pool = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static int worker_pool_size = MAX_WORKERS;

PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void pg_worker_main(Datum main_arg);
PGDLLEXPORT void pg_worker_pool_on_xact(XactEvent event, void *arg);

PG_FUNCTION_INFO_V1(pg_worker_pool_submit);
Datum pg_worker_pool_submit(PG_FUNCTION_ARGS)
{
	const char *worker_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (strlen(worker_name) >= NAMEDATALEN)
		ereport(ERROR, (errmsg("Worker name too long")));

	SPI_connect();

	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "INSERT INTO pg_worker_pool_jobs(worker_name, query_text, status) VALUES ('%s', '%s', 'pending')",
		worker_name, query);

	if (SPI_execute(buf.data, false, 0) != SPI_OK_INSERT)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("Failed to submit query to worker")));
	}

	SPI_finish();

	XactArgs* xact_args = MemoryContextAlloc(TopTransactionContext, sizeof(XactArgs));
	strncpy(xact_args->worker_name, worker_name, NAMEDATALEN);
	RegisterXactCallback(pg_worker_pool_on_xact, xact_args);

	PG_RETURN_VOID();
}

void pg_worker_pool_on_xact(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT) return;
	if (event == XACT_EVENT_PREPARE || event == XACT_EVENT_PRE_PREPARE) return;
	UnregisterXactCallback(pg_worker_pool_on_xact, arg);
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT) return;

	XactArgs* xact_args = (XactArgs*)arg;
	const char* worker_name = xact_args->worker_name;

	WorkerParams params = {
		.database = MyDatabaseId,
		.user = GetUserId(),
	};

	BackgroundWorkerHandle* handle;
	BackgroundWorker worker = {
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
		.bgw_start_time = BgWorkerStart_RecoveryFinished,
		.bgw_restart_time = BGW_NEVER_RESTART,
		.bgw_notify_pid = MyProcPid,
	};
	memcpy(worker.bgw_extra, &params, sizeof(WorkerParams));

	volatile bool found = false;
	LWLockAcquire(worker_pool->lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		for (int i = 0; i < worker_pool_size; i++)
		{
			if (strcmp(worker_pool->worker[i].name, worker_name) == 0)
			{
				if (!worker_pool->worker[i].is_active)
				{
					worker.bgw_main_arg = Int32GetDatum(i),

					snprintf(worker.bgw_name, BGW_MAXLEN, "pgworker: %s", worker_name);
					snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_worker_pool");
					snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_worker_main");

					if (RegisterDynamicBackgroundWorker(&worker, &handle))
					{
						worker_pool->worker[i].is_active = true;
						found = true;
					}
					else ereport(WARNING, (errmsg("Failed to start background worker")));
				}
				break;
			}
		}
		if (!found)
			for (int i = 0; i < worker_pool_size; i++)
			{
				if (!worker_pool->worker[i].is_active)
				{
					worker.bgw_main_arg = Int32GetDatum(i),
					snprintf(worker_pool->worker[i].name, BGW_MAXLEN, "%s", worker_name);

					snprintf(worker.bgw_name, BGW_MAXLEN, "pgworker: %s", worker_name);
					snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_worker_pool");
					snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_worker_main");

					if (RegisterDynamicBackgroundWorker(&worker, &handle))
					{
						worker_pool->worker[i].is_active = true;
						found = true;
					}
					else ereport(WARNING, (errmsg("Failed to register background worker")));
					break;
				}
			}
	}
	PG_FINALLY();
	{
		LWLockRelease(worker_pool->lock);
	}
	PG_END_TRY();

	if (!found)
		ereport(WARNING, (errmsg("Worker name '%s' not found", worker_name)));
}

void pg_worker_main(Datum main_arg)
{
	const int worker_index = DatumGetInt32(main_arg);
	WorkerParams params;
	memcpy(&params, MyBgworkerEntry->bgw_extra, sizeof(WorkerParams));

	if (worker_index < 0 || worker_index >= worker_pool_size)
		ereport(ERROR, (errmsg("Invalid worker index")));

	BackgroundWorkerInitializeConnectionByOid(params.database, params.user, 0);

	BackgroundWorkerUnblockSignals();

	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_worker_pool");

	LWLockAcquire(worker_pool->lock, LW_SHARED);
	WorkerInfo* worker = &worker_pool->worker[worker_index];
	const char* worker_name = MemoryContextStrdup(TopMemoryContext, worker->name);
	LWLockRelease(worker_pool->lock);

	while (true)
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not connect to SPI manager"),
				 errdetail("SPI_connect failed")));
		}
		PushActiveSnapshot(GetTransactionSnapshot());

		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "SELECT id, query_text FROM pg_worker_pool_jobs WHERE worker_name = '%s' AND status = 'pending' LIMIT 1",
			worker_name);

		bool isnull;
		if (SPI_execute(buf.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
		{
			Datum id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
			const char *query = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

			int code = SPI_execute(query, false, 0);
			if (code > 0)
			{
				resetStringInfo(&buf);
				appendStringInfo(&buf, "UPDATE pg_worker_pool_jobs SET status = 'done' WHERE id = %d",
					DatumGetInt32(id));

				if (SPI_execute(buf.data, false, 0) != SPI_OK_UPDATE)
					ereport(WARNING, (errmsg("Failed to update job status")));
			}
			else
			{
				ereport(WARNING, (errmsg("Query failed with error code %d: %s", code, query)));
			}
		}
		else
		{
			LWLockAcquire(worker_pool->lock, LW_EXCLUSIVE);
			worker->is_active = false;
			LWLockRelease(worker_pool->lock);
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			break;
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pg_usleep(100000);
	}

	proc_exit(0);
}

static void worker_pool_shmem_request(void)
{
	if (prev_shmem_request_hook) {
		prev_shmem_request_hook();
	}

	RequestNamedLWLockTranche("worker_pool", 1);
}

static void worker_pool_shmem_startup(void)
{
	if (prev_shmem_startup_hook) {
		prev_shmem_startup_hook();
	}

	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	InitShmemIndex();
	worker_pool = ShmemInitStruct("pg_worker_pool", sizeof(WorkerPool) + worker_pool_size * sizeof(WorkerInfo), &found);
	if (!found) {
		for (int i = 0; i < worker_pool_size; i++)
		{
			worker_pool->worker[i].is_active = false;
			memset(worker_pool->worker[i].name, 0, NAMEDATALEN);
		}
		worker_pool->lock = &(GetNamedLWLockTranche("worker_pool"))->lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

void _PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR, (errmsg("pg_worker_pool must be loaded via shared_preload_libraries")));

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = worker_pool_shmem_startup;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = worker_pool_shmem_request;
}
