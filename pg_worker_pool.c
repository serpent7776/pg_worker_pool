#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "utils/backend_status.h"
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

#define MAX_WORKERS 8

static WorkerPool* pool = NULL;
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
	appendStringInfo(&buf, "INSERT INTO worker_pool.jobs(worker_name, query_text, status) VALUES ('%s', '%s', 'waiting')",
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

PG_FUNCTION_INFO_V1(pg_worker_pool_launch);
Datum pg_worker_pool_launch(PG_FUNCTION_ARGS)
{
	const char *worker_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (strlen(worker_name) >= NAMEDATALEN)
		ereport(ERROR, (errmsg("Worker name too long")));

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
	LWLockAcquire(pool->lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		for (int i = 0; i < worker_pool_size; i++)
		{
			if (strcmp(pool->worker[i].name, worker_name) == 0)
			{
				if (!pool->worker[i].is_active)
				{
					worker.bgw_main_arg = Int32GetDatum(i),

					snprintf(worker.bgw_name, BGW_MAXLEN, "pgworker: %s", worker_name);
					snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_worker_pool");
					snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_worker_main");

					if (RegisterDynamicBackgroundWorker(&worker, &handle))
					{
						pool->worker[i].is_active = true;
					}
					else ereport(WARNING, (errmsg("Failed to start background worker")));
				}
				found = true;
				break;
			}
		}
		if (!found)
			for (int i = 0; i < worker_pool_size; i++)
			{
				if (!pool->worker[i].is_active)
				{
					worker.bgw_main_arg = Int32GetDatum(i),
					snprintf(pool->worker[i].name, BGW_MAXLEN, "%s", worker_name);

					snprintf(worker.bgw_name, BGW_MAXLEN, "pgworker: %s", worker_name);
					snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_worker_pool");
					snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_worker_main");

					if (RegisterDynamicBackgroundWorker(&worker, &handle))
					{
						pool->worker[i].is_active = true;
						found = true;
					}
					else ereport(WARNING, (errmsg("Failed to register background worker")));
					break;
				}
			}
	}
	PG_FINALLY();
	{
		LWLockRelease(pool->lock);
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

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_worker_pool");

	LWLockAcquire(pool->lock, LW_SHARED);
	WorkerInfo* worker = &pool->worker[worker_index];
	const char* worker_name = MemoryContextStrdup(TopMemoryContext, worker->name);
	LWLockRelease(pool->lock);

	MyBackendType = B_BACKEND;
	pgstat_report_appname("pg_worker_pool");
	pgstat_report_activity(STATE_IDLE, NULL);

	StringInfoData buf;
	initStringInfo(&buf);

	while (true)
	{
		StartTransactionCommand();
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not connect to SPI manager"),
				 errdetail("SPI_connect failed")));
		}
		PushActiveSnapshot(GetTransactionSnapshot());

		resetStringInfo(&buf);
		appendStringInfo(&buf,
			"WITH x AS (\n"
			"  SELECT id, worker_name, query_text FROM worker_pool.jobs\n"
			"  WHERE worker_name = '%s' AND status = 'waiting'\n"
			"  FOR UPDATE\n"
			"  LIMIT 1\n"
			")\n"
			"UPDATE worker_pool.jobs AS j SET status = 'pending'\n"
			"FROM x\n"
			"WHERE j.id = x.id\n"
			"RETURNING j.id, j.query_text",
			worker_name);
		SetCurrentStatementStartTimestamp();
		pgstat_report_activity(STATE_RUNNING, buf.data);

		bool isnull;
		if (SPI_execute(buf.data, false, 0) == SPI_OK_UPDATE_RETURNING && SPI_processed == 1)
		{
			Datum id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
			const char *query = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
			SetCurrentStatementStartTimestamp();
			pgstat_report_activity(STATE_RUNNING, query);

			volatile bool exn = false;
			volatile MemoryContext mycontext = CurrentMemoryContext;
			PG_TRY();
			{
				int code = SPI_execute(query, false, 0);
				if (code > 0)
				{
					resetStringInfo(&buf);
					appendStringInfo(&buf, "UPDATE worker_pool.jobs SET status = 'done' WHERE id = %d",
						DatumGetInt32(id));
					SetCurrentStatementStartTimestamp();
					pgstat_report_activity(STATE_RUNNING, buf.data);

					if (SPI_execute(buf.data, false, 0) != SPI_OK_UPDATE)
						ereport(WARNING, (errmsg("Failed to update job status")));
				}
				else
				{
					ereport(WARNING, (errmsg("Query failed with error code %d: %s", code, query)));
				}
			}
			PG_CATCH();
			{
				exn = true;
				MemoryContextSwitchTo(mycontext);
				EmitErrorReport();
				FlushErrorState();
			}
			PG_END_TRY();
			if (exn)
			{
				SPI_finish();
				PopActiveSnapshot();
				if (IsTransactionState())
					AbortCurrentTransaction();

				StartTransactionCommand();
				if (SPI_connect() != SPI_OK_CONNECT)
					ereport(ERROR, (errmsg("could not connect to SPI manager")));
				PushActiveSnapshot(GetTransactionSnapshot());

				resetStringInfo(&buf);
				appendStringInfo(&buf, "UPDATE worker_pool.jobs SET status = 'failed' WHERE id = %d",
					DatumGetInt32(id));
				SetCurrentStatementStartTimestamp();
				pgstat_report_activity(STATE_RUNNING, buf.data);

				if (SPI_execute(buf.data, false, 0) != SPI_OK_UPDATE)
					ereport(WARNING, (errmsg("Failed to update job status")));
			}
		}
		else
		{
			LWLockAcquire(pool->lock, LW_EXCLUSIVE);
			worker->is_active = false;
			LWLockRelease(pool->lock);
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			break;
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
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
	pool = ShmemInitStruct("pg_worker_pool", sizeof(WorkerPool) + worker_pool_size * sizeof(WorkerInfo), &found);
	if (!found) {
		for (int i = 0; i < worker_pool_size; i++)
		{
			pool->worker[i].is_active = false;
			memset(pool->worker[i].name, 0, NAMEDATALEN);
		}
		pool->lock = &(GetNamedLWLockTranche("worker_pool"))->lock;
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
