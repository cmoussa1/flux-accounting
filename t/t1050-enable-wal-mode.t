#!/bin/bash

test_description='test enabling WAL mode on a flux-accounting database'

. `dirname $0`/sharness.sh
TEST_DB=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job

flux setattr log-stderr-level 1

test_expect_success 'create small_no_tie flux-accounting DB with WAL mode enabled' '
	flux account -p ${TEST_DB} create-db --enable-wal-mode
'

test_expect_success 'start flux-accounting service on small_no_tie DB' '
	flux account-service -p ${TEST_DB} -t
'

test_expect_success 'get DB metadata and ensure WAL mode is enabled' '
	flux account get-db-info > db_metadata.out &&
	grep "Journal Mode: wal" db_metadata.out
'

test_expect_success 'toggle WAL mode to OFF for DB' '
	flux account toggle-wal-mode > toggle.out &&
	grep "Journal mode changed from WAL to DELETE." toggle.out
'

test_expect_success 'get DB metadata and ensure WAL mode is disabled' '
	flux account get-db-info > db_metadata.out &&
	grep "Journal Mode: delete" db_metadata.out
'

test_expect_success 'toggle WAL mode back to ON for DB' '
	flux account toggle-wal-mode > toggle.out &&
	grep "Journal mode changed from DELETE to WAL." toggle.out
'

test_expect_success 'remove flux-accounting DB' '
	rm ${TEST_DB}
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
