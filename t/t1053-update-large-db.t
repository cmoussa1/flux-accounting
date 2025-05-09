#!/bin/bash

test_description='test updating fair-share and job usage with a large DB'

. `dirname $0`/sharness.sh

FLUX_ACCOUNTING_DB=${SHARNESS_TEST_SRCDIR}/expected/large_dbs/FluxAccountingLarge.db
UPDATE_RANDOM_USG=${SHARNESS_TEST_SRCDIR}/scripts/update_random_usage.py

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job

flux setattr log-stderr-level 1

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${FLUX_ACCOUNTING_DB} -t
'

test_expect_success 'update the job_usage column with random job usage values' '
    flux python ${UPDATE_RANDOM_USG} ${FLUX_ACCOUNTING_DB}
'

test_expect_success 'call update-usage && update-fshare' '
    flux account update-usage &&
    flux account-update-fshare -p ${FLUX_ACCOUNTING_DB}
'

test_expect_success 'reset the job_usage column to just 0' '
    flux python ${UPDATE_RANDOM_USG} ${FLUX_ACCOUNTING_DB} --reset
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
