#!/bin/bash

test_description='test updating fair-share and job usage with a large DB'

. `dirname $0`/sharness.sh

FLUX_ACCOUNTING_DB=$(pwd)/FluxAccountingLarge.db
UPDATE_RANDOM_USG=${SHARNESS_TEST_SRCDIR}/scripts/update_random_usage.py

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job

flux setattr log-stderr-level 1

test_expect_success 'create flux-accounting DB' '
    flux account -p ${FLUX_ACCOUNTING_DB} create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${FLUX_ACCOUNTING_DB} -t
'

test_expect_success 'add banks to DB' '
    flux account add-bank root 1 &&
    flux account add-bank --parent-bank=root A 1 &&
    flux account add-bank --parent-bank=root B 1
'

test_expect_success 'add 100 associations to bank A' '
    for i in $(seq 1 100); do
        username="user$i"
        userid=$((50000 + i))
        flux account add-user --username=$username --userid=$userid --bank=A
    done
'

test_expect_success 'add 100 associations to bank B' '
    for i in $(seq 101 200); do
        username="user$i"
        userid=$((50000 + i))
        flux account add-user --username=$username --userid=$userid --bank=B
    done
'

test_expect_success 'update the job_usage column with random job usage values' '
    flux python ${UPDATE_RANDOM_USG} ${FLUX_ACCOUNTING_DB}
'

test_expect_success 'call update-usage && update-fshare' '
    flux account update-usage &&
    flux account-update-fshare -p ${FLUX_ACCOUNTING_DB}
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
