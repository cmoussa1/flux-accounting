#!/bin/bash

test_description='test max-preempt-after attributes for banks'

. `dirname $0`/sharness.sh
MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB_PATH=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job

flux setattr log-stderr-level 1

test_expect_success 'load multi-factor priority plugin' '
	flux jobtap load -r .priority-default ${MULTI_FACTOR_PRIORITY}
'

test_expect_success 'check that mf_priority plugin is loaded' '
	flux jobtap list | grep mf_priority
'

test_expect_success 'create flux-accounting DB' '
	flux account -p ${DB_PATH} create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'add banks to the DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 --max-preempt-after=60s &&
	flux account add-bank --parent-bank=root B 1 --max-preempt-after=1h &&
	flux account add-bank --parent-bank=root C 1
'

test_expect_success 'add an association to the DB' '
	flux account add-user --username=user1 --userid=5001 --bank=A &&
	flux account add-user --username=user1 --userid=5001 --bank=B &&
	flux account add-user --username=user1 --userid=5001 --bank=C
'

test_expect_success 'send flux-accounting DB information to the plugin' '
	flux account-priority-update -p ${DB_PATH}
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
