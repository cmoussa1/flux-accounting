#!/bin/bash

test_description='test priority plugin keeping track of dependencies after reload'

. `dirname $0`/sharness.sh

mkdir -p config

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 16 job -o,--config-path=$(pwd)/config

flux setattr log-stderr-level 1

test_expect_success 'allow guest access to testexec' '
	flux config load <<-EOF
	[exec.testexec]
	allow-guests = true
	EOF
'

test_expect_success 'create flux-accounting DB' '
	flux account -p ${DB} create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB} -t
'

test_expect_success 'load multi-factor priority plugin' '
	flux jobtap load -r .priority-default ${MULTI_FACTOR_PRIORITY} &&
	flux jobtap list | grep mf_priority
'

test_expect_success 'add some banks' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1
'

test_expect_success 'add an association' '
	flux account add-user \
		--username=user1 \
		--userid=50001 \
		--bank=A \
		--max-running-jobs=1
'

test_expect_success 'send flux-accounting DB information to the plugin' '
	flux account-priority-update -p ${DB}
'

test_expect_success 'submit two jobs to trigger max-running-jobs-limit' '
	job1=$(flux python ${SUBMIT_AS} 50001 -N1 sleep inf) &&
	flux job wait-event -t 5 ${job1} alloc &&
	job2=$(flux python ${SUBMIT_AS} 50001 -N1 sleep inf) &&
	flux job wait-event -t 5 \
		--match-context=description="max-running-jobs-user-limit" \
		${job2} dependency-add
'

# Reloading the plugin will clear the priority plugin's aux items from the
# current jobs. Any jobs held with a flux-accounting dependency before the
# plugin was reloaded will have their previous dependencies cleared.
test_expect_success 'reload plugin' '
	flux jobtap remove mf_priority.so &&
	flux jobtap load ${MULTI_FACTOR_PRIORITY} hello=1234 &&
	flux jobtap list | grep mf_priority
'

test_expect_success 'ensure job1 is still running' '
	flux job wait-event -t 5 ${job1} alloc
'

# job2 will have its old flux-accounting dependencies cleared.
test_expect_success 'ensure old dependencies on job2 have been cleared' '
	flux job wait-event -t 5 \
		--match-context=description="max-running-jobs-user-limit" \
		${job2} dependency-remove
'

# Once the priority plugin is updated with flux-accounting data, the job will
# be updated with the appropriate information and can proceed to run.
test_expect_success 'send flux-accounting DB information to the plugin' '
	flux account-priority-update -p ${DB} &&
	flux job wait-event -t 5 ${job2} alloc
'

test_expect_success 'cancel job1' '
	flux cancel ${job1} &&
	flux job wait-event -t 5 ${job1} clean
'

test_expect_success 'cancel job2' '
	flux cancel ${job2} &&
	flux job wait-event -t 5 ${job2} clean
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
