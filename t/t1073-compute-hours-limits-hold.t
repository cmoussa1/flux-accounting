#!/bin/bash

test_description='test loading compute hours limit plugin'

. `dirname $0`/sharness.sh

COMPUTE_HOURS_LIMITS=${FLUX_BUILD_DIR}/src/plugins/.libs/compute_hours_limits.so
DB_PATH=$(pwd)/FluxAccountingTest.db
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py

mkdir -p config

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
	flux account -p ${DB_PATH} create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'load compute hours limits plugin' '
	flux jobtap load ${COMPUTE_HOURS_LIMITS}
'

test_expect_success 'check to see if plugin is loaded' '
	flux jobtap list | grep compute_hours_limits
'

test_expect_success 'add data to flux-accounting DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 &&
	flux account add-user --username=user1 --userid=50001 --bank=A
'

test_expect_success 'send flux-accounting DB data to plugin' '
	flux account-compute-hours-update -p ${DB_PATH}
'

test_expect_success 'set a global usage limit for all associations' '
	cat <<-EOF >config_usage_limits.py
	import json
	import flux

	bulk_update_data = {
		"data" : [
			{
				"max_compute_usage": 10000
			}
		]
	}
	flux.Flux().rpc("job-manager.compute_hours_limits.configure", json.dumps(bulk_update_data)).get()
	EOF
	flux python config_usage_limits.py
'

test_expect_success 'submit a job to take up all usage up to limit' '
	job1=$(flux python ${SUBMIT_AS} 50001 -N1 -S duration=10000 sleep inf) &&
	flux job wait-event --quiet -t 3 ${job1} alloc
'

test_expect_success 'a job submitted while the association is at their max gets held' '
	job2=$(flux python ${SUBMIT_AS} 50001 -N1 -S duration=60 sleep inf) &&
	flux job wait-event -t 3 \
		--match-context=description="max-usage-global" \
		${job2} dependency-add
'

test_expect_success 'killing the first job will free up usage for the second job' '
	flux cancel ${job1} &&
	flux job wait-event -t 3 \
		--match-context=description="max-usage-global" \
		${job2} dependency-remove &&
	flux job wait-event -t 3 ${job2} alloc
'

test_expect_success 'cancel second job' '
	flux cancel ${job2}
'

test_done
