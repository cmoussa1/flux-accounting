#!/bin/bash

test_description='test validating job size compared to max resources limits'

. `dirname $0`/sharness.sh

mkdir -p config

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB_PATH=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 4 job -o,--config-path=$(pwd)/config

flux setattr log-stderr-level 1

test_expect_success 'allow guest access to testexec' '
	flux config load <<-EOF
	[exec.testexec]
	allow-guests = true
	EOF
'

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

test_expect_success 'add banks' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1
'

test_expect_success 'add an association' '
	flux account add-user --username=user1 --userid=50001 --bank=A
'

test_expect_success 'send flux-accounting DB information to the plugin' '
	flux account-priority-update -p ${DB_PATH}
'

test_expect_success 'submit a sleep job and specify duration' '
    job1=$(flux python ${SUBMIT_AS} 50001 -S duration=3600 sleep 60) &&
	flux job wait-event -vt 3 ${job1} alloc
'

test_expect_success 'update duration of job' '
    flux update ${job1} duration=+1h
'

test_expect_success 'cancel job' '
    flux cancel ${job1} &&
    flux job wait-event ${job1} clean
'

test_expect_success 'submit a sleep job and do not specify duration' '
    job2=$(flux python ${SUBMIT_AS} 50001 sleep 60) &&
	flux job wait-event -vt 3 ${job2} alloc
'

test_expect_success 'cancel job' '
    flux cancel ${job2} &&
    flux job wait-event ${job2} clean
'

test_expect_success 'fetch job records' '
    flux account-fetch-job-records -p ${DB_PATH}
'

test_expect_success 'configure policy.limits.duration and an unlimited queue' '
	cat >config/config.toml <<-EOT &&
	[queues.bronze]
	[queues.bronze.policy.limits]
	duration = "1h"
	EOT
	flux config reload &&
	flux queue start --all
'

test_expect_success 'add a queue' '
	flux account add-queue bronze &&
	flux account edit-user user1 --queues=bronze &&
	flux account-priority-update -p ${DB_PATH}
'

test_expect_success 'submit a sleep job and do not specify duration' '
	job3=$(flux python ${SUBMIT_AS} 50001 --queue=bronze -S duration=3600 sleep 60) &&
	flux job wait-event -vt 3 ${job3} alloc &&
	flux cancel ${job3} &&
	flux job wait-event -vt 3 ${job3} clean
'

test_expect_success 'fetch job records' '
	flux account-fetch-job-records -p ${DB_PATH}
'

test_expect_success 'call view-job-records; ensure all three jobs show up' '
	flux account view-job-records > all_jobs.out &&
	test $(grep -c "A" all_jobs.out) -eq 3
'

test_expect_success 'call view-job-records and format with just "duration"' '
	flux account view-job-records -o "{jobid:<12} | {elapsed:<15}"
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
