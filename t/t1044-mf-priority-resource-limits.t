#!/bin/bash

test_description='track resources across running jobs per-association in priority plugin'

. `dirname $0`/sharness.sh

mkdir -p conf.d

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB_PATH=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 4 job -o,--config-path=$(pwd)/conf.d

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

test_expect_success 'add an association, configure max resource limits' '
	flux account add-user \
		--username=user1 --userid=5001 --bank=A \
		--max-active-jobs=1000 --max-running-jobs=999 \
		--max-nodes=2 --max-cores=3
'

test_expect_success 'send flux-accounting DB information to the plugin' '
	flux account-priority-update -p ${DB_PATH}
'

test_expect_success 'submit 2 jobs that take up 1 node each; check resource counts' '
	job1=$(flux python ${SUBMIT_AS} 5001 -N1 sleep 60) &&
	flux job wait-event -f json ${job1} priority &&
	job2=$(flux python ${SUBMIT_AS} 5001 -N1 sleep 60) &&
	flux job wait-event -f json ${job2} priority &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_nodes == 2" <query.json &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_cores == 2" <query.json
'

test_expect_success 'cancel jobs; check resource counts' '
	flux cancel ${job1} &&
	flux job wait-event -f json ${job1} clean &&
	flux cancel ${job2} &&
	flux job wait-event -f json ${job2} clean &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_nodes == 0" <query.json &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_cores == 0" <query.json
'

test_expect_success 'submit a job that takes up one core' '
	job3=$(flux python ${SUBMIT_AS} 5001 -n1 sleep 60) &&
	flux job wait-event -f json ${job3} priority &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_nodes == 0" <query.json &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_cores == 1" <query.json
'

test_expect_success 'cancel job; check resource counts' '
	flux cancel ${job3} &&
	flux job wait-event -f json ${job3} clean &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_nodes == 0" <query.json &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_cores == 0" <query.json
'

test_expect_success 'submit enough jobs to take up max-nodes limit' '
	job1=$(flux python ${SUBMIT_AS} 5001 -N1 sleep 60) &&
	flux job wait-event -f json ${job1} priority &&
	job2=$(flux python ${SUBMIT_AS} 5001 -N1 sleep 60) &&
	flux job wait-event -f json ${job2} priority
'

test_expect_success 'check resource counts of association' '
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].max_nodes == 2" <query.json &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].cur_nodes == 2" <query.json
'

test_expect_success 'trigger max-nodes limit for association' '
	job3=$(flux python ${SUBMIT_AS} 5001 -N1 sleep 60) &&
	flux job wait-event -vt 10 \
		--match-context=description="max-resource-user-limit" \
		${job3} dependency-add &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".mf_priority_map[] | select(.userid == 5001) | .banks[0].held_jobs | length == 1" <query.json
'

test_expect_success 'cancel first job; check inactive_cb()' '
	flux cancel ${job1} &&
	flux job wait-event -vt 10 ${job3} alloc
'

test_expect_success 'sleep and then cancel the other jobs' '
	flux cancel ${job2} &&
	flux cancel ${job3}
'

test_done
