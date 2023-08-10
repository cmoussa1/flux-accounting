#!/bin/bash

test_description='test validating and setting project names in priority plugin'

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
	flux account -p $(pwd)/FluxAccountingTest.db create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'add banks to the DB' '
	flux account -p ${DB_PATH} add-bank root 1 &&
	flux account -p ${DB_PATH} add-bank --parent-bank=root account1 1
'

test_expect_success 'add projects to the DB' '
	flux account -p ${DB_PATH} add-project projectA &&
	flux account -p ${DB_PATH} add-project projectB &&
	flux account -p ${DB_PATH} add-project projectC
'

test_expect_success 'submit a job under a user before plugin gets updated' '
	job0=$(flux python ${SUBMIT_AS} 1003 hostname) &&
	test $(flux jobs -no {state} ${job0}) = PRIORITY
'

test_expect_success 'add user to flux-accounting DB and to plugin; job transitions to RUN' '
	flux account -p ${DB_PATH} add-user --username=user1003 --userid=1003 --bank=account1 &&
	flux account-priority-update -p ${DB_PATH} &&
	test $(flux jobs -no {state} ${job0}) = RUN
'

test_expect_success 'check that project gets updated for submitted job' '
	flux job info $job0 eventlog > eventlog.out &&
	grep "{\"attributes.system.project\":\"\*\"}" eventlog.out &&
	flux job cancel $job0
'

test_expect_success 'add a user with a list of projects to the DB' '
	flux account -p ${DB_PATH} add-user --username=user1001 --userid=1001 --bank=account1 --projects="projectA,projectB"
'

test_expect_success 'send flux-accounting DB information to the plugin' '
	flux account-priority-update -p $(pwd)/FluxAccountingTest.db
'

test_expect_success 'successfully submit a job under a valid project' '
	job1=$(flux python ${SUBMIT_AS} 1001 --setattr=system.project=projectA hostname) &&
	flux job wait-event -f json $job1 priority | jq '.context.priority' > job1.test &&
	cat <<-EOF >job1.expected &&
	50000
	EOF
	test_cmp job1.expected job1.test &&
	flux job info $job1 jobspec > jobspec.out &&
	grep "projectA" jobspec.out &&
	flux job cancel $job1
'

test_expect_success 'submit a job under a project that does not exist' '
	test_must_fail flux python ${SUBMIT_AS} 1001 --setattr=system.project=projectFOO \
		hostname > project_dne.out 2>&1 &&
	test_debug "project_dne.out" &&
	grep "project does not exist: projectFOO" project_dne.out
'

test_expect_success 'submit a job under a project that user does not belong to' '
	test_must_fail flux python ${SUBMIT_AS} 1001 --setattr=system.project=projectC \
		hostname > project_dnb.out 2>&1 &&
	test_debug "project_dnb.out" &&
	grep "project not valid for user: projectC" project_dnb.out
'

test_expect_success 'successfully submit a job under a default project' '
	job2=$(flux python ${SUBMIT_AS} 1001 hostname) &&
	flux job wait-event -f json $job2 priority &&
	flux job info $job2 eventlog > eventlog.out &&
	grep "{\"attributes.system.project\":\"projectA\"}" eventlog.out &&
	flux job cancel $job2
'

test_expect_success 'successfully submit a job under a secondary project' '
	job3=$(flux python ${SUBMIT_AS} 1001 --setattr=system.project=projectB hostname) &&
	flux job info $job3 jobspec > jobspec.out &&
	grep "projectB" jobspec.out &&
	flux job cancel $job3
'

test_expect_success 'update the default project for a user and submit a job under new default' '
	flux account -p ${DB_PATH} edit-user user1001 --default-project=projectB &&
	flux account-priority-update -p ${DB_PATH} &&
	job4=$(flux python ${SUBMIT_AS} 1001 hostname) &&
	flux job info $job4 eventlog > eventlog.out &&
	grep "{\"attributes.system.project\":\"projectB\"}" eventlog.out &&
	flux job cancel $job4
'

test_expect_success 'add a user without specifying any projects (will add a default project of "*")' '
	flux account -p ${DB_PATH} add-user --username=user1002 --userid=1002 --bank=account1 &&
	flux account-priority-update -p ${DB_PATH} &&
	job5=$(flux python ${SUBMIT_AS} 1002 hostname) &&
	flux job info $job5 eventlog > eventlog.out &&
	grep "{\"attributes.system.project\":\"\*\"}" eventlog.out &&
	flux job cancel $job5
'

test_expect_success 'add a project to the new user and update the plugin' '
	flux account -p ${DB_PATH} edit-user user1002 --projects=projectA --default-project=projectA &&
	flux account-priority-update -p ${DB_PATH} &&
	job6=$(flux python ${SUBMIT_AS} 1002 hostname) &&
	flux job info $job6 eventlog > eventlog.out &&
	grep "{\"attributes.system.project\":\"projectA\"}" eventlog.out &&
	flux job cancel $job6
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
