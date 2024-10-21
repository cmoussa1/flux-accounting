#!/bin/bash

test_description='test calculating job usage by project'

. `dirname $0`/sharness.sh

DB_PATH=$(pwd)/FluxAccountingTest.db
MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
QUERYCMD="flux python ${SHARNESS_TEST_SRCDIR}/scripts/query.py"

# export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
export FLUX_CONF_DIR=$(pwd)
test_under_flux 4 job

flux setattr log-stderr-level 1

# select job records from flux-accounting DB
select_job_records() {
		local dbpath=$1
		query="SELECT * FROM jobs;"
		${QUERYCMD} -t 100 ${dbpath} "${query}"
}

test_expect_success 'allow guest access to testexec' '
	flux config load <<-EOF
	[exec.testexec]
	allow-guests = true
	EOF
'

test_expect_success 'create flux-accounting DB' '
	flux account -p $(pwd)/FluxAccountingTest.db create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'load multi-factor priority plugin' '
	flux jobtap load -r .priority-default ${MULTI_FACTOR_PRIORITY} &&
	flux jobtap list | grep mf_priority
'

test_expect_success 'add some banks to the DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1
'

test_expect_success 'add some projects to the DB' '
	flux account add-project P1 &&
	flux account add-project P2
'

test_expect_success 'add an association to the DB' '
	username=$(whoami) &&
	uid=$(id -u) &&
	flux account add-user --username=$username --userid=$uid --bank=A
'

test_expect_success 'edit project list for association' '
	flux account edit-user $username --projects="P1,P2"
'

test_expect_success 'update plugin with flux-accounting information' '
	flux account-priority-update -p ${DB_PATH}
'

test_expect_success 'submit some jobs under P1 project' '
	job1=$(flux submit -N1 -S=system.bank=A -S=system.project=P1 sleep 1) &&
	job2=$(flux submit -N1 -S=system.bank=A -S=system.project=P1 sleep 1) &&
	flux job wait-event -vt 5 ${job1} clean &&
	flux job wait-event -vt 5 ${job2} clean
'

test_expect_success 'submit some jobs under P2 project' '
	job1=$(flux submit -N1 -S=system.bank=A -S=system.project=P2 sleep 1) &&
	job2=$(flux submit -N1 -S=system.bank=A -S=system.project=P2 sleep 1) &&
	flux job wait-event -vt 5 ${job1} clean &&
	flux job wait-event -vt 5 ${job2} clean
'

test_expect_success 'submit some jobs under * project' '
	job1=$(flux submit -N1 -S=system.bank=A sleep 1) &&
	job2=$(flux submit -N1 -S=system.bank=A sleep 1) &&
	flux job wait-event -vt 5 ${job1} clean &&
	flux job wait-event -vt 5 ${job2} clean
'

test_expect_success 'get completed jobs' '
	flux account-fetch-job-records -p ${DB_PATH}
'

test_expect_success 'update all-time job usage by project' '
	flux account calc-project-usage
'

test_expect_success 'get job usage for a specific project' '
	flux account calc-project-usage --project=P1
'

test_expect_success 'list all projects' '
	flux account list-projects
'

test_expect_success 'remove flux-accounting DB' '
	rm $(pwd)/FluxAccountingTest.db
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
