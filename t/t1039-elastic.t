#!/bin/bash

test_description='test fetching jobs and updating the fair share values for a group of users'

. $(dirname $0)/sharness.sh

DB_PATH=$(pwd)/FluxAccountingTest.db
ARCHIVEDIR=`pwd`
ARCHIVEDB="${ARCHIVEDIR}/jobarchive.db"
QUERYCMD="flux python ${SHARNESS_TEST_SRCDIR}/scripts/query.py"
NO_JOBS=${SHARNESS_TEST_SRCDIR}/expected/job_usage/no_jobs.expected

export FLUX_CONF_DIR=$(pwd)
test_under_flux 16 job

flux setattr log-stderr-level 1

test_expect_success 'create flux-accounting DB' '
	flux account -p $(pwd)/FluxAccountingTest.db create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'add some banks to the DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root account1 1 &&
	flux account add-bank --parent-bank=root account2 1
'

test_expect_success 'add some users to the DB' '
	username=$(whoami) &&
	uid=$(id -u) &&
	flux account add-user --username=$username --userid=$uid --bank=account1 --shares=1 &&
	flux account add-user --username=$username --userid=$uid --bank=account2 --shares=1 &&
	flux account add-user --username=user5011 --userid=5011 --bank=account1 --shares=1 &&
	flux account add-user --username=user5012 --userid=5012 --bank=account1 --shares=1
'

test_expect_success 'submit some jobs so they populate flux-core job-archive' '
	jobid1=$(flux submit -N 1 hostname) &&
	jobid2=$(flux submit -N 1 hostname) &&
	jobid3=$(flux submit -N 2 hostname) &&
	jobid4=$(flux submit -N 1 hostname)
'

test_expect_success 'submit some sleep 1 jobs under one user' '
	jobid1=$(flux submit -N 1 sleep 1) &&
	jobid2=$(flux submit -N 1 sleep 1) &&
	jobid3=$(flux submit -n 2 -N 2 sleep 1)
'

test_expect_success 'wait 10 seconds for jobs to finish running' '
	sleep 10
'

test_expect_success 'run fetch-job-records script' '
	flux account-create-elastic-logs flux_jobs.ndjson
'

test_expect_success 'remove flux-accounting DB' '
	rm $(pwd)/FluxAccountingTest.db
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
