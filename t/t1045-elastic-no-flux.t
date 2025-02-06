#!/bin/bash

test_description='test fetching jobs and updating the fair share values for a group of users'

. $(dirname $0)/sharness.sh

export FLUX_CONF_DIR=$(pwd)

flux setattr log-stderr-level 1

test_expect_success 'run fetch-job-records script' '
	test_must_fail flux account-create-elastic-logs > failure.out 2>&1 &&
    grep "Could not connect to Flux instance; Flux may be down" failure.out
'

test_expect_success 'check log file' '
    cat create_flux_job_logs.log
'

test_done
