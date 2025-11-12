#!/bin/bash

test_description='test basic tests with data module'

. `dirname $0`/sharness.sh

mkdir -p config

DB=$(pwd)/FluxJobsTest.db

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
	flux data -p ${DB} create-db
'

test_expect_success 'start flux-accounting service' '
	flux data-service -p ${DB} -t
'

test_expect_success 'call hello' '
    flux data hello
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"data.shutdown_service\").get()"
'

test_done
