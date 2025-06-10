#!/bin/bash

test_description='test the calc-job-priority command'

. `dirname $0`/sharness.sh

DB_PATH=$(pwd)/FluxAccountingTest.db

mkdir -p config

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
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

test_expect_success 'add some banks' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 &&
	flux account add-bank --parent-bank=root B 1 --priority=100
'

test_expect_success 'add some queues' '
	flux account add-queue bronze &&
	flux account add-queue gold --priority=100
'


test_expect_success 'add two different associations' '
	flux account add-user --username=user1 --userid=50001 --bank=A &&
	flux account add-user --username=user1 --userid=50001 --bank=B --queues=gold
'

test_expect_success 'call calc-job-priority with no optional argument' '
	flux account calc-job-priority user1
'

test_expect_success 'call calc-job-priority with the --bank optional argument' '
	flux account calc-job-priority user1 --bank=B
'

test_expect_success 'call calc-job-priority with the --bank and --queue optional arguments' '
	flux account calc-job-priority user1 --bank=B --queue=gold
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
