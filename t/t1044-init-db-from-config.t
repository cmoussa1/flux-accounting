#!/bin/bash

test_description='Test configuring weights of multi-factor priority factors'

. `dirname $0`/sharness.sh
MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
SEND_PAYLOAD=${SHARNESS_TEST_SRCDIR}/scripts/send_payload.py
DB_PATH=$(pwd)/FluxAccountingTest.db

mkdir -p config

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job -o,--config-path=$(pwd)/config

flux setattr log-stderr-level 1

test_expect_success 'allow guest access to testexec' '
	flux config load <<-EOF
	[exec.testexec]
	allow-guests = true
	EOF
'

test_expect_success 'create a flux-accounting DB' '
	flux account -p ${DB_PATH} create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'set up new configuration for multi-factor priority plugin' '
	cat >config/test.toml <<-EOT &&
	[accounting.queue-priorities]
	bronze = 100
	silver = 200
	gold = 300
	platinum = 400
	EOT
	flux config reload &&
	flux account load-config
'

test_expect_success 'ensure queues, associated priorities exist in queue_table' '
	flux account view-queue bronze > bronze.test &&
	grep "bronze" bronze.test &&
	grep "100" bronze.test &&
	flux account view-queue silver > silver.test &&
	grep "silver" silver.test &&
	grep "200" silver.test &&
	flux account view-queue gold > gold.test &&
	grep "gold" gold.test &&
	grep "300" gold.test &&
	flux account view-queue platinum > platinum.test &&
	grep "platinum" platinum.test &&
	grep "400" platinum.test
'

test_expect_success 'create new configuration and re-load config' '
	cat >config/test.toml <<-EOT &&
	[accounting.queue-priorities]
	bronze = 900
	red = 600
	blue = 700
	EOT
	flux config reload &&
	flux account load-config --config-path config/test.toml
'

test_expect_success 'ensure queues, associated priorities exist in queue_table' '
	flux account view-queue bronze > bronze.test &&
	grep "bronze" bronze.test &&
	grep "900" bronze.test &&
	flux account view-queue red > red.test &&
	grep "red" red.test &&
	grep "600" red.test &&
	flux account view-queue blue > blue.test &&
	grep "blue" blue.test &&
	grep "700" blue.test
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
