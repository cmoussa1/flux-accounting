#!/bin/bash

test_description='test limiting total scheduled resources per queue'

. `dirname $0`/sharness.sh

mkdir -p config

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 4 job -o,--config-path=$(pwd)/config -Slog-stderr-level=1

test_expect_success 'allow guest access to testexec' '
	flux config load <<-EOF
	[exec.testexec]
	allow-guests = true
	EOF
'

test_expect_success 'create flux-accounting DB' '
	flux account -p ${DB} create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB} -t
'

test_expect_success 'add queues to DB' '
	flux account add-queue ptotal --max-nodes=4 --max-cores=4 &&
	flux account add-queue pfill
'

test_expect_success 'add banks to DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 &&
	flux account add-bank --parent-bank=root B 1
'

test_expect_success 'add associations to DB' '
	flux account add-user \
		--username=user1 \
		--bank=A \
		--userid=50001 \
		--queues=ptotal,pfill \
		--max-active-jobs=10000 \
		--max-running-jobs=1000 &&
	flux account add-user \
		--username=user2 \
		--bank=B \
		--userid=50002 \
		--queues=ptotal \
		--max-active-jobs=10000 \
		--max-running-jobs=1000
'

test_expect_success 'load and initialize priority plugin' '
	flux jobtap load -r .priority-default \
		${MULTI_FACTOR_PRIORITY} "config=$(flux account export-json)" &&
	flux jobtap list | grep mf_priority
'

test_expect_success 'configure flux with queues' '
	cat >config/queues.toml <<-EOT &&
	[queues.ptotal]
	[queues.pfill]
	EOT
	flux config reload &&
	flux queue start --all
'

test_expect_success 'queue total limits are configured in plugin' '
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e ".queues.ptotal.max_nodes == 4" <query.json &&
	jq -e ".queues.ptotal.max_cores == 4" <query.json
'

test_expect_success 'user1 job fills ptotal headroom in SCHED' '
	filler=$(flux python ${SUBMIT_AS} 50001 -N4 --queue=pfill sleep inf) &&
	flux job wait-event -t 5 ${filler} alloc &&
	job1=$(flux python ${SUBMIT_AS} 50001 -N4 --queue=ptotal sleep inf) &&
	flux job wait-event -t 5 ${job1} priority
'

test_expect_success 'user2 job is held by queue total dependencies' '
	job2=$(flux python ${SUBMIT_AS} 50002 -N1 --queue=ptotal sleep inf) &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-nodes-queue-total-limit" \
		${job2} dependency-add &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-cores-queue-total-limit" \
		${job2} dependency-add
'

test_expect_success 'user1 job is held by queue total dependencies' '
	job3=$(flux python ${SUBMIT_AS} 50001 -N1 --queue=ptotal sleep inf) &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-nodes-queue-total-limit" \
		${job3} dependency-add &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-cores-queue-total-limit" \
		${job3} dependency-add
'

test_expect_success 'SCHED to RUN does not release queue total dependencies' '
	flux cancel ${filler} &&
	flux job wait-event -t 5 ${job1} alloc &&
	test_must_fail flux job wait-event -t 2 \
		--match-context=description="max-sched-nodes-queue-total-limit" \
		${job3} dependency-remove &&
	test_must_fail flux job wait-event -t 2 \
		--match-context=description="max-sched-cores-queue-total-limit" \
		${job3} dependency-remove &&
	flux cancel ${job3} &&
	flux job wait-event -t 5 ${job3} clean
'

test_expect_success 'inactive releases queue total dependencies' '
	flux cancel ${job1} &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-nodes-queue-total-limit" \
		${job2} dependency-remove &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-cores-queue-total-limit" \
		${job2} dependency-remove &&
	flux job wait-event -t 5 ${job2} alloc
'

test_expect_success 'clean up jobs' '
	flux cancel ${job2} &&
	flux job wait-event -t 5 ${job2} clean &&
	flux job wait-event -t 5 ${job1} clean &&
	flux job wait-event -t 5 ${job3} clean &&
	flux job wait-event -t 5 ${filler} clean
'

test_expect_success 'shut down flux-accounting service' '
	flux python <<-EOF
	import flux
	flux.Flux().rpc("accounting.shutdown_service").get()
	EOF
'

test_done
