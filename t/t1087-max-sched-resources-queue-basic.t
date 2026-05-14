#!/bin/bash

test_description='test managing the max-sched-nodes/cores property per-queue'

. `dirname $0`/sharness.sh

mkdir -p config

DB=$(pwd)/FluxAccountingTest.db

export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 16 job -o,--config-path=$(pwd)/config -Slog-stderr-level=1

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

test_expect_success 'view-queue: default max_sched_*_per_assoc values show up' '
	flux account add-queue q1 &&
	flux account view-queue q1 > view_queue1.out &&
	grep "\"max_sched_nodes_per_assoc\": \"unlimited\"" view_queue1.out
'

test_expect_success 'view-queue: configured max_sched_*_per_assoc values show up' '
	flux account add-queue q2 \
		--max-sched-nodes-per-assoc=8765 \
		--max-sched-cores-per-assoc=4321 &&
	flux account view-queue q2 > view_queue2.out &&
	grep "\"max_sched_nodes_per_assoc\": 8765" view_queue2.out &&
	grep "\"max_sched_cores_per_assoc\": 4321" view_queue2.out
'

test_expect_success 'view-queue --parsable: default max_sched_*_per_assoc values show up' '
	flux account view-queue q1 --parsable > view_queue3.out &&
	grep "max_sched_nodes_per_assoc | max_sched_cores_per_assoc" view_queue3.out
'

test_expect_success 'view-queue --parsable: configured max_sched_*_per_assoc values show up' '
	flux account view-queue q2 --parsable > view_queue4.out &&
	grep "max_sched_nodes_per_assoc | max_sched_cores_per_assoc" view_queue4.out &&
	grep "8765                      | 4321" view_queue4.out
'

test_expect_success 'view-queue -o: max_sched_*_per_assoc can be passed' '
	flux account view-queue q1 \
		-o "{queue:<8} | {max_sched_nodes_per_assoc:<15} | {max_sched_cores_per_assoc:<15}" > view_queue5.out &&
	grep "queue    | max_sched_nodes_per_assoc | max_sched_cores_per_assoc" view_queue5.out &&
	grep "q1       | 2147483647      | 2147483647" view_queue5.out
'

test_expect_success 'list-queues: max_sched_*_per_assoc properties show up' '
	flux account list-queues > list_queues1.out &&
	grep "max_sched_nodes_per_assoc" list_queues1.out &&
	grep "max_sched_cores_per_assoc" list_queues1.out
'

test_expect_success 'list-queues: max_sched_*_per_assoc properties can be specified in --fields' '
	flux account list-queues --fields=queue,max_sched_nodes_per_assoc,max_sched_cores_per_assoc > list_queues2.out &&
	grep "queue | max_sched_nodes_per_assoc | max_sched_cores_per_assoc" list_queues2.out &&
	grep "q1    | unlimited                 | unlimited" list_queues2.out &&
	grep "q2    | 8765                      | 4321" list_queues2.out
'

test_expect_success 'edit-queue: max_sched_*_per_assoc properties can be edited' '
	flux account edit-queue q1 -msn=9999 -msc=9999 &&
	flux account view-queue q1 > edit_queue1.out &&
	grep "\"max_sched_nodes_per_assoc\": 9999" edit_queue1.out &&
	grep "\"max_sched_cores_per_assoc\": 9999" edit_queue1.out
'

test_expect_success 'edit-queue: max_sched_*_per_assoc properties can be reset' '
	flux account edit-queue q1 -msn=-1 -msc=-1 &&
	flux account view-queue q1 > edit_queue2.out &&
	grep "\"max_sched_nodes_per_assoc\": \"unlimited\"" edit_queue2.out &&
	grep "\"max_sched_cores_per_assoc\": \"unlimited\"" edit_queue2.out
'

test_expect_success 'edit-queue: invalid max_sched_*_per_assoc values are rejected' '
	test_must_fail flux account edit-queue q1 -msn=-2 > bad_value1.out 2>&1 &&
	grep "value must be a non-negative integer or -1 to reset to default" bad_value1.out &&
	test_must_fail flux account edit-queue q1 -msc=-2 > bad_value2.out 2>&1 &&
	grep "value must be a non-negative integer or -1 to reset to default" bad_value2.out
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
