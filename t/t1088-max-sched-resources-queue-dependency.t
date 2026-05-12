#!/bin/bash

test_description='test limiting number of resources in SCHED per-queue'

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

# Setting max-sched-nodes and max-sched-cores to 4 means that an association
# can have up to 4 nodes and 4 cores in SCHED state in the "pdebug" queue at
# any given time.
test_expect_success 'add a queue to DB' '
	flux account add-queue pdebug \
		--max-sched-nodes-per-assoc=4 \
		--max-sched-cores-per-assoc=4
'

test_expect_success 'add banks to DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1
'

test_expect_success 'add an association to DB' '
	flux account add-user \
		--username=user1 \
		--bank=A \
		--userid=50001 \
		--queues=pdebug \
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
	[queues.pdebug]
	EOT
	flux config reload &&
	flux queue start --all
'

test_expect_success 'queue is properly configured in plugin internal data structure' '
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e \
		".queues.pdebug.max_sched_nodes_per_assoc == 4" <query.json &&
	jq -e \
		".queues.pdebug.max_sched_cores_per_assoc == 4" <query.json
'

# This job will proceed to RUN immediately but take up all current resources
# of this instance, so any job submitted while this job is running will be
# placed in SCHED state.
test_expect_success 'first job proceeds to RUN immediately' '
	job1=$(flux python ${SUBMIT_AS} 50001 -N4 --queue=pdebug sleep inf) &&
	flux job wait-event -t 5 ${job1} alloc
'

# Since the first job is currently running, this job is placed in SCHED state
# until enough resources are freed up for this job to run.
test_expect_success 'second job gets placed in SCHED state' '
	job2=$(flux python ${SUBMIT_AS} 50001 -N4 --queue=pdebug sleep inf) &&
	flux job wait-event -t 5 ${job2} priority
'

test_expect_success 'association has 4 nodes and 4 cores in SCHED state' '
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].queue_usage.pdebug.cur_sched_nodes == 4" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].queue_usage.pdebug.cur_sched_cores == 4" <query.json
'

# Since the association already has 4 nodes and 4 cores in SCHED state, this
# job has dependencies placed on it.
test_expect_success 'third submitted job has SCHED-state-related dependency' '
	job3=$(flux python ${SUBMIT_AS} 50001 -N1 --queue=pdebug sleep inf) &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-nodes-queue-limit" \
		${job3} dependency-add &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-cores-queue-limit" \
		${job3} dependency-add
'

# When the first job gets cancelled, the second job can proceed to run because
# enough resources are freed.
test_expect_success 'second job receives alloc event' '
	flux cancel ${job1} &&
	flux job wait-event -t 5 ${job2} alloc
'

# When the second job proceeds to run, the job.state.run callback checks to see
# if any other jobs held due to the max_sched_nodes and max_sched_cores
# per-queue limit can have their dependency removed.
test_expect_success 'third job has max-sched-nodes and max-sched-cores dependencies removed' '
	flux job wait-event -t 5 \
		--match-context=description="max-sched-nodes-queue-limit" \
		${job3} dependency-remove &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-cores-queue-limit" \
		${job3} dependency-remove
'

test_expect_success 'cancel jobs' '
	flux cancel ${job2} ${job3} &&
	flux job wait-event -t 5 ${job2} clean &&
	flux job wait-event -t 5 ${job3} clean
'

test_expect_success 'check association counts' '
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	cat query.json | jq
'

# In this set of tests, we make sure that with two queues, a job held in queue
# A due to a max SCHED limit is not released when a job in B transitions to RUN
# state.
test_expect_success 'edit pdebug limits' '
	flux account edit-queue pdebug --max-sched-nodes=2 --max-sched-cores=2
'
test_expect_success 'add another queue and give association access to that queue' '
	flux account add-queue pbatch \
		--max-sched-nodes-per-assoc=2 \
		--max-sched-cores-per-assoc=2
	flux account edit-user user1 --queues=pdebug,pbatch &&
	flux account-priority-update -p ${DB}
'

test_expect_success 'update flux with the new queue' '
	cat >config/queues.toml <<-EOT &&
	[queues.pdebug]
	[queues.pbatch]
	EOT
	flux config reload &&
	flux queue start --all
'

test_expect_success 'association hits limits in both queues' '
	pdebug_job1=$(flux python ${SUBMIT_AS} 50001 -N2 --queue=pdebug sleep inf) &&
 	flux job wait-event -t 5 ${pdebug_job1} alloc &&
	pbatch_job1=$(flux python ${SUBMIT_AS} 50001 -N2 --queue=pbatch sleep inf) &&
 	flux job wait-event -t 5 ${pbatch_job1} alloc &&
	pdebug_job2=$(flux python ${SUBMIT_AS} 50001 -N2 --queue=pdebug sleep inf) &&
 	flux job wait-event -t 5 ${pdebug_job2} priority &&
	pbatch_job2=$(flux python ${SUBMIT_AS} 50001 -N2 --queue=pbatch sleep inf) &&
 	flux job wait-event -t 5 ${pbatch_job2} priority
'

test_expect_success 'jobs have dependencies placed on them after limits are hit' '
	pdebug_job3=$(flux python ${SUBMIT_AS} 50001 -N1 --queue=pdebug sleep inf) &&
 	flux job wait-event -t 5 \
 		--match-context=description="max-sched-nodes-queue-limit" \
 		${pdebug_job3} dependency-add &&
 	flux job wait-event -t 5 \
 		--match-context=description="max-sched-cores-queue-limit" \
 		${pdebug_job3} dependency-add &&
	pbatch_job3=$(flux python ${SUBMIT_AS} 50001 -N1 --queue=pbatch sleep inf) &&
 	flux job wait-event -t 5 \
 		--match-context=description="max-sched-nodes-queue-limit" \
 		${pbatch_job3} dependency-add &&
 	flux job wait-event -t 5 \
 		--match-context=description="max-sched-cores-queue-limit" \
 		${pbatch_job3} dependency-add
'

test_expect_success 'ensure job counts for association are accurate' '
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_active_jobs == 6" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_run_jobs == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_sched_jobs == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].held_jobs | length == 2" <query.json &&
	jq -e ".mf_priority_map[] | \
		select(.userid == 50001) | \
		.banks[0].queue_usage[\"pdebug\"].cur_run_jobs == 1" <query.json &&
	jq -e ".mf_priority_map[] | \
		select(.userid == 50001) | \
		.banks[0].queue_usage[\"pbatch\"].cur_run_jobs == 1" <query.json &&
	jq -e ".mf_priority_map[] | \
		select(.userid == 50001) | \
		.banks[0].queue_usage[\"pdebug\"].cur_sched_jobs == 1" <query.json &&
	jq -e ".mf_priority_map[] | \
		select(.userid == 50001) | \
		.banks[0].queue_usage[\"pbatch\"].cur_sched_jobs == 1" <query.json
'

test_expect_success 'job1 in pdebug is cancelled; job2 in pdebug can now run' '
	flux cancel ${pdebug_job1} &&
	flux job wait-event -t 5 ${pdebug_job2} alloc
'

test_expect_success 'held job in pbatch is still not released' '
	pbatch_job3_dec=$(flux job id -t dec ${pbatch_job3}) &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	cat query.json | jq &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_active_jobs == 5" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_run_jobs == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_sched_jobs == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].held_jobs | length == 1" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].held_jobs[\"${pbatch_job3_dec}\"].deps \
			| length == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].held_jobs[\"${pbatch_job3_dec}\"].deps[0] \
			== \"max-sched-nodes-queue-limit\"" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].held_jobs[\"${pbatch_job3_dec}\"].deps[1] \
			== \"max-sched-cores-queue-limit\"" <query.json
'

test_expect_success 'job2 in pdebug cancelled; job2 in pbatch can run' '
	flux cancel ${pdebug_job2} &&
	flux job wait-event -t 5 ${pbatch_job2} alloc
'

test_expect_success 'dependency removed from job3 in pbatch now that job2 in pbatch runs' '
	flux job wait-event -t 5 \
		--match-context=description="max-sched-nodes-queue-limit" \
		${pbatch_job3} dependency-remove &&
	flux job wait-event -t 5 \
		--match-context=description="max-sched-cores-queue-limit" \
		${pbatch_job3} dependency-remove &&
	flux jobtap query mf_priority.so > query.json &&
	test_debug "jq -S . <query.json" &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_active_jobs == 4" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_run_jobs == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].cur_sched_jobs == 2" <query.json &&
	jq -e \
		".mf_priority_map[] |
		 select(.userid == 50001) |
		 .banks[0].held_jobs | length == 0" <query.json
'

test_expect_success 'cancel jobs' '
	flux cancel ${pbatch_job1} ${pbatch_job2} ${pdebug_job3} ${pbatch_job3}
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
