#!/bin/bash

test_description='test priority plugin reload before accounting data arrives'

. `dirname $0`/sharness.sh

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB_PATH=$(pwd)/FluxAccountingTest.db

mkdir -p config
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 4 job -o,--config-path=$(pwd)/config -Slog-stderr-level=1

reload_without_data () {
	flux jobtap remove mf_priority.so &&
	flux jobtap load ${MULTI_FACTOR_PRIORITY}
}

update_data () {
	flux account-priority-update -p ${DB_PATH} &&
	flux python -c 'import flux; flux.Flux().rpc("job-manager.mf_priority.reprioritize").get()'
}

# Explicitly keep testexec jobs alive until canceled. The command itself is
# not executed for these guest jobs.
submit_job () {
	flux python ${SUBMIT_AS} 50001 -N1 -n1 --queue=debug \
		--setattr=system.exec.test.run_duration=1h "$@" sleep inf
}

wait_clean () {
	flux job wait-event -t 10 "$1" clean
}

no_plugin_exception () {
	flux job eventlog -f json "$1" >events.json &&
	jq -s -e 'all(.[]; .name != "exception" or
		.context.type != "mf_priority")' <events.json
}

check_usage () {
	flux jobtap query mf_priority.so >query.json &&
	jq -e --arg bank "$1" --argjson active "$2" \
		--argjson running "$3" --argjson sched "$4" '
		.mf_priority_map[] | select(.userid == 50001) |
		.banks[] | select(.bank_name == $bank) |
		.cur_active_jobs == $active and
		.cur_run_jobs == $running and .cur_sched_jobs == $sched and
		.cur_nodes == $running and .cur_cores == $running and
		((.queue_usage.debug.cur_run_jobs // 0) == $running) and
		((.queue_usage.debug.cur_nodes // 0) == $running) and
		((.queue_usage.debug.cur_sched_jobs // 0) == $sched) and
		((.queue_usage.debug.cur_sched_nodes // 0) == $running + $sched) and
		((.queue_usage.debug.cur_sched_cores // 0) == $running + $sched)
		' <query.json
}

check_hold () {
	flux job wait-event -t 10 --match-context=priority=0 "$1" priority &&
	flux jobs -no '{state} {priority}' "$1" >held.out &&
	grep '^SCHED 0$' held.out
}

test_expect_success 'configure queues and guest execution' '
	cat >config/queues.toml <<-EOT &&
	[queues.debug]
	[queues.other]
	[exec.testexec]
	allow-guests = true
	EOT
	flux config reload &&
	flux queue start --all
'

test_expect_success 'create accounting data for two banks of one user' '
	flux account -p ${DB_PATH} create-db &&
	flux account-service -p ${DB_PATH} -t &&
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 &&
	flux account add-bank --parent-bank=root B 1 &&
	flux account add-queue debug &&
	flux account add-queue other &&
	flux account add-user --username=user1 --userid=50001 --bank=A \
		--queues=debug,other --max-running-jobs=999 &&
	flux account add-user --username=user1 --userid=50001 --bank=B \
		--queues=debug,other &&
	flux jobtap load -r .priority-default ${MULTI_FACTOR_PRIORITY} &&
	update_data
'

# Start the queue before the data update, unlike the older t1048 test.
test_expect_success 'reload holds ordinary SCHED jobs at priority zero' '
	flux queue stop --all &&
	ordinary=$(submit_job --setattr=system.bank=A) &&
	flux job wait-event -t 10 ${ordinary} priority &&
	reload_without_data &&
	check_hold ${ordinary} &&
	check_usage DNE 1 0 1 &&
	flux queue start --all &&
	test_must_fail flux job wait-event -t 0.2 ${ordinary} alloc
'

test_expect_success 'jobspec update during data gap does not kill the job' '
	flux update ${ordinary} duration=2h &&
	flux job info ${ordinary} jobspec | \
		jq -e ".attributes.system.duration == 7200" &&
	test_must_fail flux update ${ordinary} bank=B >bank-error.out 2>&1 &&
	grep "plugin still waiting on flux-accounting data" bank-error.out &&
	check_usage DNE 1 0 1 &&
	no_plugin_exception ${ordinary}
'

test_expect_success 'data restores priority and transfers charges exactly once' '
	update_data &&
	flux job wait-event -t 10 ${ordinary} start &&
	check_usage A 1 1 0 &&
	check_usage DNE 0 0 0 &&
	update_data &&
	check_usage A 1 1 0 &&
	flux cancel ${ordinary} &&
	wait_clean ${ordinary} &&
	no_plugin_exception ${ordinary} &&
	check_usage A 0 0 0
'

test_expect_success 'new jobs still wait in PRIORITY and cancel without data' '
	reload_without_data &&
	fresh=$(submit_job) &&
	flux job wait-event -t 10 ${fresh} depend &&
	test "$(flux jobs -no {state} ${fresh})" = PRIORITY &&
	check_usage DNE 1 0 0 &&
	flux cancel ${fresh} &&
	wait_clean ${fresh} &&
	no_plugin_exception ${fresh} &&
	check_usage DNE 0 0 0 &&
	update_data &&
	check_usage A 0 0 0
'

test_expect_success 'late data supplies a new job with its default bank' '
	reload_without_data &&
	defaulted=$(submit_job) &&
	flux job wait-event -t 10 ${defaulted} depend &&
	test "$(flux jobs -no {state} ${defaulted})" = PRIORITY &&
	update_data &&
	flux job wait-event -t 10 ${defaulted} start &&
	flux job info ${defaulted} jobspec | \
		jq -e ".attributes.system.bank == \"A\"" &&
	check_usage A 1 1 0 &&
	check_usage DNE 0 0 0 &&
	flux cancel ${defaulted} &&
	wait_clean ${defaulted} &&
	no_plugin_exception ${defaulted} &&
	check_usage A 0 0 0
'

test_expect_success 'cancel a replayed SCHED job before data arrives' '
	flux queue stop --all &&
	canceled=$(submit_job --setattr=system.bank=A) &&
	flux job wait-event -t 10 ${canceled} priority &&
	reload_without_data &&
	check_hold ${canceled} &&
	flux cancel ${canceled} &&
	wait_clean ${canceled} &&
	no_plugin_exception ${canceled} &&
	check_usage DNE 0 0 0 &&
	update_data &&
	check_usage A 0 0 0
'

test_expect_success 'expedite can run during data gap and finish normally' '
	expedited=$(flux python ${SUBMIT_AS} 50001 -N1 -n1 --queue=debug \
		--setattr=system.bank=A --setattr=system.exec.test.run_duration=0.1s \
		true) &&
	flux job wait-event -t 10 ${expedited} priority &&
	flux job urgency ${expedited} 31 &&
	reload_without_data &&
	flux queue start --all &&
	flux job wait-event -t 10 ${expedited} start &&
	wait_clean ${expedited} &&
	flux job eventlog -f json ${expedited} >completed.json &&
	jq -s -e "any(.[]; .name == \"finish\" and .context.status == 0)" \
		<completed.json &&
	no_plugin_exception ${expedited} &&
	check_usage DNE 0 0 0 &&
	update_data &&
	check_usage A 0 0 0
'

test_expect_success 'transfer a job that starts under a temporary association' '
	flux queue stop --all &&
	late_runner=$(submit_job --setattr=system.bank=A) &&
	flux job wait-event -t 10 ${late_runner} priority &&
	flux job urgency ${late_runner} 31 &&
	reload_without_data &&
	flux queue start --all &&
	flux job wait-event -t 10 ${late_runner} start &&
	check_usage DNE 1 1 0 &&
	update_data &&
	check_usage A 1 1 0 &&
	check_usage DNE 0 0 0 &&
	update_data &&
	check_usage A 1 1 0 &&
	flux cancel ${late_runner} &&
	wait_clean ${late_runner} &&
	no_plugin_exception ${late_runner} &&
	check_usage A 0 0 0
'

test_expect_success 'replay reconstructs multiple runs and SCHED usage' '
	running_a=$(submit_job --setattr=system.bank=A) &&
	running_b=$(submit_job --setattr=system.bank=B) &&
	flux job wait-event -t 10 ${running_a} start &&
	flux job wait-event -t 10 ${running_b} start &&
	flux queue stop --all &&
	pending=$(submit_job --setattr=system.bank=A) &&
	flux job wait-event -t 10 ${pending} priority &&
	reload_without_data &&
	check_hold ${pending} &&
	check_usage DNE 3 2 1 &&
	update_data &&
	check_usage DNE 0 0 0 &&
	check_usage A 2 1 1 &&
	check_usage B 1 1 0 &&
	update_data &&
	check_usage A 2 1 1 &&
	check_usage B 1 1 0
'

test_expect_success 'recorded banks survive default-bank changes on reload' '
	flux account edit-user user1 --default-bank=B &&
	reload_without_data &&
	update_data &&
	flux job info ${pending} jobspec | \
		jq -e ".attributes.system.bank == \"A\"" &&
	flux job info ${running_a} jobspec | \
		jq -e ".attributes.system.bank == \"A\"" &&
	check_usage A 2 1 1 &&
	check_usage B 1 1 0
'

test_expect_success 'queue and bank updates move reconciled SCHED charges' '
	flux update ${pending} queue=other &&
	flux jobtap query mf_priority.so >moved.json &&
	jq -e ".mf_priority_map[] | select(.userid == 50001) |
		.banks[] | select(.bank_name == \"A\") |
		.queue_usage.other.cur_sched_nodes == 1 and
		.queue_usage.debug.cur_sched_nodes == 1" <moved.json &&
	flux update ${pending} queue=debug bank=B &&
	check_usage A 1 1 0 &&
	check_usage B 2 1 1 &&
	flux cancel ${pending} ${running_a} ${running_b} &&
	wait_clean ${pending} &&
	wait_clean ${running_a} &&
	wait_clean ${running_b} &&
	no_plugin_exception ${pending} &&
	no_plugin_exception ${running_a} &&
	no_plugin_exception ${running_b} &&
	check_usage A 0 0 0 &&
	check_usage B 0 0 0
'

test_expect_success 'a running job remains charged temporarily if its bank is absent' '
	flux queue start --all &&
	missing=$(submit_job --setattr=system.bank=B) &&
	flux job wait-event -t 10 ${missing} start &&
	flux account export-json >full.json &&
	cat >send_subset.py <<-PY &&
	import json
	import flux

	with open("full.json") as stream:
	    data = json.load(stream)
	associations = [a for a in data["associations"] if a["bank"] != "B"]
	handle = flux.Flux()
	handle.rpc("job-manager.mf_priority.rec_update", {"data": associations}).get()
	handle.rpc("job-manager.mf_priority.reprioritize").get()
	PY
	reload_without_data &&
	flux python send_subset.py &&
	flux python send_subset.py &&
	check_usage DNE 1 1 0 &&
	test "$(flux jobs -no {state} ${missing})" = RUN &&
	flux dmesg >missing.log &&
	test $(grep -c "job $(flux job id --to=dec ${missing}): association unavailable" missing.log) = 1
'

test_expect_success 'a later update resolves the running job without changing jobspec' '
	update_data &&
	check_usage DNE 0 0 0 &&
	check_usage B 1 1 0 &&
	flux job info ${missing} jobspec | \
		jq -e ".attributes.system.bank == \"B\"" &&
	flux cancel ${missing} &&
	wait_clean ${missing} &&
	no_plugin_exception ${missing} &&
	check_usage B 0 0 0
'

test_expect_success 'restored running usage enforces limits and releases held work' '
	flux account edit-user user1 --bank=A --max-running-jobs=1 &&
	update_data &&
	limited_run=$(submit_job --setattr=system.bank=A) &&
	flux job wait-event -t 10 ${limited_run} start &&
	reload_without_data &&
	update_data &&
	limited_wait=$(submit_job --setattr=system.bank=A) &&
	flux job wait-event -t 10 \
		--match-context=description=max-running-jobs-user-limit \
		${limited_wait} dependency-add &&
	check_usage A 2 1 0 &&
	flux cancel ${limited_run} &&
	wait_clean ${limited_run} &&
	flux job wait-event -t 10 ${limited_wait} start &&
	check_usage A 1 1 0 &&
	flux cancel ${limited_wait} &&
	wait_clean ${limited_wait} &&
	no_plugin_exception ${limited_run} &&
	no_plugin_exception ${limited_wait} &&
	check_usage A 0 0 0
'

test_expect_success 'urgency holds survive accounting-data restoration' '
	flux queue stop --all &&
	held=$(submit_job --setattr=system.bank=B) &&
	flux job wait-event -t 10 ${held} priority &&
	flux job urgency ${held} 0 &&
	reload_without_data &&
	update_data &&
	flux queue start --all &&
	check_hold ${held} &&
	test_must_fail flux job wait-event -t 0.2 ${held} alloc &&
	flux cancel ${held} &&
	wait_clean ${held} &&
	no_plugin_exception ${held} &&
	check_usage B 0 0 0
'

test_expect_success 'reload never reports an unavailable SCHED priority' '
	flux dmesg >broker.log &&
	test_must_fail grep "BUG: plugin didn.t return priority" broker.log
'

test_expect_success 'shut down accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
