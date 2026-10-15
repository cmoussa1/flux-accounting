#!/bin/bash

test_description='test estimating job priority without submitting a job'

. `dirname $0`/sharness.sh

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
SUBMIT_AS=${SHARNESS_TEST_SRCDIR}/scripts/submit_as.py
DB_PATH=$(pwd)/FluxAccountingTest.db
username=user1
userid=50001
caller=$(id -un)
callerid=$(id -u)

mkdir -p config

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job -o,--config-path=$(pwd)/config -Slog-stderr-level=1

test_expect_success 'allow guest access to testexec' '
	flux config load <<-EOF
	[exec.testexec]
	allow-guests = true
	EOF
'

test_expect_success 'create flux-accounting DB and service' '
	flux account -p ${DB_PATH} create-db &&
	flux account-service -p ${DB_PATH} -t
'

test_expect_success 'add banks, queues, and associations' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 --priority=2 &&
	flux account add-bank --parent-bank=root B 1 --priority=4 &&
	flux account add-queue pdebug --priority=7 &&
	flux account add-queue pbatch --priority=3 &&
	flux account add-user \
		--username=${username} \
		--userid=${userid} \
		--bank=A \
		--fairshare=0.5 \
		--queues=pdebug &&
	flux account add-user \
		--username=${username} \
		--userid=${userid} \
		--bank=B \
		--fairshare=0.25 \
		--queues=pbatch &&
	flux account add-user \
		--username=${caller} \
		--userid=${callerid} \
		--bank=B \
		--fairshare=0.25 \
		--queues=pbatch
'

test_expect_success 'configure queues and load priority plugin' '
	cat >config/queues.toml <<-EOT &&
	[queues.pdebug]
	[queues.pbatch]
	EOT
	flux config reload &&
	flux queue start --all &&
	flux jobtap load -r .priority-default \
		${MULTI_FACTOR_PRIORITY} "config=$(flux account export-json)"
'

test_expect_success 'default output includes notes' '
	flux account estimate-priority ${username} --bank=A --queue=pdebug \
		> estimate.table &&
	grep "120000" estimate.table &&
	grep "Notes:" estimate.table &&
	grep "limits are not evaluated" estimate.table
'

test_expect_success 'JSON output omits prose notes' '
	flux account estimate-priority ${username} \
		--bank=A --queue=pdebug --json > estimate.json &&
	jq -e ".[] |
		select(.username == \"${username}\") |
		.bank == \"A\" and
		.priority == 120000 and
		.limits_evaluated == false" <estimate.json &&
	! grep "Notes:" estimate.json
'

test_expect_success 'custom format takes precedence over JSON' '
	flux account estimate-priority ${username} \
		--bank=A --queue=pdebug --json \
		-o "{username}:{bank}:{priority}" > estimate.format &&
	grep "username:bank:priority" estimate.format &&
	grep "${username}:A:120000" estimate.format &&
	! grep "Notes:" estimate.format
'

test_expect_success 'omitted username defaults to caller account' '
	flux account estimate-priority --bank=B --queue=pbatch --json \
		> estimate.caller.json &&
	jq -e ".[] |
		select(.username == \"${caller}\") |
		.bank == \"B\"" <estimate.caller.json
'

test_expect_success 'rejected single-bank estimate fails' '
	test_must_fail flux account estimate-priority \
		${username} --bank=B --queue=pdebug > reject.out 2>&1 &&
	grep "not valid for user" reject.out
'

test_expect_success '--all-banks includes rejected rows and succeeds' '
	flux account estimate-priority ${username} \
		--all-banks --queue=pdebug --json > all-banks.json &&
	jq -e ". | length == 2" <all-banks.json &&
	jq -e ".[] | select(.bank == \"A\") | .accepted == true" \
		<all-banks.json &&
	jq -e ".[] |
		select(.bank == \"B\") |
		.accepted == false and
		(.rejection_reason | test(\"not valid for user\"))" \
		<all-banks.json
'

test_expect_success 'non-owner urgency above 16 is rejected' '
	test_must_fail env FLUX_HANDLE_ROLEMASK=0x2 \
		flux account estimate-priority ${username} \
		--bank=A --queue=pdebug --urgency=17 > urgency.err 2>&1 &&
	grep "estimate-priority: only the instance owner can submit with urgency >16" \
		urgency.err
'

test_expect_success 'estimator matches plugin priority before factor update' '
	flux account estimate-priority ${username} \
		--bank=A --queue=pdebug --json |
		jq ".[0].priority" > estimate.before &&
	job=$(flux python ${SUBMIT_AS} ${userid} \
		--setattr=system.bank=A \
		--queue=pdebug \
		--urgency=16 \
		sleep inf) &&
	flux job wait-event -vt 5 -f json ${job} priority |
		jq "select(.context.priority != null) | .context.priority" \
			> plugin.before &&
	test_cmp estimate.before plugin.before &&
	flux cancel ${job}
'

test_expect_success 'estimator matches plugin priority after factor update' '
	flux account edit-factor --factor=fairshare --weight=10 &&
	flux account-priority-update -p ${DB_PATH} &&
	flux account estimate-priority ${username} \
		--bank=A --queue=pdebug --json |
		jq ".[0].priority" > estimate.after &&
	job=$(flux python ${SUBMIT_AS} ${userid} \
		--setattr=system.bank=A \
		--queue=pdebug \
		--urgency=16 \
		sleep inf) &&
	flux job wait-event -vt 5 -f json ${job} priority |
		jq "select(.context.priority != null) | .context.priority" \
			> plugin.after &&
	test_cmp estimate.after plugin.after &&
	flux cancel ${job}
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
