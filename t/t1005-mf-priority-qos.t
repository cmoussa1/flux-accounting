#!/bin/bash

test_description='Test multi-factor priority plugin with a single user'

. `dirname $0`/sharness.sh
MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job

flux setattr log-stderr-level 1

test_expect_success 'load multi-factor priority plugin' '
	flux jobtap load -r .priority-default ${MULTI_FACTOR_PRIORITY}
'

test_expect_success 'check that mf_priority plugin is loaded' '
	flux jobtap list | grep mf_priority
'

test_expect_success 'create fake_payload.py' '
	cat <<-EOF >fake_payload.py
	import flux
	import pwd
	import getpass

	username = getpass.getuser()
	userid = pwd.getpwnam(username).pw_uid
	# create a JSON payload
	data = {"userid": str(userid), "bank": "account3", "default_bank": "account3", "fairshare": "0.45321", "max_jobs": "10", "qos": "standby,bronze,silver,gold,expedite"}
	flux.Flux().rpc("job-manager.mf_priority.get_users", data).get()
	data = {"userid": str(userid), "bank": "account2", "default_bank": "account3", "fairshare": "0.11345", "max_jobs": "10", "qos": "standby"}
	flux.Flux().rpc("job-manager.mf_priority.get_users", data).get()
	data = {"qos": "standby", "priority": "-1000"}
	flux.Flux().rpc("job-manager.mf_priority.get_qos", data).get()
	data = {"qos": "bronze", "priority": "20"}
	flux.Flux().rpc("job-manager.mf_priority.get_qos", data).get()
	data = {"qos": "silver", "priority": "30"}
	flux.Flux().rpc("job-manager.mf_priority.get_qos", data).get()
	data = {"qos": "gold", "priority": "40"}
	flux.Flux().rpc("job-manager.mf_priority.get_qos", data).get()
	data = {"qos": "expedite", "priority": "1000"}
	flux.Flux().rpc("job-manager.mf_priority.get_qos", data).get()
	EOF
'

test_expect_success 'update plugin with sample test data' '
	flux python fake_payload.py
'

test_expect_success 'stop the queue' '
	flux queue stop
'

test_expect_success 'submit a job using a QoS the user does not belong to' '
	test_must_fail flux mini submit --setattr=system.bank=account2 --setattr=system.qos=expedite -n1 hostname > unavail_qos.out 2>&1 &&
	test_debug "unavail_qos.out" &&
	grep "QoS not valid for user" unavail_qos.out
'

test_expect_success 'submit a job using a nonexistent QoS' '
	test_must_fail flux mini submit --setattr=system.qos=foo -n1 hostname > bad_qos.out 2>&1 &&
	test_debug "bad_qos.out" &&
	grep "QoS does not exist" bad_qos.out
'

test_expect_success 'submit a job using standby QoS, which should decrease job priority' '
	jobid=$(flux mini submit --job-name=standby --setattr=system.bank=account3 --setattr=system.qos=standby -n1 hostname) &&
	flux job wait-event -f json $jobid priority | jq '.context.priority' > job7.test &&
	cat <<-EOF >job7.expected &&
	0
	EOF
	test_cmp job7.expected job7.test
'

test_expect_success 'submit a job using expedite QoS, which should increase priority' '
	jobid=$(flux mini submit --job-name=expedite --setattr=system.bank=account3 --setattr=system.qos=expedite -n1 hostname) &&
	flux job wait-event -f json $jobid priority | jq '.context.priority' > job8.test &&
	cat <<-EOF >job8.expected &&
	1045321
	EOF
	test_cmp job8.expected job8.test
'

test_expect_success 'submit a job using the rest of the available QoS' '
	flux mini submit --job-name=bronze --setattr=system.bank=account3 --setattr=system.qos=bronze -n1 hostname &&
	flux job wait-event -f json $jobid priority | jq '.context.priority' &&
	flux mini submit --job-name=silver --setattr=system.bank=account3 --setattr=system.qos=silver -n1 hostname &&
	flux job wait-event -f json $jobid priority | jq '.context.priority' &&
	flux mini submit --job-name=gold --setattr=system.bank=account3 --setattr=system.qos=gold -n1 hostname &&
	flux job wait-event -f json $jobid priority | jq '.context.priority'
'


test_expect_success 'check order of job queue' '
	flux jobs --suppress-header --format={name} > multi_qos.test &&
	cat <<-EOF >multi_qos.expected &&
	expedite
	gold
	silver
	bronze
	standby
	EOF
	test_cmp multi_qos.expected multi_qos.test
'

test_done