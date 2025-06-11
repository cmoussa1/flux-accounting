#!/bin/bash

test_description='test listing job priority breakdowns using flux account jobs'

. `dirname $0`/sharness.sh

MULTI_FACTOR_PRIORITY=${FLUX_BUILD_DIR}/src/plugins/.libs/mf_priority.so
DB_PATH=$(pwd)/FluxAccountingTest.db

mkdir -p config

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

test_expect_success 'load multi-factor priority plugin' '
	flux jobtap load -r .priority-default ${MULTI_FACTOR_PRIORITY}
'

test_expect_success 'check that mf_priority plugin is loaded' '
	flux jobtap list | grep mf_priority
'

# Configure the banks to have drastically different priorities, where
# bank A has the highest priority and bank C has the lowest.
test_expect_success 'add some banks' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 --priority=100 &&
	flux account add-bank --parent-bank=root B 1 --priority=10 &&
	flux account add-bank --parent-bank=root C 1 --priority=1
'

# Configure the queues in flux-accounting to also have drastically different
# priorities, where gold has the highest priority and bronze has the lowest.
test_expect_success 'add some queues to the DB' '
	flux account add-queue bronze --priority=1 &&
	flux account add-queue silver --priority=100 &&
	flux account add-queue gold --priority=1000
'

test_expect_success 'add three different associations' '
    username=$(whoami) &&
	uid=$(id -u) &&
	flux account add-user --username=${username} --userid=${uid} --bank=A --queues=bronze,silver,gold &&
	flux account add-user --username=${username} --userid=${uid} --bank=B --queues=bronze,silver,gold &&
	flux account add-user --username=${username} --userid=${uid} --bank=C --queues=bronze,silver,gold
'

test_expect_success 'edit the associations to have different fairshare values' '
    flux account edit-user ${username} --bank=A --fairshare=0.99 &&
    flux account edit-user ${username} --bank=B --fairshare=0.50 &&
    flux account edit-user ${username} --bank=C --fairshare=0.08
'

test_expect_success 'configure flux with those queues' '
	cat >config/queues.toml <<-EOT &&
	[queues.bronze]
	[queues.silver]
	[queues.gold]
	EOT
	flux config reload
'

test_expect_success 'configure priority plugin with bank factor weight' '
	cat >config/test.toml <<-EOT &&
	[accounting.factor-weights]
	bank = 1000
	EOT
	flux config reload
'

test_expect_success 'send flux-accounting information to the plugin' '
	flux account-priority-update -p ${DB_PATH}
'

test_expect_success 'submit jobs to different banks but the same queue' '
	job1=$(flux submit -S bank=A --queue=bronze sleep 60) &&
	flux job wait-event -vt 5 ${job1} priority &&
    job2=$(flux submit -S bank=B --queue=bronze sleep 60) &&
	flux job wait-event -vt 5 ${job2} priority &&
    job3=$(flux submit -S bank=C --queue=bronze sleep 60) &&
	flux job wait-event -vt 5 ${job3} priority
'

test_expect_success 'passing in a username that is not found in flux-accounting DB fails' '
    test_must_fail flux account jobs foo > error_association.out 2>&1 &&
    grep "could not find entry for foo in association_table" error_association.out
'

test_expect_success 'passing in a queue that is not found in flux-accounting DB fails' '
    test_must_fail flux account jobs ${username} --queue=foo > error_queue.out 2>&1 &&
    grep "could not find entry for foo in queue_table" error_queue.out
'

# By default, we can just specify a username and fetch all of their jobs under
# every bank and every queue.
test_expect_success 'look at flux account jobs default output (will return all jobs for user)' '
    flux account jobs ${username}
'

# We can filter to only return jobs that are running under a certain bank but
# are running under any queue.
test_expect_success 'filter jobs by a specific bank' '
    flux account jobs ${username} --bank=A
'

# We can filter to only return jobs that are running under a certain queue but
# are running under any bank.
test_expect_success 'filter jobs by a specific queue' '
    flux account jobs ${username} --queue=bronze
'

test_expect_success 'submit jobs to different queues but the same bank' '
    job4=$(flux submit -S bank=C --queue=silver sleep 60) &&
	flux job wait-event -vt 5 ${job4} priority &&
    job5=$(flux submit -S bank=C --queue=gold sleep 60) &&
	flux job wait-event -vt 5 ${job5} priority
'

test_expect_success 'filter jobs by a specific queue' '
    flux account jobs ${username} --queue=silver
'

test_expect_success 'filter jobs by a specific queue' '
    flux account jobs ${username} --queue=gold
'

test_expect_success 'cancel jobs' '
    flux cancel ${job1} &&
    flux cancel ${job2} &&
    flux cancel ${job3} &&
    flux cancel ${job4} &&
    flux cancel ${job5}
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
