#!/bin/bash

test_description='test exporting flux-accounting database data into .csv files'
. `dirname $0`/sharness.sh

DB_PATHv1=$(pwd)/FluxAccountingTestv1.db
DB_PATHv2=$(pwd)/FluxAccountingTestv2.db
EXPECTED_FILES=${SHARNESS_TEST_SRCDIR}/expected/pop_db

export TEST_UNDER_FLUX_NO_JOB_EXEC=y
export TEST_UNDER_FLUX_SCHED_SIMPLE_MODE="limited=1"
test_under_flux 1 job

flux setattr log-stderr-level 1

test_expect_success 'create flux-accounting DB' '
	flux account -p $(pwd)/FluxAccountingTestv1.db create-db
'

test_expect_success 'start flux-accounting service' '
	flux account-service -p ${DB_PATHv1} -t
'

test_expect_success 'add some banks to the DB' '
	flux account add-bank root 1 &&
	flux account add-bank --parent-bank=root A 1 &&
	flux account add-bank --parent-bank=root B 1 &&
	flux account add-bank --parent-bank=root C 1 &&
	flux account add-bank --parent-bank=root D 1 &&
	flux account add-bank --parent-bank=D E 1
	flux account add-bank --parent-bank=D F 1
'

test_expect_success 'add some users to the DB' '
	flux account add-user --username=user5011 --userid=5011 --bank=A &&
	flux account add-user --username=user5012 --userid=5012 --bank=A &&
	flux account add-user --username=user5013 --userid=5013 --bank=B &&
	flux account add-user --username=user5014 --userid=5014 --bank=C
'

# we only use custom formatting here because the creation_time and mod_time
# fields will be different from every run (since they are timestamps)
test_expect_success 'export bank DB information into .csv files' '
	flux account export-db \
		--banks=banks_test.csv \
		--users=users_test.csv \
		--bank-fields=bank,parent_bank,shares \
		--user-fields=username,userid,bank,shares,max_running_jobs,max_active_jobs,max_nodes,queues
'

test_expect_success 'compare banks.csv' '
	cat <<-EOF >banks_expected.csv
	bank,parent_bank,shares
	root,,1
	A,root,1
	B,root,1
	C,root,1
	D,root,1
	E,D,1
	F,D,1
	EOF
	test_cmp -b banks_expected.csv banks_test.csv
'

test_expect_success 'compare users.csv' '
	cat <<-EOF >users_expected.csv
	username,userid,bank,shares,max_running_jobs,max_active_jobs,max_nodes,queues
	user5011,5011,A,1,5,7,2147483647,
	user5012,5012,A,1,5,7,2147483647,
	user5013,5013,B,1,5,7,2147483647,
	user5014,5014,C,1,5,7,2147483647,
	EOF
	test_cmp -b users_expected.csv users_test.csv
'

test_expect_success 'export custom bank DB information' '
	flux account export-db --banks=banks_test_custom.csv --bank-fields=bank_id,bank &&
	cat <<-EOF >banks_expected_custom.csv
	bank_id,bank
	1,root
	2,A
	3,B
	4,C
	5,D
	6,E
	7,F
	EOF
	test_cmp -b banks_expected_custom.csv banks_test_custom.csv
'

test_expect_success 'create hierarchy output of the flux-accounting DB and store it in a file' '
	flux account view-bank root -t > db1.test
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_expect_success 'create a new flux-accounting DB' '
	flux account -p $(pwd)/FluxAccountingTestv2.db create-db
'

test_expect_success 'restart service against new DB' '
	flux account-service -p ${DB_PATHv2} -t
'

test_expect_success 'import information into new DB' '
	flux account pop-db -b banks_test.csv &&
	flux account pop-db -u users_test.csv
'

test_expect_success 'create hierarchy output of the new DB and store it in a file' '
	flux account view-bank root -t > db2.test
'

test_expect_success 'compare DB hierarchies to make sure they are the same' '
	test_cmp db1.test db2.test
'

test_expect_success 'specify a different filename for exported users and banks .csv files' '
	flux account export-db \
		--banks=bar.csv \
		--users=foo.csv \
		--bank-fields=bank,parent_bank,shares \
		--user-fields=username,userid,bank,shares,max_running_jobs,max_active_jobs,max_nodes,queues &&
	test_cmp -b users_expected.csv foo.csv &&
	test_cmp -b banks_expected.csv bar.csv
'

test_expect_success 'shut down flux-accounting service' '
	flux python -c "import flux; flux.Flux().rpc(\"accounting.shutdown_service\").get()"
'

test_done
