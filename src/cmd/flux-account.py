###############################################################
# Copyright 2020 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import argparse
import sys
import json

import fluxacct.accounting
from fluxacct.accounting import user_subcommands as u
from fluxacct.accounting import bank_subcommands as b
from fluxacct.accounting import job_archive_interface as jobs
from fluxacct.accounting import create_db as c
from fluxacct.accounting import queue_subcommands as qu
from fluxacct.accounting import project_subcommands as p


def add_path_arg(parser):
    parser.add_argument(
        "-p", "--path", dest="path", help="specify location of database file"
    )


def add_output_file_arg(parser):
    parser.add_argument(
        "-o",
        "--output-file",
        dest="output_file",
        help="specify location of output file",
    )


def add_view_user_arg(subparsers):
    subparser_view_user = subparsers.add_parser(
        "view-user",
        help="view a user's information in the accounting database",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_view_user.set_defaults(func="view_user")
    subparser_view_user.add_argument("username", help="username", metavar=("USERNAME"))


def add_add_user_arg(subparsers):
    subparser_add_user = subparsers.add_parser(
        "add-user",
        help="add a user to the accounting database",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_add_user.set_defaults(func="add_user")
    subparser_add_user.add_argument(
        "--username",
        help="username",
        metavar="USERNAME",
    )
    subparser_add_user.add_argument(
        "--userid",
        help="userid",
        default=65534,
        metavar="USERID",
    )
    subparser_add_user.add_argument(
        "--bank",
        help="bank to charge jobs against",
        metavar="BANK",
    )
    subparser_add_user.add_argument(
        "--shares",
        help="shares",
        default=1,
        metavar="SHARES",
    )
    subparser_add_user.add_argument(
        "--max-running-jobs",
        help="max number of jobs that can be running at the same time",
        default=5,
        metavar="MAX_RUNNING_JOBS",
    )
    subparser_add_user.add_argument(
        "--max-active-jobs",
        help="max number of both pending and running jobs",
        default=7,
        metavar="max_active_jobs",
    )
    subparser_add_user.add_argument(
        "--max-nodes",
        help="max number of nodes a user can have across all of their running jobs",
        default=2147483647,
        metavar="MAX_NODES",
    )
    subparser_add_user.add_argument(
        "--queues",
        help="queues the user is allowed to run jobs in",
        default="",
        metavar="QUEUES",
    )
    subparser_add_user.add_argument(
        "--projects",
        help="projects the user is allowed to submit jobs under",
        default="*",
        metavar="PROJECTS",
    )


def add_delete_user_arg(subparsers):
    subparser_delete_user = subparsers.add_parser(
        "delete-user",
        help="remove a user from the accounting database",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_delete_user.set_defaults(func="delete_user")
    subparser_delete_user.add_argument(
        "username", help="username", metavar=("USERNAME")
    )
    subparser_delete_user.add_argument("bank", help="bank", metavar=("BANK"))


def add_edit_user_arg(subparsers):
    subparser_edit_user = subparsers.add_parser(
        "edit-user",
        help="edit a user's value",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_edit_user.set_defaults(func="edit_user")
    subparser_edit_user.add_argument(
        "username",
        help="username",
        metavar="USERNAME",
    )
    subparser_edit_user.add_argument(
        "--bank",
        help="bank to charge jobs against",
        default=None,
        metavar="BANK",
    )
    subparser_edit_user.add_argument(
        "--default-bank",
        help="default bank",
        default=None,
        metavar="DEFAULT_BANK",
    )
    subparser_edit_user.add_argument(
        "--shares",
        help="shares",
        default=None,
        metavar="SHARES",
    )
    subparser_edit_user.add_argument(
        "--max-running-jobs",
        help="max number of jobs that can be running at the same time",
        default=None,
        metavar="MAX_RUNNING_JOBS",
    )
    subparser_edit_user.add_argument(
        "--max-active-jobs",
        help="max number of both pending and running jobs",
        default=7,
        metavar="max_active_jobs",
    )
    subparser_edit_user.add_argument(
        "--max-nodes",
        help="max number of nodes a user can have across all of their running jobs",
        default=5,
        metavar="MAX_NODES",
    )
    subparser_edit_user.add_argument(
        "--queues",
        help="queues the user is allowed to run jobs in",
        default=None,
        metavar="QUEUES",
    )
    subparser_edit_user.add_argument(
        "--projects",
        help="projects the user is allowed to submit jobs under",
        default=None,
        metavar="PROJECTS",
    )
    subparser_edit_user.add_argument(
        "--default-project",
        help="default projects the user submits jobs under when no project is specified",
        default=None,
        metavar="DEFAULT_PROJECT",
    )


def add_view_job_records_arg(subparsers):
    subparser_view_job_records = subparsers.add_parser(
        "view-job-records",
        help="view job records",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_view_job_records.set_defaults(func="view_job_records")
    subparser_view_job_records.add_argument(
        "-u",
        "--user",
        help="username",
        metavar="USERNAME",
    )
    subparser_view_job_records.add_argument(
        "-j", "--jobid", help="jobid", metavar="JOBID"
    )
    subparser_view_job_records.add_argument(
        "-a",
        "--after-start-time",
        help="start time",
        metavar="START TIME",
    )
    subparser_view_job_records.add_argument(
        "-b",
        "--before-end-time",
        help="end time",
        metavar="END TIME",
    )


def add_create_db_arg(subparsers):
    subparser_create_db = subparsers.add_parser(
        "create-db",
        help="create the flux-accounting database",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_create_db.set_defaults(func="create_db")
    subparser_create_db.add_argument(
        "--priority-usage-reset-period",
        help="the number of weeks at which usage information gets reset to 0",
        metavar=("PRIORITY USAGE RESET PERIOD"),
    )
    subparser_create_db.add_argument(
        "--priority-decay-half-life",
        help="contribution of historical usage in weeks on the composite usage value",
        metavar=("PRIORITY DECAY HALF LIFE"),
    )


def add_add_bank_arg(subparsers):
    subparser_add_bank = subparsers.add_parser(
        "add-bank", help="add a new bank", formatter_class=flux.util.help_formatter()
    )
    subparser_add_bank.set_defaults(func="add_bank")
    subparser_add_bank.add_argument(
        "bank",
        help="bank name",
        metavar="BANK",
    )
    subparser_add_bank.add_argument(
        "--parent-bank", help="parent bank name", default="", metavar="PARENT BANK"
    )
    subparser_add_bank.add_argument(
        "shares", help="number of shares to allocate to bank", metavar="SHARES"
    )


def add_view_bank_arg(subparsers):
    subparser_view_bank = subparsers.add_parser(
        "view-bank",
        help="view bank information",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_view_bank.set_defaults(func="view_bank")
    subparser_view_bank.add_argument(
        "bank",
        help="bank name",
        metavar="BANK",
    )
    subparser_view_bank.add_argument(
        "-u",
        "--users",
        action="store_const",
        const=True,
        help="list all potential users under bank",
        metavar="USERS",
    )


def add_delete_bank_arg(subparsers):
    subparser_delete_bank = subparsers.add_parser(
        "delete-bank", help="remove a bank", formatter_class=flux.util.help_formatter()
    )
    subparser_delete_bank.set_defaults(func="delete_bank")
    subparser_delete_bank.add_argument(
        "bank",
        help="bank name",
        metavar="BANK",
    )


def add_edit_bank_arg(subparsers):
    subparser_edit_bank = subparsers.add_parser(
        "edit-bank",
        help="edit a bank's allocation",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_edit_bank.set_defaults(func="edit_bank")
    subparser_edit_bank.add_argument(
        "bank",
        help="bank",
        metavar="BANK",
    )
    subparser_edit_bank.add_argument(
        "--shares",
        help="new shares value",
        metavar="SHARES",
    )
    subparser_edit_bank.add_argument(
        "--parent-bank",
        help="parent bank",
        metavar="PARENT BANK",
    )


def add_update_usage_arg(subparsers):
    subparser_update_usage = subparsers.add_parser(
        "update-usage",
        help="update usage factors for associations",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_update_usage.set_defaults(func="update_usage")
    subparser_update_usage.add_argument(
        "job_archive_db_path",
        help="job-archive DB location",
        metavar="JOB-ARCHIVE_DB_PATH",
    )
    subparser_update_usage.add_argument(
        "--priority-decay-half-life",
        default=1,
        type=int,
        help="number of weeks for a job's usage contribution to a half-life decay",
        metavar="PRIORITY DECAY HALF LIFE",
    )


def add_add_queue_arg(subparsers):
    subparser_add_queue = subparsers.add_parser(
        "add-queue", help="add a new queue", formatter_class=flux.util.help_formatter()
    )

    subparser_add_queue.set_defaults(func="add_queue")
    subparser_add_queue.add_argument("queue", help="queue name", metavar="QUEUE")
    subparser_add_queue.add_argument(
        "--min-nodes-per-job",
        help="min nodes per job",
        default=1,
        metavar="MIN NODES PER JOB",
    )
    subparser_add_queue.add_argument(
        "--max-nodes-per-job",
        help="max nodes per job",
        default=1,
        metavar="MAX NODES PER JOB",
    )
    subparser_add_queue.add_argument(
        "--max-time-per-job",
        help="max time per job",
        default=60,
        metavar="MAX TIME PER JOB",
    )
    subparser_add_queue.add_argument(
        "--priority",
        help="associated priority for the queue",
        default=0,
        metavar="PRIORITY",
    )


def add_view_queue_arg(subparsers):
    subparser_view_queue = subparsers.add_parser(
        "view-queue",
        help="view queue information",
        formatter_class=flux.util.help_formatter(),
    )

    subparser_view_queue.set_defaults(func="view_queue")
    subparser_view_queue.add_argument("queue", help="queue name", metavar="QUEUE")


def add_edit_queue_arg(subparsers):
    subparser_edit_queue = subparsers.add_parser(
        "edit-queue",
        help="edit a queue's priority",
        formatter_class=flux.util.help_formatter(),
    )

    subparser_edit_queue.set_defaults(func="edit_queue")
    subparser_edit_queue.add_argument("queue", help="queue name", metavar="QUEUE")
    subparser_edit_queue.add_argument(
        "--min-nodes-per-job",
        type=int,
        help="min nodes per job",
        default=None,
        metavar="MIN NODES PER JOB",
    )
    subparser_edit_queue.add_argument(
        "--max-nodes-per-job",
        type=int,
        help="max nodes per job",
        default=None,
        metavar="MAX NODES PER JOB",
    )
    subparser_edit_queue.add_argument(
        "--max-time-per-job",
        type=int,
        help="max time per job",
        default=None,
        metavar="MAX TIME PER JOB",
    )
    subparser_edit_queue.add_argument(
        "--priority",
        type=int,
        help="associated priority for the queue",
        default=None,
        metavar="PRIORITY",
    )


def add_delete_queue_arg(subparsers):
    subparser_delete_queue = subparsers.add_parser(
        "delete-queue",
        help="remove a queue",
        formatter_class=flux.util.help_formatter(),
    )

    subparser_delete_queue.set_defaults(func="delete_queue")
    subparser_delete_queue.add_argument("queue", help="queue name", metavar="QUEUE")


def add_add_project_arg(subparsers):
    subparser_add_project = subparsers.add_parser(
        "add-project",
        help="add a new project",
        formatter_class=flux.util.help_formatter(),
    )

    subparser_add_project.set_defaults(func="add_project")
    subparser_add_project.add_argument(
        "project", help="project name", metavar="PROJECT"
    )


def add_view_project_arg(subparsers):
    subparser_view_project = subparsers.add_parser(
        "view-project",
        help="view project information",
        formatter_class=flux.util.help_formatter(),
    )

    subparser_view_project.set_defaults(func="view_project")
    subparser_view_project.add_argument(
        "project", help="project name", metavar="PROJECT"
    )


def add_delete_project_arg(subparsers):
    subparser_delete_project = subparsers.add_parser(
        "delete-project",
        help="remove a project",
        formatter_class=flux.util.help_formatter(),
    )

    subparser_delete_project.set_defaults(func="delete_project")
    subparser_delete_project.add_argument(
        "project", help="project name", metavar="PROJECT"
    )


def add_arguments_to_parser(parser, subparsers):
    add_path_arg(parser)
    add_output_file_arg(parser)
    add_view_user_arg(subparsers)
    add_add_user_arg(subparsers)
    add_delete_user_arg(subparsers)
    add_edit_user_arg(subparsers)
    add_view_job_records_arg(subparsers)
    add_create_db_arg(subparsers)
    add_add_bank_arg(subparsers)
    add_view_bank_arg(subparsers)
    add_delete_bank_arg(subparsers)
    add_edit_bank_arg(subparsers)
    add_update_usage_arg(subparsers)
    add_add_queue_arg(subparsers)
    add_view_queue_arg(subparsers)
    add_edit_queue_arg(subparsers)
    add_delete_queue_arg(subparsers)
    add_add_project_arg(subparsers)
    add_view_project_arg(subparsers)
    add_delete_project_arg(subparsers)


def set_db_location(args):
    path = args.path if args.path else fluxacct.accounting.db_path

    return path


def set_output_file(args):
    # set path for output file
    output_file = args.output_file if args.output_file else None

    return output_file


def select_accounting_function(args, conn, output_file, parser):
    return_val = 0
    if args.func == "view_user":
        return_val = u.view_user(conn, args.username)
    elif args.func == "add_user":
        return_val = u.add_user(
            conn,
            args.username,
            args.bank,
            args.userid,
            args.shares,
            args.max_running_jobs,
            args.max_active_jobs,
            args.max_nodes,
            args.queues,
            args.projects,
        )
    elif args.func == "delete_user":
        return_val = u.delete_user(conn, args.username, args.bank)
    elif args.func == "edit_user":
        return_val = u.edit_user(
            conn,
            args.username,
            args.bank,
            args.default_bank,
            args.shares,
            args.max_running_jobs,
            args.max_active_jobs,
            args.max_nodes,
            args.queues,
            args.projects,
            args.default_project,
        )
    elif args.func == "view_job_records":
        return_val = jobs.output_job_records(
            conn,
            output_file,
            jobid=args.jobid,
            user=args.user,
            before_end_time=args.before_end_time,
            after_start_time=args.after_start_time,
        )
    elif args.func == "add_bank":
        return_val = b.add_bank(conn, args.bank, args.shares, args.parent_bank)
    elif args.func == "view_bank":
        return_val = b.view_bank(conn, args.bank, args.users)
    elif args.func == "delete_bank":
        return_val = b.delete_bank(conn, args.bank)
    elif args.func == "edit_bank":
        return_val = b.edit_bank(conn, args.bank, args.shares, args.parent_bank)
    elif args.func == "update_usage":
        jobs_conn = establish_sqlite_connection(args.job_archive_db_path)
        jobs.update_job_usage(conn, jobs_conn, args.priority_decay_half_life)
    elif args.func == "add_queue":
        return_val = qu.add_queue(
            conn,
            args.queue,
            args.min_nodes_per_job,
            args.max_nodes_per_job,
            args.max_time_per_job,
            args.priority,
        )
    elif args.func == "view_queue":
        return_val = qu.view_queue(conn, args.queue)
    elif args.func == "delete_queue":
        return_val = qu.delete_queue(conn, args.queue)
    elif args.func == "edit_queue":
        return_val = qu.edit_queue(
            conn,
            args.queue,
            args.min_nodes_per_job,
            args.max_nodes_per_job,
            args.max_time_per_job,
            args.priority,
        )
    elif args.func == "add_project":
        return_val = p.add_project(conn, args.project)
    elif args.func == "view_project":
        return_val = p.view_project(conn, args.project)
    elif args.func == "delete_project":
        return_val = p.delete_project(conn, args.project)
    else:
        print(parser.print_usage())
        return

    if return_val != 0:
        print(return_val)


def main():

    parser = argparse.ArgumentParser(
        description="""
        Description: Translate command line arguments into
        SQLite instructions for the Flux Accounting Database.
        """
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="subcommand")
    subparsers.required = True

    add_arguments_to_parser(parser, subparsers)
    args = parser.parse_args()

    path = set_db_location(args)

    # if we are creating the DB for the first time, we need
    # to ONLY create the DB and then exit out successfully
    if args.func == "create_db":
        c.create_db(
            path, args.priority_usage_reset_period, args.priority_decay_half_life
        )
        sys.exit(0)

    output_file = set_output_file(args)

    select_accounting_function(args, output_file, parser)


if __name__ == "__main__":
    main()
