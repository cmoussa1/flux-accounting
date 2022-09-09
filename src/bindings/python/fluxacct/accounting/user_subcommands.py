#!/usr/bin/env python3

###############################################################
# Copyright 2020 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import sqlite3
import time
import pwd


def get_uid(username):
    try:
        return pwd.getpwnam(username).pw_uid
    except KeyError:
        return str(username)


def validate_queue(conn, queue):
    cur = conn.cursor()
    queue_list = queue.split(",")

    for service in queue_list:
        cur.execute("SELECT queue FROM queue_table WHERE queue=?", (service,))
        row = cur.fetchone()
        if row is None:
            raise ValueError("Queue specified does not exist in queue_table")


def validate_project(conn, projects):
    cur = conn.cursor()
    project_list = projects.split(",")
    project_list.append("*")

    for project in project_list:
        cur.execute("SELECT project FROM project_table WHERE project=?", (project,))
        row = cur.fetchone()
        if row is None:
            raise ValueError('Project "%s" does not exist in project_table' % project)

    return ",".join(project_list)


def set_uid(username, uid):

    if uid == 65534:
        fetched_uid = get_uid(username)

        try:
            if isinstance(fetched_uid, int):
                uid = fetched_uid
            else:
                raise KeyError
        except KeyError:
            print("could not find UID for user; adding default UID")
            uid = 65534

    return uid


def print_user_rows(headers, rows):
    # find length of longest column name
    col_width = len(sorted(headers, key=len)[-1])

    for header in headers:
        print(header.ljust(col_width), end=" ")
    print()
    for row in rows:
        for col in list(row):
            print(str(col).ljust(col_width), end=" ")
        print()


# check for a default bank of the user being added; if the user is new, set
# the first bank they were added to as their default bank
def set_default_bank(cur, username, bank):
    select_stmt = "SELECT default_bank FROM association_table WHERE username=?"
    cur.execute(select_stmt, (username,))
    row = cur.fetchone()

    if row is None:
        return bank

    return row[0]


# check if user/bank entry already exists but was disabled first; if so,
# just update the 'active' column in already existing row
def check_if_user_disabled(conn, cur, username, bank):
    cur.execute(
        "SELECT * FROM association_table WHERE username=? AND bank=?",
        (
            username,
            bank,
        ),
    )
    rows = cur.fetchall()
    if len(rows) == 1:
        cur.execute(
            "UPDATE association_table SET active=1 WHERE username=? AND bank=?",
            (
                username,
                bank,
            ),
        )
        conn.commit()
        return True

    return False


def view_user(conn, user):
    cur = conn.cursor()
    try:
        # get the information pertaining to a user in the DB
        cur.execute("SELECT * FROM association_table where username=?", (user,))
        rows = cur.fetchall()
        headers = [description[0] for description in cur.description]  # column names
        if not rows:
            print("User not found in association_table")
        else:
            print_user_rows(headers, rows)
    except sqlite3.OperationalError as e_database_error:
        print(e_database_error)


def add_user(
    conn,
    username,
    bank,
    uid=65534,
    shares=1,
    max_running_jobs=5,
    max_active_jobs=7,
    max_nodes=2147483647,
    queues="",
    projects="*",
):
    cur = conn.cursor()

    userid = set_uid(username, uid)

    # set default bank for user
    default_bank = set_default_bank(cur, username, bank)

    # validate the queue specified if any were passed in
    if queues != "":
        try:
            validate_queue(conn, queues)
        except ValueError as err:
            print(err)
            return -1

    # if True, we don't need to execute an add statement, so just return
    if check_if_user_disabled(conn, cur, username, bank):
        return 0

    # validate the project(s) specified if any were passed in;
    # add default project name ('*') to project(s) specified if
    # any were passed in
    #
    # determine default_project for user; if no other projects
    # were specified, use '*' as the default. If a project was
    # specified, then use the first one as the default
    if projects != "*":
        try:
            projects = validate_project(conn, projects)
        except ValueError as err:
            print(err)
            return -1

        project_list = projects.split(",")
        default_project = project_list[0]
    else:
        default_project = "*"

    try:
        # insert the user values into association_table
        conn.execute(
            """
            INSERT INTO association_table (creation_time, mod_time, username,
                                           userid, bank, default_bank, shares,
                                           max_running_jobs, max_active_jobs,
                                           max_nodes, queues, projects, default_project)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(time.time()),
                int(time.time()),
                username,
                userid,
                bank,
                default_bank,
                shares,
                max_running_jobs,
                max_active_jobs,
                max_nodes,
                queues,
                projects,
                default_project,
            ),
        )
        # commit changes
        conn.commit()
        # insert the user values into job_usage_factor_table
        conn.execute(
            """
            INSERT OR IGNORE INTO job_usage_factor_table (username, userid, bank)
            VALUES (?, ?, ?)
            """,
            (
                username,
                uid,
                bank,
            ),
        )
        conn.commit()
    # make sure entry is unique
    except sqlite3.IntegrityError as integrity_error:
        print(integrity_error)
        return -1

    return 0


def delete_user(conn, username, bank):
    # set deleted flag in user row
    update_stmt = "UPDATE association_table SET active=0 WHERE username=? AND bank=?"
    conn.execute(
        update_stmt,
        (
            username,
            bank,
        ),
    )
    # commit changes
    conn.commit()

    # check if bank being deleted is the user's default bank
    cur = conn.cursor()
    select_stmt = "SELECT default_bank FROM association_table WHERE username=?"
    cur.execute(select_stmt, (username,))
    rows = cur.fetchall()
    default_bank = rows[0][0]

    if default_bank == bank:
        # get first bank from other potential existing rows from user
        select_stmt = """SELECT bank FROM association_table WHERE active=1 AND username=?
                         ORDER BY creation_time"""
        cur.execute(select_stmt, (username,))
        rows = cur.fetchall()
        # if len(rows) == 0, then the user only belongs to one bank (the bank they are being
        # disabled in); thus the user's default bank does not need to be updated
        if len(rows) > 0:
            # update user rows to have a new default bank (the next earliest user/bank row created)
            new_default_bank = rows[0][0]
            edit_user(conn, username, default_bank=new_default_bank)


def edit_user(
    conn,
    username,
    bank=None,
    default_bank=None,
    shares=None,
    max_running_jobs=None,
    max_active_jobs=None,
    max_nodes=None,
    queues=None,
    projects=None,
    default_project=None,
):
    params = locals()
    editable_fields = [
        "username",
        "bank",
        "default_bank",
        "shares",
        "max_running_jobs",
        "max_active_jobs",
        "max_nodes",
        "queues",
        "projects",
        "default_project",
    ]
    for field in editable_fields:
        if params[field] is not None:
            if field == "queues":
                try:
                    validate_queue(conn, params[field])
                except ValueError as err:
                    print(err)
                    return -1
            if field == "projects":
                try:
                    params[field] = validate_project(conn, params[field])
                except ValueError as err:
                    print(err)
                    return -1

            update_stmt = "UPDATE association_table SET " + field

            # passing -1 will reset the column to its default value
            if params[field] == "-1":
                update_stmt += "=NULL WHERE username=?"
                tup = (username,)
            else:
                update_stmt += "=? WHERE username=?"
                tup = (
                    params[field],
                    username,
                )

            if bank is not None:
                update_stmt += " AND BANK=?"
                tup = tup + (bank,)

            conn.execute(update_stmt, tup)

    # update mod_time column
    mod_time_tup = (
        int(time.time()),
        username,
    )
    if bank is not None:
        update_stmt = """UPDATE association_table SET mod_time=?
                         WHERE username=? AND bank=?"""
        mod_time_tup = mod_time_tup + (bank,)
    else:
        update_stmt = "UPDATE association_table SET mod_time=? WHERE username=?"

    conn.execute(update_stmt, mod_time_tup)

    # commit changes
    conn.commit()

    return 0
