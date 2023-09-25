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
import argparse
import sys
import os
import sqlite3
import json
import subprocess

import flux

import fluxacct.accounting


def set_db_loc(args):
    path = args.path if args.path else fluxacct.accounting.db_path

    return path


def est_sqlite_conn(path):
    # try to open database file; will exit with -1 if database file not found
    if not os.path.isfile(path):
        print(f"Database file does not exist: {path}", file=sys.stderr)
        sys.exit(1)

    db_uri = "file:" + path + "?mode=rw"
    try:
        conn = sqlite3.connect(db_uri, uri=True)
        # set foreign keys constraint
        conn.execute("PRAGMA foreign_keys = 1")
    except sqlite3.OperationalError as exc:
        print(f"Unable to open database file: {db_uri}", file=sys.stderr)
        print(f"Exception: {exc}")
        sys.exit(1)

    # check version of database; if not up to date, output message
    # and exit
    cur = conn.cursor()
    cur.execute("PRAGMA user_version")
    db_version = cur.fetchone()[0]
    if db_version < fluxacct.accounting.db_schema_version:
        print(
            """flux-accounting database out of date; updating DB with """
            """'flux account-update-db' before sending infomation to plugin"""
        )
        # if flux account-update-db fails, we should not attempt to send data from
        # the DB to the priority plugin, and instead we should abort
        try:
            subprocess.run(["flux", "account-update-db", "-p", path], check=True)
        except SystemExit as exc:
            print(f"Exception: {exc.code}")
            sys.exit(1)

    return conn


def bulk_update(path):
    conn = est_sqlite_conn(path)
    cur = conn.cursor()

    data = {}
    bulk_user_data = []
    bulk_q_data = []
    bulk_p_data = []

    # fetch all rows from association_table (will print out tuples)
    for row in cur.execute(
        """SELECT userid, bank, default_bank,
           fairshare, max_running_jobs, max_active_jobs,
           queues, active, projects, default_project
           FROM association_table"""
    ):
        # create a JSON payload with the results of the query
        single_user_data = {
            "userid": int(row[0]),
            "bank": str(row[1]),
            "def_bank": str(row[2]),
            "fairshare": float(row[3]),
            "max_running_jobs": int(row[4]),
            "max_active_jobs": int(row[5]),
            "queues": str(row[6]),
            "active": int(row[7]),
            "projects": str(row[8]),
            "def_project": str(row[9]),
        }
        bulk_user_data.append(single_user_data)

    data = {"data": bulk_user_data}

    flux.Flux().rpc("job-manager.mf_priority.rec_update", json.dumps(data)).get()

    # fetch all rows from queue_table
    for row in cur.execute("SELECT * FROM queue_table"):
        # create a JSON payload with the results of the query
        single_q_data = {
            "queue": str(row[0]),
            "min_nodes_per_job": int(row[1]),
            "max_nodes_per_job": int(row[2]),
            "max_time_per_job": int(row[3]),
            "priority": int(row[4]),
        }
        bulk_q_data.append(single_q_data)

    data = {"data": bulk_q_data}

    flux.Flux().rpc("job-manager.mf_priority.rec_q_update", json.dumps(data)).get()

    # fetch all rows from project_table
    for row in cur.execute("SELECT project FROM project_table"):
        # create a JSON payload with the results of the query
        single_p_data = {
            "project": str(row[0]),
        }
        bulk_p_data.append(single_p_data)

    data = {"data": bulk_p_data}

    flux.Flux().rpc("job-manager.mf_priority.rec_project_update", data).get()

    flux.Flux().rpc("job-manager.mf_priority.reprioritize")

    # close DB connection
    cur.close()


def main():
    parser = argparse.ArgumentParser(
        description="""
        Description: Send a bulk update of user information from a
        flux-accounting database to the multi-factor priority plugin.
        """
    )

    parser.add_argument(
        "-p", "--path", dest="path", help="specify location of database file"
    )
    args = parser.parse_args()

    path = set_db_loc(args)

    bulk_update(path)


if __name__ == "__main__":
    main()
