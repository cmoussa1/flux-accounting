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
import flux
import argparse
import sys
import os
import sqlite3

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
    except sqlite3.OperationalError:
        print(f"Unable to open database file: {db_uri}", file=sys.stderr)
        sys.exit(1)

    return conn


def bulk_user_update(cur):
    # fetch all rows from association_table (will print out tuples)
    for row in cur.execute(
        "SELECT userid, bank, default_bank, fairshare, max_jobs, qos FROM association_table"
    ):
        # create a JSON payload with the results of the query
        data = {
            "userid": str(row[0]),
            "bank": str(row[1]),
            "default_bank": str(row[2]),
            "fairshare": str(row[3]),
            "max_jobs": str(row[4]),
            "qos": str(row[5]),
        }

        flux.Flux().rpc("job-manager.mf_priority.get_users", data).get()


def bulk_qos_update(cur):
    # fetch all rows from association_table (will print out tuples)
    for row in cur.execute("SELECT qos, priority FROM qos_table"):
        # create a JSON payload with the results of the query
        data = {
            "qos": str(row[0]),
            "priority": str(row[1]),
        }

        flux.Flux().rpc("job-manager.mf_priority.get_qos", data).get()


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

    conn = est_sqlite_conn(path)
    cur = conn.cursor()

    bulk_user_update(cur)
    bulk_qos_update(cur)

    conn.close()


if __name__ == "__main__":
    main()