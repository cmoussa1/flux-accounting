#!/usr/bin/env python3

###############################################################
# Copyright 2023 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################

import os
import sys
import argparse
import sqlite3

import flux
import fluxacct.accounting

from flux.job import JobID


def set_db_loc(args):
    path = args.path if args.path else fluxacct.accounting.db_path

    return path


# try to open database file; will exit with -1 if database file not found
def est_sqlite_conn(path):
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

    return conn


def get_jobs(rpc_handle):
    try:
        jobs = rpc_handle.get_jobs()
        return jobs
    except EnvironmentError as exc:
        print("{}: {}".format("rpc", exc.strerror), file=sys.stderr)
        sys.exit(1)


# fetch the timestamp of last seen job; this is used as a filter
# when looking for newly inactive jobs
def get_last_job_ts(conn):
    s_ts = "SELECT timestamp FROM last_seen_job_table"
    cur = conn.cursor()
    cur.execute(s_ts)
    row = cur.fetchone()

    return float(row[0])


# update the timestamp of the last seen inactive job
def update_last_seen_job(conn, last_seen_timestamp):
    u_ts = "UPDATE last_seen_job_table SET timestamp=?"
    conn.execute(
        u_ts,
        (last_seen_timestamp,),
    )
    conn.commit()


# fetch new jobs using Flux's job-list and job-info interfaces;
# create job records for each newly seen job
def fetch_new_jobs(last_timestamp=0.0):
    handle = flux.Flux()

    # attributes needed using job-list
    custom_attrs = ["userid", "t_submit", "t_run", "t_inactive", "ranks"]

    # construct and send RPC
    rpc_handle = flux.job.job_list_inactive(
        handle, attrs=custom_attrs, since=last_timestamp
    )
    jobs = get_jobs(rpc_handle)

    # job_records is a list of dictionaries where each dictionary contains
    # information about a single job record
    job_records = []
    for single_job in jobs:
        single_record = {}
        # get attributes from job-list
        for attr in single_job:
            single_record[attr] = single_job[attr]

        # attributes needed using job-info
        payload = {"id": JobID(single_job["id"]), "keys": ["jobspec", "R"], "flags": 0}

        resource_set = handle.rpc("job-info.lookup", payload).get()["R"]
        single_record["R"] = resource_set
        jobspec = handle.rpc("job-info.lookup", payload).get()["jobspec"]
        single_record["jobspec"] = jobspec

        # append job to job_records list
        job_records.append(single_record)

    return job_records


# insert newly seen jobs into the "jobs" table in the flux-accounting DB
def insert_jobs_in_db(conn, job_records):
    cur = conn.cursor()

    for single_job in job_records:
        cur.execute(
            """
            INSERT OR IGNORE INTO jobs
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                single_job["id"],
                single_job["userid"],
                single_job["t_submit"],
                single_job["t_run"],
                single_job["t_inactive"],
                single_job["ranks"],
                single_job["R"],
                single_job["jobspec"],
            ),
        )

    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="""
        Description: Fetch new job records using Flux's job-list and job-info
        interfaces and insert them into a table in the flux-accounting DB.
        """
    )

    parser.add_argument(
        "-p", "--path", dest="path", help="specify location of database file"
    )
    args = parser.parse_args()

    path = set_db_loc(args)
    conn = est_sqlite_conn(path)

    timestamp = get_last_job_ts(conn)

    job_records = []
    job_records = fetch_new_jobs(timestamp)

    insert_jobs_in_db(conn, job_records)

    # if there are new jobs found, the first job seen in job_records
    # will be the last seen completed job
    if len(job_records) > 0:
        last_seen_timestamp = job_records[0]["t_inactive"]
        update_last_seen_job(conn, last_seen_timestamp)


if __name__ == "__main__":
    main()
