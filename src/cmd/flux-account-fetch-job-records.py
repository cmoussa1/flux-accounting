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


# we calculate where the cut-off is for old jobs using the following algorithm:
#
# cut_off_ts = last_seen_timestamp - (how many weeks * 604800)
#
# we can calculate how many weeks there are by looking at how many usage_period
# columns there are in the job_usage_factor_table; it's the number of columns
# minus 4
def purge_old_jobs(conn, last_seen_timestamp):
    cur = conn.cursor()

    # determine the number of weeks are in a usage period by looking at the
    # flux-accounting DB
    cur.execute("SELECT * FROM job_usage_factor_table")
    names = [description[0] for description in cur.description]
    num_usage_periods = len(names) - 4
    cut_off_ts = last_seen_timestamp - (num_usage_periods * 604800)

    del_stmt = "DELETE FROM jobs WHERE t_inactive < ?"
    cur.execute(del_stmt, (cut_off_ts,))

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
    last_seen_timestamp = 0
    if len(job_records) > 0:
        last_seen_timestamp = job_records[0]["t_inactive"]
        update_last_seen_job(conn, last_seen_timestamp)

    # if last_seen_timestamp == 0, then we haven't seen any jobs yet, so we
    # don't need to purge anything
    if last_seen_timestamp > 0:
        # finally, we should clean up any old jobs that are no longer
        # considered by flux-accounting for job usage and fair share
        purge_old_jobs(conn, last_seen_timestamp)


if __name__ == "__main__":
    main()
