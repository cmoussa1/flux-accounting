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

import sys
import json
import argparse
import pwd
import grp
import time

import flux
import flux.job


def get_username(uid):
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def get_gid(uid):
    try:
        return pwd.getpwuid(uid).pw_gid
    except KeyError:
        return ""


def get_groupname(gid):
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return ""


def get_jobs(rpc_handle):
    try:
        jobs = rpc_handle.get_jobs()
        return jobs
    except EnvironmentError as exc:
        print("{}: {}".format("rpc", exc.strerror), file=sys.stderr)
        sys.exit(1)


def fetch_new_jobs(last_timestamp=time.time() - (30 * 60)):
    """
    Fetch new jobs using Flux's job-list and job-info interfaces. Return a
    list of dictionaries that contain attribute information for inactive jobs.

    last_timstamp: a timestamp field to filter to only look for jobs that have
    finished since this time.
    """
    handle = flux.Flux()

    # construct and send RPC
    rpc_handle = flux.job.job_list_inactive(handle, since=last_timestamp)
    jobs = get_jobs(rpc_handle)

    for job in jobs:
        # fetch jobspec
        job_data = flux.job.job_kvs_lookup(
            handle, job["id"], keys=["jobspec"], decode=False
        )
        if job_data["jobspec"] is not None:
            try:
                jobspec = json.loads(job_data["jobspec"])
                accounting_attributes = jobspec.get("attributes", {}).get("system", {})

                job["bank"] = accounting_attributes.get("bank")
                job["queue"] = accounting_attributes.get("queue")
                job["project"] = accounting_attributes.get("project")
            except json.JSONDecodeError as exc:
                # the job does not have a valid jobspec, so don't add it to
                # the job dictionary
                continue

    return jobs


def create_job_dicts(jobs):
    """
    Create a list of dictionaries where each dictionary represents info about
    a single inactive job.

    jobs: a list of job dictionaries.
    """
    job_dicts = []

    # the 'result' field represents a pre-defined set of values for a job,
    # defined in libjob/job.h in flux-core
    for job in jobs:
        rec = {
            key: job[key]
            for key in [
                "id",
                "userid",
                "name",
                "priority",
                "state",
                "bank",
                "queue",
                "expiration",
                "t_run",
                "t_inactive",
                "t_run",
                "nodelists",
                "nnodes",
                "cwd",
                "urgency",
                "t_submit",
                "success",
                "result",
                "queue",
                "project",
            ]
            if job.get(key) is not None
        }

        if rec.get("userid") is not None:
            # add username, gid, groupname
            rec["username"] = get_username(rec["userid"])
            rec["gid"] = get_gid(rec["userid"])
            rec["groupname"] = get_groupname(rec["gid"])

        if rec.get("t_run") is not None and rec.get("t_inactive") is not None:
            # compute job duration
            rec["duration"] = rec["t_inactive"] - rec["t_run"]

        # TODO: compute number of processes * number of nodes
        # TODO: compute eligible time?

        # add scheduler used
        rec["scheduler"] = "flux"

        job_dicts.append(rec)

    return job_dicts


def write_to_file(job_records, output_file):
    with open(output_file, "w") as file:
        for record in job_records:
            file.write(json.dumps(record) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="""
        Description: Fetch ianctive job records using Flux's job-list interface
        and create custom JSON objects out of each one.
        """
    )

    parser.add_argument(
        "output_file",
        help="specify output file",
        metavar="OUTPUT_FILE",
    )
    args = parser.parse_args()

    jobs = fetch_new_jobs()
    job_records = create_job_dicts(jobs)

    for record in job_records:
        print(record)

    write_to_file(job_records, args.output_file)


if __name__ == "__main__":
    main()
