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

import flux
import flux.job


def get_jobs(rpc_handle):
    try:
        jobs = rpc_handle.get_jobs()
        return jobs
    except EnvironmentError as exc:
        print("{}: {}".format("rpc", exc.strerror), file=sys.stderr)
        sys.exit(1)


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
        data = flux.job.job_kvs_lookup(
            handle, single_job["id"], keys=["R", "jobspec"], decode=False
        )

        if data is None:
            # this job never ran; don't add it to a user's list of job records
            continue
        if data["R"] is not None:
            single_record["R"] = data["R"]
        if data["jobspec"] is not None:
            single_record["jobspec"] = data["jobspec"]

        # append job to job_records list
        job_records.append(single_record)

    return job_records


def write_to_file(job_records, output_file):
    with open(output_file, 'w') as file:
        for record in job_records:
            file.write(json.dumps(record) + '\n')


def main():
    parser = argparse.ArgumentParser(
        description="""
        Description: Fetch new job records using Flux's job-list and job-info
        interfaces and insert them into a table in the flux-accounting DB.
        """
    )

    parser.add_argument(
        "output_file",
        help="specify output file",
        metavar="OUTPUT_FILE",
    )
    args = parser.parse_args()

    job_records = fetch_new_jobs()

    print(f"length of job_records: {len(job_records)}")
    for record in job_records:
        print(record)

    write_to_file(job_records, args.output_file)


if __name__ == "__main__":
    main()
