#!/usr/bin/env python3

import sys
import json
import argparse
import pwd
import grp
import time
import datetime

import flux
import flux.job


queue_timelimits = {}


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


def fetch_new_jobs(last_timestamp=time.time() - 3600):
    """
    Fetch new jobs using Flux's job-list and job-info interfaces. Return a
    list of dictionaries that contain attribute information for inactive jobs.

    last_timstamp: a timestamp field to filter to only look for jobs that have
    finished since this time.
    """
    handle = flux.Flux()

    # get queue information
    future = handle.rpc("config.get")
    try:
        qlist = future.get()
    except EnvironmentError:
        sys.exit(1)

    queue_info = qlist.get("queues")
    if queue_info is not None:
        for q in queue_info:
            # place queue name and time limit in map
            queue_timelimits[q] = queue_info[q]["policy"]["limits"]["duration"]

    # construct and send RPC
    rpc_handle = flux.job.job_list_inactive(handle, since=last_timestamp, max_entries=0)
    jobs = get_jobs(rpc_handle)

    for job in jobs:
        # fetch jobspec
        job_data = flux.job.job_kvs_lookup(
            handle, job["id"], keys=["jobspec", "eventlog"], decode=True
        )
        if job_data is not None and job_data.get("jobspec") is not None:
            try:
                jobspec = job_data["jobspec"]
                job["jobspec"] = job_data["jobspec"]

                job["duration"] = (
                    jobspec.get("attributes", {}).get("system", {}).get("duration", {})
                )

                accounting_attributes = jobspec.get("attributes", {}).get("system", {})
                job["bank"] = accounting_attributes.get("bank")
                job["queue"] = accounting_attributes.get("queue")
                job["project"] = accounting_attributes.get("project")
            except json.JSONDecodeError as exc:
                # the job does not have a valid jobspec, so don't add it to
                # the job dictionary
                continue

        if job_data is not None and job_data.get("eventlog") is not None:
            job["eventlog"] = job_data.get("eventlog")

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
                "duration",
                "nodelist",
                "nnodes",
                "ntasks",
                "cwd",
                "urgency",
                "success",
                "result",
                "queue",
                "project",
                "eventlog",
                "jobspec",
            ]
            if job.get(key) is not None
        }

        if rec.get("queue") is not None:
            # place max timelimit for queue in job record
            rec["queue_max_timelimit"] = queue_timelimits[rec.get("queue")]

        if rec.get("userid") is not None:
            # add username, gid, groupname
            rec["username"] = get_username(rec["userid"])
            rec["gid"] = get_gid(rec["userid"])
            rec["groupname"] = get_groupname(rec["gid"])

        # convert timestamps to ISO8601
        if job.get("t_submit") is not None:
            rec["t_submit"] = datetime.datetime.fromtimestamp(
                job["t_submit"], tz=datetime.timezone.utc
            ).isoformat()
        if job.get("t_run") is not None:
            rec["t_run"] = datetime.datetime.fromtimestamp(
                job["t_run"], tz=datetime.timezone.utc
            ).isoformat()
        if job.get("t_inactive") is not None:
            rec["t_inactive"] = datetime.datetime.fromtimestamp(
                job["t_inactive"], tz=datetime.timezone.utc
            ).isoformat()
        if job.get("expiration") is not None:
            # convert expiration to total seconds
            rec["expiration"] = datetime.datetime.fromtimestamp(
                job.get("expiration"), tz=datetime.timezone.utc
            ).isoformat()

        if job.get("t_depend") is not None and job.get("t_run") is not None:
            # compute eligible time
            rec["t_eligible"] = job.get("t_run") - job.get("t_depend")

        if job.get("t_inactive") is not None and job.get("t_run") is not None:
            # compute actual execution time
            rec["actual_duration"] = round(job.get("t_inactive") - job.get("t_run"), 1)

        if job.get("nnodes") is not None and job.get("ntasks") is not None:
            # compute number of processes * number of nodes
            rec["proc.count"] = job.get("nnodes") * job.get("ntasks")

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
        Description: Fetch inactive job records using Flux's job-list and
        job-info interfaces and create custom NDJSON objects out of each one.
        """
    )

    parser.add_argument(
        "--since",
        type=int,
        help="fetch all jobs since a certain time (formatted in seconds since epoch)",
        metavar="TIMESTAMP",
    )
    args = parser.parse_args()

    jobs = fetch_new_jobs(args.since) if args.since is not None else fetch_new_jobs()
    job_records = create_job_dicts(jobs)

    filename = f"{round(time.time())}_flux_jobs.ndjson"
    write_to_file(job_records, filename)


if __name__ == "__main__":
    main()
