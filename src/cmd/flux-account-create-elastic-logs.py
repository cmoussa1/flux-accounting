#! /usr/bin/python3

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

OUTCOME_CONVERSION = {1: "COMPLETED", 2: "FAILED", 4: "CANCELLED", 8: "TIMEOUT"}


def get_username(uid) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def get_gid(uid) -> str:
    try:
        return pwd.getpwuid(uid).pw_gid
    except KeyError:
        return ""


def get_groupname(gid) -> str:
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return ""


def get_jobs(rpc_handle) -> list:
    try:
        jobs = rpc_handle.get_jobs()
        return jobs
    except EnvironmentError as exc:
        print("{}: {}".format("rpc", exc.strerror), file=sys.stderr)
        sys.exit(1)


def fetch_new_jobs(last_timestamp) -> list:
    """
    Fetch new jobs using Flux's job-list and job-info interfaces. Return a
    list of dictionaries that contain attribute information for inactive jobs.

    last_timstamp: a timestamp field to filter to only look for jobs that have
    finished since this time.
    """
    if last_timestamp is None:
        # a timestamp wasn't specified; default to gathering all jobs
        # that finished in the last hour
        last_timestamp = time.time() - 3600
    handle = flux.Flux()

    # get queue information
    future = handle.rpc("config.get")
    try:
        qlist = future.get()
    except EnvironmentError:
        sys.exit(1)

    queue_info = qlist.get("queues")
    if queue_info is not None:
        for queue in queue_info:
            # place queue name and time limit in map
            queue_timelimits[queue] = queue_info[queue]["policy"]["limits"]["duration"]

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


def create_job_dicts(jobs) -> list:
    """
    Create a list of dictionaries where each dictionary represents info about
    a single inactive job.

    jobs: a list of job dictionaries.
    """
    job_dicts = []

    # the 'result' field represents a pre-defined set of values for a job,
    # defined in libjob/job.h in flux-core
    for job in jobs:
        # create dictionary for job
        rec = {}

        # create empty parent dictionaries
        rec["event"] = {}
        rec["job"] = {}
        rec["job"]["node"] = {}
        rec["job"]["task"] = {}
        rec["job"]["proc"] = {}
        rec["user"] = {}
        rec["group"] = {}

        rec["event"]["dataset"] = "flux.joblog"

        # convert flux keys to defined common schema keys
        rec["job"]["id"] = job.get("id")
        rec["user"]["id"] = job.get("userid")
        rec["job"]["name"] = job.get("name")
        rec["job"]["priority"] = job.get("priority")
        rec["job"]["state"] = job.get("state")
        rec["job"]["bank"] = job.get("bank")
        rec["job"]["queue"] = job.get("queue")
        rec["job"]["project"] = job.get("project")
        rec["job"]["jobspec"] = job.get("jobspec")
        rec["job"]["eventlog"] = job.get("eventlog")
        rec["event"]["duration"] = job.get("duration")
        rec["job"]["node"]["list"] = job.get("nodelist")
        rec["job"]["node"]["count"] = job.get("nnodes")
        rec["job"]["task"]["count"] = job.get("ntasks")
        rec["job"]["cwd"] = job.get("cwd")
        rec["job"]["urgency"] = job.get("urgency")
        rec["job"]["success"] = job.get("success")

        if job.get("result") is not None:
            # convert outcome code to a text value
            rec["event"]["outcome"] = OUTCOME_CONVERSION[job.get("result")]

        if rec.get("job", {}).get("queue") is not None:
            # place max timelimit for queue in job record
            rec["job"]["queue_maxtimelimit"] = queue_timelimits[rec["job"]["queue"]]

        if rec.get("user", {}).get("id") is not None:
            # add username, gid, groupname
            rec["user"]["name"] = get_username(rec["user"]["id"])
            rec["group"]["id"] = get_gid(rec["user"]["id"])
            rec["group"]["name"] = get_groupname(rec["group"]["id"])

        # convert timestamps to ISO8601
        if job.get("t_submit") is not None:
            rec["job"]["submittime"] = datetime.datetime.fromtimestamp(
                job["t_submit"], tz=datetime.timezone.utc
            ).isoformat()
        if job.get("t_run") is not None:
            rec["event"]["start"] = datetime.datetime.fromtimestamp(
                job["t_run"], tz=datetime.timezone.utc
            ).isoformat()
        if job.get("t_inactive") is not None:
            rec["event"]["end"] = datetime.datetime.fromtimestamp(
                job["t_inactive"], tz=datetime.timezone.utc
            ).isoformat()
        if job.get("expiration") is not None:
            # convert expiration to total seconds
            rec["job"]["timelimit"] = datetime.datetime.fromtimestamp(
                job.get("expiration"), tz=datetime.timezone.utc
            ).isoformat()

        if job.get("t_depend") is not None and job.get("t_run") is not None:
            # compute the timestamp of when the job first became eligible
            t_eligible = job.get("t_run") - (job.get("t_run") - job.get("t_depend"))
            rec["job"]["eligibletime"] = datetime.datetime.fromtimestamp(
                t_eligible, tz=datetime.timezone.utc
            ).isoformat()
            # compute the time spend in queue
            rec["job"]["queue_time"] = round(job.get("t_run") - t_eligible, 1)

        if job.get("t_inactive") is not None and job.get("t_run") is not None:
            # compute actual execution time
            rec["event"]["duration_seconds"] = round(
                job.get("t_inactive") - job.get("t_run"), 1
            )

        if job.get("nnodes") is not None and job.get("ntasks") is not None:
            # compute number of processes * number of nodes
            rec["job"]["proc"]["count"] = job.get("nnodes") * job.get("ntasks")

        if job.get("exception_occurred") is not None and job.get("exception_occurred") == True:
            print(f"exception occurred!")
            if job.get("exception_type") is not None:
                rec["job"]["exception_type"] = job.get("exception_type")
            if job.get("exception_note") is not None:
                rec["job"]["exception_note"] = job.get("exception_note")

        # add scheduler used
        rec["job"]["scheduler"] = "flux"

        job_dicts.append(rec)

    return job_dicts


def write_to_file(job_records, output_file):
    with open(output_file, "a") as fp:
        for record in job_records:
            fp.write(json.dumps(record) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="""
        Description: Fetch inactive job records using Flux's job-list and
        job-info interfaces and create custom NDJSON objects out of each one.
        """
    )

    parser.add_argument(
        "--output-file",
        type=str,
        help="specify a file path to append logs to",
        metavar="OUTPUT_FILE",
    )
    parser.add_argument(
        "--since",
        type=int,
        help=(
            "fetch all jobs since a certain time (formatted in seconds since epoch); "
            "by default, this script will fetch all jobs that have completed in the "
            "last hour"
        ),
        metavar="TIMESTAMP",
    )
    args = parser.parse_args()

    jobs = fetch_new_jobs(args.since)
    job_records = create_job_dicts(jobs)

    if args.output_file is None:
        filename = "flux_jobs.ndjson"
    else:
        filename = args.output_file
    write_to_file(job_records, filename)


if __name__ == "__main__":
    main()
