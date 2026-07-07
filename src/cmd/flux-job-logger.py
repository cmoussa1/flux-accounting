#!/usr/bin/env python3
import sys
import json
import argparse
import pwd
import grp
import signal
import datetime

import flux
import flux.job
from flux.job import JournalConsumer


# Queue timelimits cache
queue_timelimits = {}

# Map of flux job result codes to outcome strings
OUTCOME_CONVERSION = {1: "COMPLETED", 2: "FAILED", 4: "CANCELLED", 8: "TIMEOUT"}


def get_username(uid) -> str:
    try:
        username = pwd.getpwuid(uid).pw_name
    except (KeyError, ValueError, TypeError):
        username = str(uid)
    return username


def get_gid(uid) -> str:
    try:
        gid = pwd.getpwuid(uid).pw_gid
    except (KeyError, ValueError, TypeError):
        gid = ""
    return gid


def get_groupname(gid) -> str:
    try:
        groupname = grp.getgrgid(gid).gr_name
    except (KeyError, ValueError, TypeError):
        groupname = ""
    return groupname


def fetch_queue_timelimits(handle):
    """Fetch queue timelimit information from Flux config."""
    future = handle.rpc("config.get")
    try:
        qlist = future.get()
    except EnvironmentError:
        return

    queue_info = qlist.get("queues", {})
    if queue_info:
        for queue, details in queue_info.items():
            queue_timelimits[queue] = (
                details.get("policy", {}).get("limits", {}).get("duration", "UNKNOWN")
            )


def fetch_job_data(handle, jobid):
    """
    Fetch complete job data for a completed job using job-list and job-kvs.
    Returns a dictionary with job attributes, or None if the job cannot be fetched.
    """
    try:
        # use job_list_id to get job details
        rpc_handle = flux.job.job_list_id(handle, jobid)
        job = rpc_handle.get_job()

        if job is None:
            return None

        # fetch jobspec and eventlog from KVS
        job_data = flux.job.job_kvs_lookup(
            handle, jobid, keys=["jobspec", "eventlog"], decode=True
        )

        if job_data is not None and job_data.get("jobspec") is not None:
            try:
                jobspec = job_data["jobspec"]
                job["jobspec"] = jobspec

                job["duration"] = (
                    jobspec.get("attributes", {}).get("system", {}).get("duration", {})
                )

                accounting_attributes = jobspec.get("attributes", {}).get("system", {})
                job["bank"] = accounting_attributes.get("bank")
                job["queue"] = accounting_attributes.get("queue")
                job["project"] = accounting_attributes.get("project")
            except (json.JSONDecodeError, AttributeError):
                pass

        if job_data is not None and job_data.get("eventlog") is not None:
            job["eventlog"] = job_data.get("eventlog")

        return job
    except Exception as exc:
        print(f"Error fetching job data for {jobid}: {exc}", file=sys.stderr)
        return None


def create_job_record(job) -> dict:
    """
    Create a job record dictionary from job data.
    Reuses the formatting logic from create-flux-job-logs.py.
    """
    rec = {}

    rec["event"] = {}
    rec["job"] = {}
    rec["job"]["node"] = {}
    rec["job"]["task"] = {}
    rec["job"]["proc"] = {}
    rec["user"] = {}
    rec["group"] = {}

    rec["event"]["dataset"] = "flux.joblog"
    rec["schema"] = {}
    rec["schema"]["version_number"] = 0.1
    # initialize job.node.list
    rec["job"]["node"]["list"] = -1

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
    rec["job"]["requested_duration"] = job.get("duration")
    rec["job"]["node"]["list"] = job.get("nodelist")
    rec["job"]["node"]["count"] = job.get("nnodes")
    rec["job"]["task"]["count"] = job.get("ntasks")
    rec["job"]["cwd"] = job.get("cwd")
    rec["job"]["urgency"] = job.get("urgency")
    rec["job"]["success"] = job.get("success")
    rec["job"]["exit_code"] = job.get("waitstatus")
    rec["job"]["t_run"] = job.get("t_run")
    rec["job"]["t_inactive"] = job.get("t_inactive")
    rec["job"]["t_cleanup"] = job.get("t_cleanup")

    if job.get("result") is not None:
        rec["event"]["outcome"] = OUTCOME_CONVERSION.get(job.get("result"), "UNKNOWN")

    if rec.get("job", {}).get("queue") is not None:
        rec["job"]["queue_maxtimelimit"] = queue_timelimits.get(
            rec["job"]["queue"], "UNKNOWN"
        )

    if rec.get("user", {}).get("id") is not None:
        rec["user"]["name"] = get_username(rec["user"]["id"])
        rec["group"]["id"] = get_gid(rec["user"]["id"])
        rec["group"]["name"] = get_groupname(rec["group"]["id"])

    # convert timestamps to ISO8601
    if job.get("t_submit") is not None:
        rec["job"]["submittime_epoch"] = job["t_submit"]
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
        rec["event"]["duration"] = rec["event"]["duration_seconds"] * 10 ** 9

    if job.get("nnodes") is not None and job.get("ntasks") is not None:
        # compute number of processes * number of nodes
        rec["job"]["proc"]["count"] = job.get("nnodes") * job.get("ntasks")

    if (
        job.get("exception_occurred") is not None
        and job.get("exception_occurred") == True
    ):
        if job.get("exception_type") is not None:
            rec["job"]["exception_type"] = job.get("exception_type")
        if job.get("exception_note") is not None:
            rec["job"]["exception_note"] = job.get("exception_note")

    # calculate node-seconds for job
    rec["job"]["node-seconds"] = job.get("nnodes", 0) * rec.get("event", {}).get(
        "duration_seconds", 0
    )

    # add scheduler used
    rec["job"]["scheduler"] = "flux"

    return rec


def main():
    parser = argparse.ArgumentParser(
        description="""
        Stream Flux job events in real-time and output complete job records
        as JSON to stdout when jobs complete.
        """
    )
    parser.add_argument(
        "--since",
        type=float,
        default=0.0,
        help="Only process jobs that completed after this timestamp (default: 0.0)",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip historical events, only process new jobs",
    )
    args = parser.parse_args()

    try:
        flux_handle = flux.Flux()
    except Exception as exc:
        print(f"Could not connect to Flux instance: {exc}", file=sys.stderr)
        sys.exit(1)

    # fetch queue timelimits
    fetch_queue_timelimits(flux_handle)

    # create and start the journal consumer
    consumer = JournalConsumer(
        flux_handle,
        full=not args.no_history,
        since=args.since,
        include_sentinel=False,
    ).start()

    # handle Ctrl+C gracefully
    def signal_handler(signum, frame):
        print("\nStopping job logger...", file=sys.stderr)
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # # Track jobs we've already processed to avoid duplicates
    # processed_jobs = set()

    try:
        while True:
            event = consumer.poll()

            # None indicates end of stream
            if event is None:
                break

            # skip sentinel events
            if event.is_empty():
                continue

            # only begin fetching data once job reaches "clean" event
            if event.name == "clean":
                jobid = event.jobid.dec

                # # Avoid processing the same job multiple times
                # if jobid in processed_jobs:
                #     continue

                # processed_jobs.add(jobid)

                # fetch complete job data
                job = fetch_job_data(flux_handle, jobid)
                if job is None:
                    print(
                        f"Warning: Could not fetch data for job {jobid}",
                        file=sys.stderr,
                    )
                    continue

                # create and output job record
                job_record = create_job_record(job)
                print(json.dumps(job_record), flush=True)

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        consumer.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
