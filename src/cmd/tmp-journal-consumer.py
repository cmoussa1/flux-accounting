#!/usr/bin/env python3
###############################################################
# Copyright 2026 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import argparse
import signal
import sys
import json
import pwd
import grp
import datetime

import flux
from flux.job import JournalConsumer


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


def format_jobid(jobid_format, event):
    """Get job ID in the requested format."""
    if jobid_format == "f58":
        return event.jobid.f58
    return event.jobid.dec


def build_job_record(jobid, job_rec):
    """
    Build enriched job record from accumulated events.

    Args:
        jobid: Job ID object
        job_rec: Dict with 'events', 'jobspec', 'R' keys

    Returns:
        Dict with complete job information matching create-flux-job-logs.py format
    """
    rec = {
        "event": {},
        "job": {"node": {}, "task": {}, "proc": {}},
        "user": {},
        "group": {},
        "schema": {"version_number": 2.3},
    }
    rec["job"]["id"] = str(jobid)
    rec["job"]["node"]["list"] = -1

    # Extract timestamps and context from events
    timestamps = {}
    for evt in job_rec["events"]:
        timestamps[evt["name"]] = evt["timestamp"]

        # Extract context data
        if evt["name"] == "priority" and evt.get("context", {}).get("priority"):
            rec["job"]["priority"] = evt["context"]["priority"]
        elif (
            evt["name"] == "urgency"
            and evt.get("context", {}).get("urgency") is not None
        ):
            rec["job"]["urgency"] = evt["context"]["urgency"]
        elif (
            evt["name"] == "finish" and evt.get("context", {}).get("status") is not None
        ):
            rec["job"]["exit_code"] = evt["context"]["status"]
            rec["job"]["success"] = evt["context"]["status"] == 0
        elif evt["name"] == "exception":
            rec["job"]["exception_type"] = evt["context"].get("type")
            rec["job"]["exception_note"] = evt["context"].get("note")
            rec["job"]["exception_occurred"] = True

    # Extract from jobspec
    if job_rec.get("jobspec"):
        jobspec = job_rec["jobspec"]
        rec["job"]["jobspec"] = jobspec
        rec["job"]["name"] = (
            jobspec.get("tasks", [{}])[0].get("command", [""])[0]
            if jobspec.get("tasks")
            else None
        )
        rec["job"]["cwd"] = jobspec.get("attributes", {}).get("system", {}).get("cwd")

        accounting_attrs = jobspec.get("attributes", {}).get("system", {})
        rec["job"]["bank"] = accounting_attrs.get("bank")
        rec["job"]["queue"] = accounting_attrs.get("queue")
        rec["job"]["project"] = accounting_attrs.get("project")
        rec["job"]["requested_duration"] = accounting_attrs.get("duration")

        # Get expiration/timelimit
        expiration = jobspec.get("attributes", {}).get("system", {}).get("expiration")
        if expiration is not None:
            rec["job"]["timelimit"] = datetime.datetime.fromtimestamp(
                expiration, tz=datetime.timezone.utc
            ).isoformat()

        # Get userid
        userid = (
            jobspec.get("attributes", {}).get("system", {}).get("job", {}).get("userid")
        )
        if userid is not None:
            rec["user"]["id"] = userid
            rec["user"]["name"] = get_username(userid)
            rec["group"]["id"] = get_gid(userid)
            rec["group"]["name"] = get_groupname(rec["group"]["id"])

    # Extract from R
    if job_rec.get("R"):
        R = job_rec["R"]
        rec["job"]["node"]["list"] = R.get("execution", {}).get("nodelist")

    # Set timestamps
    if "submit" in timestamps:
        rec["job"]["submittime_epoch"] = timestamps["submit"]
        rec["job"]["submittime"] = datetime.datetime.fromtimestamp(
            timestamps["submit"], tz=datetime.timezone.utc
        ).isoformat()
    elif "validate" in timestamps:
        # Fallback to validate if submit event not present
        rec["job"]["submittime_epoch"] = timestamps["validate"]
        rec["job"]["submittime"] = datetime.datetime.fromtimestamp(
            timestamps["validate"], tz=datetime.timezone.utc
        ).isoformat()

    if "start" in timestamps:
        rec["job"]["t_run"] = timestamps["start"]
        rec["event"]["start"] = datetime.datetime.fromtimestamp(
            timestamps["start"], tz=datetime.timezone.utc
        ).isoformat()

    if "finish" in timestamps:
        rec["job"]["t_inactive"] = timestamps["finish"]
        rec["event"]["end"] = datetime.datetime.fromtimestamp(
            timestamps["finish"], tz=datetime.timezone.utc
        ).isoformat()

    if "clean" in timestamps:
        rec["job"]["t_cleanup"] = timestamps["clean"]

    # Calculate derived values
    if "start" in timestamps and "finish" in timestamps:
        rec["event"]["duration_seconds"] = round(
            timestamps["finish"] - timestamps["start"], 1
        )
        rec["event"]["duration"] = rec["event"]["duration_seconds"] * 10 ** 9

    if "depend" in timestamps and "start" in timestamps:
        # compute the timestamp of when the job first became eligible
        t_eligible = timestamps["start"] - (timestamps["start"] - timestamps["depend"])
        rec["job"]["eligibletime"] = datetime.datetime.fromtimestamp(
            t_eligible, tz=datetime.timezone.utc
        ).isoformat()
        # compute the time spent in queue
        rec["job"]["queue_time"] = round(timestamps["start"] - t_eligible, 1)
    elif "start" in timestamps:
        # Fallback queue time calculation using submit or validate
        t_submit = timestamps.get("submit") or timestamps.get("validate")
        if t_submit:
            rec["job"]["queue_time"] = round(timestamps["start"] - t_submit, 1)

    rec["job"]["state"] = "INACTIVE"
    rec["job"]["scheduler"] = "flux"

    # Include the full eventlog as a reconstructed list of events
    rec["job"]["eventlog"] = job_rec["events"]

    return rec


# pylint: disable=broad-except
def main():
    parser = argparse.ArgumentParser(
        description="stream enriched Flux job records in real-time as JSON"
    )
    parser.add_argument(
        "-s",
        "--since",
        type=float,
        default=0.0,
        help="Only show events after this timestamp (default: 0.0 for all history)",
    )
    parser.add_argument(
        "-j",
        "--jobid-format",
        choices=["dec", "f58"],
        default="dec",
        help="Job ID format (default: dec)",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip historical events, only show new events",
    )
    parser.add_argument(
        "-S",
        "--show-sentinel",
        action="store_true",
        help="Show sentinel event marking end of historical data",
    )
    args = parser.parse_args()

    flux_handle = flux.Flux()

    consumer = JournalConsumer(
        flux_handle,
        full=not args.no_history,
        since=args.since,
        include_sentinel=args.show_sentinel,
    ).start()

    # handle Ctrl+C gracefully
    def signal_handler(signum, frame):
        print("\nStopping consumer...")
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Track jobs in flight
    jobs_in_flight = {}

    try:
        while True:
            event = consumer.poll()

            # "None" indicates end of stream
            if event is None:
                break

            # handle sentinel event
            if event.is_empty():
                if args.show_sentinel:
                    print(event, flush=True)
                continue

            # get job ID in requested format
            jobid_str = format_jobid(args.jobid_format, event)

            # Get or create job record for accumulation
            if jobid_str not in jobs_in_flight:
                jobs_in_flight[jobid_str] = {
                    "events": [],
                    "jobspec": None,
                    "R": None,
                }

            job_rec = jobs_in_flight[jobid_str]

            # Accumulate event
            job_rec["events"].append(
                {
                    "name": event.name,
                    "timestamp": event.timestamp,
                    "context": dict(event.context) if event.context else {},
                }
            )

            # If terminal event, fetch jobspec/R and emit full job record
            if event.name in ("clean", "release", "free"):
                # Fetch jobspec and eventlog from KVS (has all attributes set)
                try:
                    job_data_kvs = flux.job.job_kvs_lookup(
                        flux_handle, event.jobid, keys=["jobspec", "R"], decode=True
                    )
                    if job_data_kvs:
                        if job_data_kvs.get("jobspec"):
                            job_rec["jobspec"] = job_data_kvs["jobspec"]
                        if job_data_kvs.get("R"):
                            job_rec["R"] = job_data_kvs["R"]
                except Exception as exc:
                    print(
                        f"warning: failed to fetch jobspec for {jobid_str}: {exc}",
                        file=sys.stderr,
                    )

                job_data = build_job_record(event.jobid, job_rec)
                print(json.dumps(job_data), flush=True)
                # Remove from tracking
                del jobs_in_flight[jobid_str]

    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        consumer.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
