#!/usr/bin/env python3

###############################################################
# Copyright 2024 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import pwd
import csv
import json

from flux.resource import ResourceSet
from flux.util import parse_datetime
from flux.job.JobID import JobID
from fluxacct.util import formatter as fmt


def get_username(userid):
    try:
        return pwd.getpwuid(userid).pw_name
    except KeyError:
        return str(userid)


def get_uid(username):
    try:
        return pwd.getpwnam(username).pw_uid
    except KeyError:
        return str(username)


def parse_timestamp(timestamp):
    """
    Parse an input timestamp for after-start-time or before-end-time, which
    could be in multiple formats. Try to first parse it as a human-readable
    format (e.g, "2025-01-27 12:00:00"), or just return as a
    seconds-since-epoch timestamp if the parsing fails.

    Returns:
        a seconds-since-epoch timestamp
    """
    try:
        # try to parse as a human-readable timestamp
        return parse_datetime(str(timestamp)).timestamp()
    except ValueError:
        # just return as a seconds-since-epoch timestamp
        return timestamp


class JobRecord:
    """
    A record of an individual job.
    """

    def __init__(
        self,
        userid,
        jobid,
        t_submit,
        t_run,
        t_inactive,
        nnodes,
        resources,
        project,
        bank,
    ):
        self.userid = userid
        self.username = get_username(userid)
        self.jobid = jobid
        self.t_submit = t_submit
        self.t_run = t_run
        self.t_inactive = t_inactive
        self.nnodes = nnodes
        self.resources = resources
        self.project = project
        self.bank = bank

    @property
    def elapsed(self):
        return self.t_inactive - self.t_run

    @property
    def queued(self):
        return self.t_run - self.t_submit


def write_records_to_file(job_records, output_file):
    with open(output_file, "w", newline="") as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter="|", escapechar="'", quoting=csv.QUOTE_NONE
        )
        spamwriter.writerow(
            (
                "UserID",
                "Username",
                "JobID",
                "T_Submit",
                "T_Run",
                "T_Inactive",
                "Nodes",
                "R",
                "Project",
                "Bank",
            )
        )
        for record in job_records:
            spamwriter.writerow(
                (
                    str(record.userid),
                    str(record.username),
                    str(record.jobid),
                    str(record.t_submit),
                    str(record.t_run),
                    str(record.t_inactive),
                    str(record.nnodes),
                    str(record.resources),
                    str(record.project),
                    str(record.bank),
                )
            )


def convert_to_str(job_records, fmt_string=None):
    """
    Convert the results of a query to the jobs table to a readable string
    that can either be output to stdout or written to a file.
    """
    # default format string
    if not fmt_string:
        fmt_string = (
            "{jobid:<15} | {username:<8} | {userid:<8} | {t_submit:<15.2f} | "
            + "{t_run:<15.2f} | {t_inactive:<15.2f} | {nnodes:<8} | {project:<8} | "
            + "{bank:<8}"
        )
    output = fmt.JobsFormatter(fmt_string)
    job_record_str = output.build_table(job_records)

    return job_record_str


def convert_to_obj(rows):
    """
    Convert the results of a query to the jobs table to a list of JobRecord
    objects.
    """
    job_records = []

    for row in rows:
        try:
            # attempt to create a ResourceSet from R
            rset = ResourceSet(row[6])
            job_nnodes = rset.nnodes
        except (ValueError, TypeError):
            # can't convert R to a ResourceSet object; skip it
            continue

        job_record = JobRecord(
            userid=row[0],
            jobid=row[1],
            t_submit=row[2],
            t_run=row[3],
            t_inactive=row[4],
            nnodes=job_nnodes,
            resources=row[6],
            project=row[8] if row[8] is not None else "",
            bank=row[9] if row[9] is not None else "",
        )
        job_records.append(job_record)

    return job_records


def check_jobspec(jobspec, bank):
    """
    Check if 1) a "bank" attribute exists in jobspec, which means the user
    submitted a job under a secondary bank, and 2) the "bank" attribute in
    jobspec matches the bank we are currently counting jobs for.
    """
    return bool(
        ("bank" in jobspec["attributes"]["system"])
        and (jobspec["attributes"]["system"]["bank"] == bank)
    )


def filter_jobs_by_bank(job_records, bank, is_default_bank=False):
    """
    Filter job records based on the specified bank. For a default bank, it
    includes jobs that either specify the default bank or do not specify any
    bank at all.
    """
    jobs = []
    for job in job_records:
        jobspec = json.loads(job[7])

        if check_jobspec(jobspec, bank):
            jobs.append(job)
        elif is_default_bank and "bank" not in jobspec["attributes"]["system"]:
            jobs.append(job)

    return jobs


def filter_jobs_by_association(conn, bank, default_bank, **kwargs):
    """
    Filter job records based on the specified association.
    """
    # fetch jobs under a specific userid
    result = get_jobs(conn, **kwargs)

    if not result:
        return []

    # find out if we are fetching jobs from an association's default bank or
    # under one of their secondary banks; this will determine how we further
    # filter the job records we've found based on the bank
    is_default_bank = bank == default_bank
    jobs = filter_jobs_by_bank(result, bank, is_default_bank)

    return convert_to_obj(jobs)


def get_jobs(conn, **kwargs):
    """
    A function to return jobs from the jobs table in the flux-accounting
    database. The query can be tuned to filter jobs by:

    - userid
    - jobs that started after a certain time
    - jobs that completed before a certain time
    - jobid
    - project
    - bank

    The function will execute a SQL query and return a list of jobs. If no
    jobs are found, an empty list is returned.
    """
    # find out which args were passed and place them in a dict
    valid_params = {
        "user",
        "after_start_time",
        "before_end_time",
        "jobid",
        "project",
        "bank",
    }
    params = {
        key: val
        for key, val in kwargs.items()
        if val is not None and key in valid_params
    }

    select_stmt = "SELECT userid,id,t_submit,t_run,t_inactive,ranks,R,jobspec,project,bank FROM jobs"
    where_clauses = []
    params_list = []

    if "user" in params:
        params["user"] = get_uid(params["user"])
        where_clauses.append("userid = ?")
        params_list.append(params["user"])
    if "after_start_time" in params:
        where_clauses.append("t_run > ?")
        params_list.append(parse_timestamp(params["after_start_time"]))
    if "before_end_time" in params:
        where_clauses.append("t_inactive < ?")
        params_list.append(parse_timestamp(params["before_end_time"]))
    if "jobid" in params:
        # convert jobID passed-in to decimal format
        params["jobid"] = JobID(params["jobid"]).dec
        where_clauses.append("id = ?")
        params_list.append(params["jobid"])
    if "project" in params:
        where_clauses.append("project = ?")
        params_list.append(params["project"])
    if "bank" in params:
        where_clauses.append("bank = ?")
        params_list.append(params["bank"])

    if where_clauses:
        select_stmt += " WHERE " + " AND ".join(where_clauses)

    cur = conn.cursor()
    cur.execute(select_stmt, tuple(params_list))
    job_records = cur.fetchall()

    return job_records


def view_jobs(conn, output_file, fields, **kwargs):
    # look up jobs in jobs table
    job_records = convert_to_obj(get_jobs(conn, **kwargs))
    # convert query result to a readable string
    job_records_str = convert_to_str(job_records, fields)

    if output_file is None:
        return job_records_str

    write_records_to_file(job_records, output_file)

    return job_records_str
