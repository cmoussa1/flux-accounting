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
import time
import pwd
import csv
import math
import json

from flux.resource import ResourceSet


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


def write_records_to_file(job_records, output_file):
    with open(output_file, "w", newline="") as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter="|", quotechar="", escapechar="'", quoting=csv.QUOTE_NONE
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
                )
            )


def fetch_job_records(job_records):
    job_record_str = []
    job_record_str.append(
        "{:<10} {:<10} {:<20} {:<20} {:<20} {:<20} {:<10}".format(
            "UserID",
            "Username",
            "JobID",
            "T_Submit",
            "T_Run",
            "T_Inactive",
            "Nodes",
        )
    )
    for record in job_records:
        job_record_str.append(
            "{:<10} {:<10} {:<20} {:<20} {:<20} {:<20} {:<10}".format(
                record.userid,
                record.username,
                record.jobid,
                record.t_submit,
                record.t_run,
                record.t_inactive,
                record.nnodes,
            )
        )

    return job_record_str


class JobRecord:
    """
    A record of an individual job.
    """

    def __init__(
        self, userid, _username, jobid, t_submit, t_run, t_inactive, nnodes, resources
    ):
        self.userid = userid
        self.username = get_username(userid)
        self.jobid = jobid
        self.t_submit = t_submit
        self.t_run = t_run
        self.t_inactive = t_inactive
        self.nnodes = nnodes
        self.resources = resources

    @property
    def elapsed(self):
        return self.t_inactive - self.t_run

    @property
    def queued(self):
        return self.t_run - self.t_submit


def add_job_records(rows):
    job_records = []

    for row in rows:
        rset = ResourceSet(row[6])  # fetch R

        job_record = JobRecord(
            row[0],  # userid
            get_username(row[0]),  # username
            row[1],  # jobid
            row[2],  # t_submit
            row[3],  # t_run
            row[4],  # t_inactive
            rset.nnodes,  # nnodes
            row[6],  # resources
        )
        job_records.append(job_record)

    return job_records


# check if 1) a "bank" attribute exists in jobspec, which means the user
# submitted a job under a secondary bank, and 2) the "bank" attribute
# in jobspec matches the bank we are currently counting jobs for
def check_jobspec(jobspec, bank):
    return bool(
        ("bank" in jobspec["attributes"]["system"])
        and (jobspec["attributes"]["system"]["bank"] == bank)
    )


# we are looking for jobs that were submitted under a secondary bank, so we'll
# only add jobs that have the same bank name attribute in the jobspec
def sec_bank_jobs(job_records, bank):
    jobs = []
    for job in job_records:
        jobspec = json.loads(job[7])

        if check_jobspec(jobspec, bank):
            jobs.append(job)

    return jobs


# we are looking for jobs that were submitted under a default bank, which has
# two cases: 1) the user submitted a job while specifying their default bank,
# or 2) the user submitted a job without specifying any bank at all
def def_bank_jobs(job_records, default_bank):
    jobs = []
    for job in job_records:
        jobspec = json.loads(job[7])

        if check_jobspec(jobspec, default_bank):
            jobs.append(job)
        elif "bank" not in jobspec["attributes"]["system"]:
            jobs.append(job)

    return jobs


def get_job_records(conn, bank, default_bank, **kwargs):
    job_records = []

    # find out which args were passed and place them in a dict
    valid_params = ("user", "after_start_time", "before_end_time", "jobid")
    params = {}
    params_list = []

    params = {
        key: val
        for (key, val) in kwargs.items()
        if val is not None and key in valid_params
    }

    select_stmt = (
        "SELECT userid,id,t_submit,t_run,t_inactive,ranks,R,jobspec FROM jobs "
    )
    where_stmt = ""

    def append_to_where(where_stmt, conditional):
        if where_stmt != "":
            return "{} AND {} ".format(where_stmt, conditional)

        return "WHERE {}".format(conditional)

    # generate the SELECT statement based on the parameters passed in
    if "user" in params:
        params["user"] = get_uid(params["user"])
        params_list.append(params["user"])
        where_stmt = append_to_where(where_stmt, "userid=? ")
    if "after_start_time" in params:
        params_list.append(params["after_start_time"])
        where_stmt = append_to_where(where_stmt, "t_run > ? ")
    if "before_end_time" in params:
        params_list.append(params["before_end_time"])
        where_stmt = append_to_where(where_stmt, "t_inactive < ? ")
    if "jobid" in params:
        params_list.append(params["jobid"])
        where_stmt = append_to_where(where_stmt, "id=? ")

    select_stmt += where_stmt

    cur = conn.cursor()
    cur.execute(select_stmt, (*tuple(params_list),))
    rows = cur.fetchall()
    # if the length of dataframe is 0, that means no job records were found
    # in the jobs table, so just return an empty list
    if len(rows) == 0:
        return job_records

    if bank is None and default_bank is None:
        # special case for unit tests in test_job_archive_interface.py
        job_records = add_job_records(rows)

        return job_records

    if bank != default_bank:
        jobs = sec_bank_jobs(rows, bank)
    else:
        jobs = def_bank_jobs(rows, default_bank)

    job_records = add_job_records(jobs)

    return job_records


def output_job_records(conn, output_file, **kwargs):
    job_record_str = ""
    job_records = get_job_records(conn, None, None, **kwargs)

    job_record_str = fetch_job_records(job_records)

    if output_file is None:
        return job_record_str

    write_records_to_file(job_records, output_file)

    return job_record_str


def update_t_inactive(acct_conn, last_t_inactive, user, bank):
    # write last seen t_inactive to last_job_timestamp for user
    u_ts = """
        UPDATE job_usage_factor_table SET last_job_timestamp=? WHERE username=? AND bank=?
        """
    acct_conn.execute(
        u_ts,
        (
            last_t_inactive,
            user,
            bank,
        ),
    )
    acct_conn.commit()


def get_last_job_ts(acct_conn, user, bank):
    # fetch timestamp of last seen job (gets jobs that have run after this time)
    s_ts = """
        SELECT last_job_timestamp FROM job_usage_factor_table WHERE username=? AND bank=?
        """
    cur = acct_conn.cursor()
    cur.execute(
        s_ts,
        (
            user,
            bank,
        ),
    )
    row = cur.fetchone()
    return float(row[0])


def fetch_usg_bins(acct_conn, user=None, bank=None):
    past_usage_factors = []

    select_stmt = "SELECT * from job_usage_factor_table WHERE username=? AND bank=?"
    cur = acct_conn.cursor()
    cur.execute(
        select_stmt,
        (
            user,
            bank,
        ),
    )
    row = cur.fetchone()

    for val in row[4:]:
        if isinstance(val, float):
            past_usage_factors.append(val)

    return past_usage_factors


def update_hist_usg_col(acct_conn, usg_h, user, bank):
    # update job_usage column in association_table
    u_usg = """
        UPDATE association_table SET job_usage=? WHERE username=? AND bank=?
        """
    acct_conn.execute(
        u_usg,
        (
            usg_h,
            user,
            bank,
        ),
    )
    acct_conn.commit()


def update_curr_usg_col(acct_conn, usg_h, user, bank):
    # write usage to first column in job_usage_factor_table
    u_usg_factor = """
        UPDATE job_usage_factor_table SET usage_factor_period_0=? WHERE username=? AND bank=?
        """
    acct_conn.execute(
        u_usg_factor,
        (
            usg_h,
            user,
            bank,
        ),
    )
    acct_conn.commit()


def apply_decay_factor(decay, acct_conn, user=None, bank=None):
    usg_past = []
    usg_past_decay = []

    usg_past = fetch_usg_bins(acct_conn, user, bank)

    # apply decay factor to past usage periods of a user's jobs
    for power, usage_factor in enumerate(usg_past, start=1):
        usg_past_decay.append(usage_factor * math.pow(decay, power))

    # update job_usage_factor_table with new values, starting with period-2;
    # the last usage factor in the table will get discarded after the update
    period = 1
    for usage_factor in usg_past_decay[1 : len(usg_past_decay) - 1]:
        update_stmt = (
            "UPDATE job_usage_factor_table SET usage_factor_period_"
            + str(period)
            + "=? WHERE username=? AND bank=?"
        )
        acct_conn.execute(
            update_stmt,
            (
                str(usage_factor),
                user,
                bank,
            ),
        )
        acct_conn.commit()
        period += 1

    # only return the usage factors up to but not including the oldest one
    # since it no longer affects a user's historical usage factor
    return sum(usg_past_decay[:-1])


def get_curr_usg_bin(acct_conn, user, bank):
    # append current usage to the first usage factor bin
    s_usg = """
        SELECT usage_factor_period_0 FROM job_usage_factor_table
        WHERE username=? AND bank=?
        """
    cur = acct_conn.cursor()
    cur.execute(
        s_usg,
        (
            user,
            bank,
        ),
    )
    row = cur.fetchone()
    return float(row[0])


def calc_usage_factor(conn, pdhl, user, bank, default_bank):

    # hl_period represents the number of seconds that represent one usage bin
    hl_period = pdhl * 604800

    cur = conn.cursor()

    # fetch timestamp of the end of the current half-life period
    s_end_hl = """
        SELECT end_half_life_period FROM t_half_life_period_table WHERE cluster='cluster'
        """
    cur.execute(s_end_hl)
    row = cur.fetchone()
    end_hl = row[0]

    # get jobs that have completed since the last seen completed job
    last_j_ts = get_last_job_ts(conn, user, bank)
    user_jobs = get_job_records(
        conn,
        bank,
        default_bank,
        user=user,
        after_start_time=last_j_ts,
    )

    last_t_inactive = 0.0
    usg_current = 0.0

    if len(user_jobs) > 0:
        user_jobs.sort(key=lambda job: job.t_inactive)

        per_job_factors = []
        for job in user_jobs:
            per_job_factors.append(round((job.nnodes * job.elapsed), 5))

        last_t_inactive = user_jobs[-1].t_inactive
        usg_current = sum(per_job_factors)

        update_t_inactive(conn, last_t_inactive, user, bank)

    if len(user_jobs) == 0 and (float(end_hl) > (time.time() - hl_period)):
        # no new jobs in the current half-life period
        usg_past = fetch_usg_bins(conn, user, bank)

        usg_historical = sum(usg_past)
    elif len(user_jobs) == 0 and (float(end_hl) < (time.time() - hl_period)):
        # no new jobs in the new half-life period
        usg_historical = apply_decay_factor(0.5, conn, user, bank)

        update_hist_usg_col(conn, usg_historical, user, bank)
    elif (last_t_inactive - float(end_hl)) < hl_period:
        # found new jobs in the current half-life period
        usg_current += get_curr_usg_bin(conn, user, bank)

        # usage_user_past = sum of the older usage factors
        usg_past = fetch_usg_bins(conn, user, bank)

        usg_historical = usg_current + sum(usg_past[1:])

        update_curr_usg_col(conn, usg_current, user, bank)
        update_hist_usg_col(conn, usg_historical, user, bank)
    else:
        # found new jobs in the new half-life period

        # apply decay factor to past usage periods of a user's jobs
        usg_past = apply_decay_factor(0.5, conn, user, bank)
        usg_historical = usg_current + usg_past

        update_curr_usg_col(conn, usg_historical, user, bank)
        update_hist_usg_col(conn, usg_historical, user, bank)

    return usg_historical


def check_end_hl(acct_conn, pdhl):
    hl_period = pdhl * 604800

    cur = acct_conn.cursor()

    # fetch timestamp of the end of the current half-life period
    s_end_hl = """
        SELECT end_half_life_period
        FROM t_half_life_period_table
        WHERE cluster='cluster'
        """
    cur.execute(s_end_hl)
    row = cur.fetchone()
    end_hl = row[0]

    if float(end_hl) < (time.time() - hl_period):
        # update new end of half-life period timestamp
        update_timestamp_stmt = """
            UPDATE t_half_life_period_table
            SET end_half_life_period=?
            WHERE cluster='cluster'
            """
        acct_conn.execute(update_timestamp_stmt, ((float(end_hl) + hl_period),))
        acct_conn.commit()


def update_job_usage(acct_conn, pdhl=1):
    s_assoc = "SELECT username, bank, default_bank FROM association_table"
    cur = acct_conn.cursor()
    cur.execute(s_assoc)
    rows = cur.fetchall()

    for row in rows:
        calc_usage_factor(acct_conn, pdhl, row[0], row[1], row[2])

    check_end_hl(acct_conn, pdhl)

    return 0
