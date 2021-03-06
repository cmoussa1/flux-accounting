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

import pandas as pd


def count_ranks(ranks):
    if "-" in ranks:
        ranks_count = ranks.replace("-", ",").split(",")
        return int(ranks_count[1]) - int(ranks_count[0]) + 1
    else:
        return int(ranks) + 1


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
                    str(record.R),
                )
            )


def print_job_records(job_records):
    records = {}
    userid_arr = []
    username_arr = []
    jobid_arr = []
    t_submit_arr = []
    t_run_arr = []
    t_inactive_arr = []
    nnodes_arr = []
    R_arr = []

    for record in job_records:
        userid_arr.append(record.userid)
        username_arr.append(record.username),
        jobid_arr.append(record.jobid),
        t_submit_arr.append(record.t_submit),
        t_run_arr.append(record.t_run),
        t_inactive_arr.append(record.t_inactive),
        nnodes_arr.append(record.nnodes),
        R_arr.append(record.R),

    records = {
        "UserID": userid_arr,
        "Username": username_arr,
        "JobID": jobid_arr,
        "T_Submit": t_submit_arr,
        "T_Run": t_run_arr,
        "T_Inactive": t_inactive_arr,
        "Nodes": nnodes_arr,
        "R": R_arr,
    }

    dataframe = pd.DataFrame(
        records,
        columns=[
            "UserID",
            "Username",
            "JobID",
            "T_Submit",
            "T_Run",
            "T_Inactive",
            "Nodes",
            "R",
        ],
    )
    pd.set_option("max_colwidth", 100)
    pd.set_option("display.float_format", lambda x: "%.5f" % x)
    print(dataframe)


class JobRecord(object):
    """
    A record of an individual job.
    """

    def __init__(self, userid, username, jobid, t_submit, t_run, t_inactive, nnodes, R):
        self.userid = userid
        self.username = get_username(userid)
        self.jobid = jobid
        self.t_submit = t_submit
        self.t_run = t_run
        self.t_inactive = t_inactive
        self.nnodes = nnodes
        self.R = R
        return None

    @property
    def elapsed(self):
        return self.t_inactive - self.t_run

    @property
    def queued(self):
        return self.t_run - self.t_submit


def add_job_records(dataframe):
    job_records = []

    for index, row in dataframe.iterrows():
        job_record = JobRecord(
            row["userid"],
            get_username(row["userid"]),
            row["id"],
            row["t_submit"],
            row["t_run"],
            row["t_inactive"],
            count_ranks(row["ranks"]),
            row["R"],
        )
        job_records.append(job_record)

    return job_records


def view_job_records(conn, output_file, **kwargs):
    job_records = []

    # find out which args were passed and place them in a dict
    valid_params = ("user", "after_start_time", "before_end_time", "jobid")
    params = {}
    params_list = []

    params = {
        key: val for (key, val) in kwargs.items() if val != None and key in valid_params
    }

    select_stmt = "SELECT userid,id,t_submit,t_run,t_inactive,ranks,R FROM jobs "
    where_stmt = ""

    def append_to_where(where_stmt, conditional):
        if where_stmt != "":
            return "{} AND {} ".format(where_stmt, conditional)
        else:
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

    dataframe = pd.read_sql_query(select_stmt, conn, params=((*tuple(params_list),)))
    # if the length of dataframe is 0, that means
    # no job records were found in the jobs table,
    # so just return an empty list
    if len(dataframe.index) == 0:
        return job_records
    else:
        job_records = add_job_records(dataframe)
        if output_file is None:
            print_job_records(job_records)
        else:
            write_records_to_file(job_records, output_file)

    return job_records


def fetch_old_usage_factors(acct_conn, user=None, bank=None):
    past_usage_factors = []

    select_stmt = "SELECT * from job_usage_factor_table WHERE username=? AND bank=?"
    dataframe = pd.read_sql_query(
        select_stmt,
        acct_conn,
        params=(
            user,
            bank,
        ),
    )

    for val in dataframe.iloc[0].values[3:]:
        if isinstance(val, float):
            past_usage_factors.append(val)

    return past_usage_factors


def apply_decay_factor(decay_factor, acct_conn, user=None, bank=None):
    past_usage_factors = []
    past_usage_factors_w_decay = []

    past_usage_factors = fetch_old_usage_factors(acct_conn, user, bank)

    # apply decay factor to past usage periods of a user's jobs
    for power, usage_factor in enumerate(past_usage_factors, start=1):
        past_usage_factors_w_decay.append(usage_factor * math.pow(decay_factor, power))

    # update job_usage_factor_table with new values, starting with period-2;
    # the last usage factor in the table will get discarded after the update
    period = 1
    for usage_factor in past_usage_factors_w_decay[
        1 : len(past_usage_factors_w_decay) - 1
    ]:
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

    return sum(past_usage_factors_w_decay)


def calc_usage_factor(
    jobs_conn,
    acct_conn,
    priority_decay_half_life,
    user,
    bank,
):

    # half_life_period represents the number of weeks (converted to
    # seconds) that represents one usage bin
    half_life_period = priority_decay_half_life * 604800

    # fetch timestamp of last seen job (will fetch jobs that
    # have run after this time)
    fetch_timestamp_query = """
        SELECT last_job_timestamp
        FROM job_usage_factor_table
        WHERE username=? AND bank=?
        """
    dataframe = pd.read_sql_query(
        fetch_timestamp_query,
        acct_conn,
        params=(
            user,
            bank,
        ),
    )
    last_job_timestamp = dataframe.iloc[0]

    # fetch timestamp of the end of the current half-life period
    fetch_half_life_timestamp_query = """
        SELECT end_half_life_period
        FROM t_half_life_period_table
        """
    dataframe = pd.read_sql_query(fetch_half_life_timestamp_query, acct_conn)
    t_end_half_life_period = dataframe.iloc[0]

    # get the total number of nodes and time elapsed across
    # all of a user's jobs that completed since the last completed
    # job that was retrieved
    job_totals_user = view_job_records(
        jobs_conn,
        output_file=None,
        user=user,
        after_start_time=float(last_job_timestamp),
    )

    last_t_inactive = 0.0
    usage_user_current = 0.0

    if len(job_totals_user) > 0:
        # sort jobs by job.t_inactive
        job_totals_user.sort(key=lambda job: job.t_inactive)

        # one per job factor = nnodes * t_elapsed
        # usage factors = total(nnodes * t_elapsed)
        per_job_factors = []
        for job in job_totals_user:
            per_job_factors.append(round((job.nnodes * job.elapsed), 5))

        last_t_inactive = job_totals_user[-1].t_inactive
        usage_user_current = sum(per_job_factors)

    # if no new jobs were found and we are still in the same half-life
    # period, then the current usage remains the same
    if len(job_totals_user) == 0 and (
        float(t_end_half_life_period) > (time.time() - half_life_period)
    ):
        # fetch past usage factors
        past_usage_factors = fetch_old_usage_factors(acct_conn, user, bank)

        usage_user_historical = sum(past_usage_factors)
    # if no new jobs were found but we are past the most recent half-life
    # period, then the current usage needs to have a decay factor applied
    # to it
    elif len(job_totals_user) == 0 and (
        float(t_end_half_life_period) < (time.time() - half_life_period)
    ):
        # fetch past usage factors
        past_usage_factors = fetch_old_usage_factors(acct_conn, user, bank)

        usage_user_historical = apply_decay_factor(0.5, acct_conn, user, bank)
    # if last_t_inactive - t_end_half_life_period < half_life_period,
    # append newly found jobs to the most recent usage factor
    elif (last_t_inactive - float(t_end_half_life_period)) < half_life_period:
        # append current usage to first usage factor bin
        fetch_current_usage_factor = """
            SELECT usage_factor_period_0
            FROM job_usage_factor_table
            WHERE username=?
            AND bank=?
            """
        dataframe = pd.read_sql_query(
            fetch_current_usage_factor,
            acct_conn,
            params=(
                user,
                bank,
            ),
        )
        usage_factor_period_0 = dataframe.iloc[0]

        usage_user_current += float(usage_factor_period_0)

        # usage_user_past will just be the sum of the older factors since
        # they will already had their decay factor applied to them in this
        # current half-life period
        usage_user_past = fetch_old_usage_factors(acct_conn, user, bank)

        # calculate historical usage factor for user
        usage_user_historical = usage_user_current + sum(usage_user_past[1:])
    # else, create a new bin, move the older factors (and throw out the oldest
    # one), and set a new end timestamp for the next half-life period
    else:
        # apply decay factor to past usage periods of a user's jobs
        usage_user_past = apply_decay_factor(0.5, acct_conn, user, bank)

        # calculate historical usage factor for user
        usage_user_historical = usage_user_current + usage_user_past

    # write last t_inactive to last_job_timestamp for user
    update_timestamp_stmt = """
        UPDATE job_usage_factor_table SET last_job_timestamp=?
        WHERE username=?
        AND bank=?
        """
    acct_conn.execute(
        update_timestamp_stmt,
        (
            last_t_inactive,
            user,
            bank,
        ),
    )
    acct_conn.commit()

    # write historical usage to first column in job_usage_factor_table
    update_stmt = """
        UPDATE job_usage_factor_table
        SET usage_factor_period_0=?
        WHERE username=?
        AND bank=?
        """
    acct_conn.execute(
        update_stmt,
        (
            usage_user_historical,
            user,
            bank,
        ),
    )
    acct_conn.commit()

    # update job_usage column in association_table
    update_usage_stmt = """
        UPDATE association_table
        SET job_usage=?
        WHERE username=?
        AND bank=?
        """
    acct_conn.execute(
        update_usage_stmt,
        (
            usage_user_historical,
            user,
            bank,
        ),
    )
    acct_conn.commit()

    return usage_user_historical


def update_end_half_life_period(acct_conn, priority_decay_half_life):
    # half_life_period represents the number of weeks (converted to
    # seconds) that represents one usage bin
    half_life_period = priority_decay_half_life * 604800

    # fetch timestamp of the end of the current half-life period
    fetch_half_life_timestamp_query = """
        SELECT end_half_life_period
        FROM t_half_life_period_table
        WHERE cluster='cluster'
        """
    dataframe = pd.read_sql_query(fetch_half_life_timestamp_query, acct_conn)
    t_end_half_life_period = dataframe.iloc[0]

    # check to see if we are still in the same half-life period;
    # if not, we need to update the end_half_life_period timestamp
    # with a new timestamp value
    if float(t_end_half_life_period) < (time.time() - half_life_period):
        # update new end of half-life period timestamp
        update_timestamp_stmt = """
            UPDATE t_half_life_period_table
            SET end_half_life_period=?
            WHERE cluster='cluster'
            """
        acct_conn.execute(
            update_timestamp_stmt, ((float(t_end_half_life_period) + half_life_period),)
        )
        acct_conn.commit()
