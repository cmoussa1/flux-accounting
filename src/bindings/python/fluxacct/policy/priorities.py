#!/usr/bin/env python3

###############################################################
# Copyright 2025 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import json

import flux
from flux.job.JobID import JobID
from fluxacct.database import schema
from fluxacct.database import sql
from fluxacct.policy import constants
from fluxacct.policy import expression
from fluxacct.formatting import formatters as fmt
from fluxacct import util


###############################################################
#                                                             #
#                          Classes                            #
#                                                             #
###############################################################
class Association:
    """
    An association (a tuple of username+bank) in the flux-accounting DB.
    Args:
        username: the username of the association.
        bank: the name of the bank of the association.
        fairshare: the association's fair-share value.
    """

    def __init__(self, username, bank, fairshare):
        self.username = username
        self.bank = bank
        self.fairshare = fairshare


class Bank:
    """
    A bank in the flux-accounting DB.
    Args:
        name: the name of the bank.
        priority: the priority associated with the bank.
    """

    def __init__(self, name, priority):
        self.name = name
        self.priority = priority


class Queue:
    """
    A queue in the flux-accounting DB.
    Args:
        name: the name of the queue.
        priority: the priority associated with the queue.
    """

    def __init__(self, name, priority):
        self.name = name
        self.priority = priority


DEFAULT_PRIORITY_WEIGHTS = {
    "fairshare": constants.FSHARE_WEIGHT_DEFAULT,
    "queue": constants.QUEUE_WEIGHT_DEFAULT,
    "bank": constants.BANK_WEIGHT_DEFAULT,
    "urgency": constants.URGENCY_WEIGHT_DEFAULT,
}


###############################################################
#                                                             #
#                     Helper Functions                        #
#                                                             #
###############################################################
def initialize_associations(cur, username, bank=None):
    """
    Create Association objects to be used when generating output for the
    "jobs" command.

    Args:
        cur: A SQLite Cursor object used to execute a query.
        username: The username of the association.
        bank: The bank name of the association. If no bank is specified, the
            query will fetch every association that has the specified username.
    """
    associations = {}
    s_assocs = "SELECT username,bank,fairshare FROM association_table WHERE username=?"

    if bank is not None:
        s_assocs += " AND bank=?"
        cur.execute(
            s_assocs,
            (
                username,
                bank,
            ),
        )
    else:
        cur.execute(s_assocs, (username,))

    result = cur.fetchall()
    if not result:
        raise ValueError(f"could not find entry for {username} in association_table")

    for row in result:
        associations[(row["username"], row["bank"])] = Association(
            username=row["username"], bank=row["bank"], fairshare=row["fairshare"]
        )

    return associations


def initialize_banks(cur, bank=None):
    """
    Create Bank objects to be used when generating output for the "jobs"
    command.

    Args:
        cur: A SQLite Cursor object used to execute a query.
        bank: The bank name. If no bank is specified, the query will fetch
            every bank and its associated priority.
    """
    banks = {}
    s_bank_prio = "SELECT bank,priority FROM bank_table"

    if bank is not None:
        s_bank_prio += " WHERE bank=?"
        cur.execute(s_bank_prio, (bank,))
    else:
        cur.execute(s_bank_prio)

    result = cur.fetchall()
    if not result:
        raise ValueError(f"could not find entry for {bank} in bank_table")

    for row in result:
        banks[row["bank"]] = Bank(name=row["bank"], priority=row["priority"])

    return banks


def initialize_queues(cur, queue=None):
    """
    Create Queue objects to be used when generating output for the "jobs"
    command.

    Args:
        cur: A SQLite Cursor object used to execute a query.
        queue: The queue name. If no queue is specified, the query will fetch
            every queue and its associated priority.
    """
    queues = {}
    s_queue_prio = "SELECT queue,priority FROM queue_table"

    if queue is not None:
        s_queue_prio += " WHERE queue=?"
        cur.execute(s_queue_prio, (queue,))
    else:
        cur.execute(s_queue_prio)

    result = cur.fetchall()
    if not result:
        # the query failed to fetch any results; since queues might not be
        # configured in flux-accounting, just return an empty dict
        return {}

    for row in result:
        queues[row["queue"]] = Queue(name=row["queue"], priority=row["priority"])

    return queues


def as_format_string(column_names, rows, format_string):
    """
    Format a list of rows (as lists) using a format string and column names.

    Args:
        column_names: The names of the columns, used as format placeholders.
        rows: Rows of data.
        format_string: A format string with placeholders like {COLNAME}.
    """
    try:
        header = format_string.format(**{col: col for col in column_names})
        formatted_rows = [
            format_string.format(**dict(zip(column_names, row))) for row in rows
        ]
        return "\n".join([header] + formatted_rows)
    except KeyError as exc:
        raise ValueError(
            f"Invalid column name in format string: {exc.args[0]}.\n"
            f"Available columns: {', '.join(column_names)}"
        )


def _priority_weights(conn):
    cur = conn.cursor()
    weights = DEFAULT_PRIORITY_WEIGHTS.copy()
    cur.execute("SELECT factor,weight FROM priority_factor_weight_table")
    for row in cur.fetchall():
        weights[row["factor"]] = row["weight"]
    return weights


def _deny_unknown_queues(conn):
    cur = conn.cursor()
    cur.execute("SELECT value FROM config_table WHERE key='deny_unknown_queues'")
    row = cur.fetchone()
    return bool(row and str(row["value"]).lower() == "true")


def _default_bank(cur, username):
    cur.execute(
        "SELECT default_bank FROM association_table WHERE username=? LIMIT 1",
        (username,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return row["default_bank"]


def _fetch_associations(cur, username, bank=None, all_banks=False):
    if all_banks:
        cur.execute(
            """
            SELECT * FROM association_table
            WHERE username=?
            ORDER BY bank
            """,
            (username,),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(f"missing association for user {username}")
        return rows

    if bank is None:
        bank = _default_bank(cur, username)
        if bank is None:
            raise ValueError(f"missing association for user {username}")

    cur.execute(
        """
        SELECT * FROM association_table
        WHERE username=? AND bank=?
        """,
        (username, bank),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError(f"missing association for user {username} bank {bank}")
    return [row]


def _bank_priorities(cur):
    cur.execute("SELECT bank,priority FROM bank_table")
    return {row["bank"]: row["priority"] for row in cur.fetchall()}


def _queue_priorities(cur):
    cur.execute("SELECT queue,priority FROM queue_table")
    return {row["queue"]: row["priority"] for row in cur.fetchall()}


def _queue_list(value):
    if not value:
        return []
    return [queue.strip() for queue in value.split(",") if queue.strip()]


def _estimate_row(
    row, queue, urgency, weights, bank_priorities, queue_priorities, deny_unknown
):
    queue_priority = 0
    accepted = True
    rejection_reason = ""
    queues = _queue_list(row["queues"])

    if row["active"] == 0:
        accepted = False
        rejection_reason = "user/bank entry has been disabled from accounting DB"
    elif queue is not None and queue not in queue_priorities and deny_unknown:
        accepted = False
        rejection_reason = (
            f'queue "{queue}" is unknown to flux-accounting and '
            "deny-unknown-queues is enabled"
        )
    elif queue is not None and queue in queue_priorities and queue not in queues:
        accepted = False
        rejection_reason = (
            f'queue "{queue}" not valid for user; valid queues for user: '
            f'{",".join(queues)}'
        )
    elif queue is not None and queue in queue_priorities:
        queue_priority = queue_priorities[queue]

    variables = {
        "fairshare": row["fairshare"],
        "queue_priority": queue_priority,
        "bank_priority": bank_priorities.get(row["bank"], 0.0),
        "urgency": urgency,
        "w_fairshare": weights["fairshare"],
        "w_queue": weights["queue"],
        "w_bank": weights["bank"],
        "w_urgency": weights["urgency"],
    }
    priority = None
    if accepted:
        priority, _ = expression.calculate_priority(urgency, variables)

    return {
        "username": row["username"],
        "userid": row["userid"],
        "bank": row["bank"],
        "default_bank": row["default_bank"],
        "queue": "" if queue is None else queue,
        "fairshare": row["fairshare"],
        "queue_priority": queue_priority,
        "bank_priority": bank_priorities.get(row["bank"], 0.0),
        "urgency": urgency,
        "w_fairshare": weights["fairshare"],
        "w_queue": weights["queue"],
        "w_bank": weights["bank"],
        "w_urgency": weights["urgency"],
        "accepted": accepted,
        "rejection_reason": rejection_reason,
        "priority": priority,
        "limits_evaluated": False,
    }


def _format_estimate_table(rows):
    header = (
        f"{'USER':<10}{'BANK':<10}{'QUEUE':<10}{'FAIRSHARE':<11}"
        f"{'QPRIO':<8}{'BANKPRIO':<10}{'URGENCY':<8}{'PRIORITY':<12}"
        f"{'STATUS':<10}{'REASON'}"
    )
    lines = [header]
    for row in rows:
        status = "accepted" if row["accepted"] else "rejected"
        priority = "" if row["priority"] is None else row["priority"]
        lines.append(
            f"{row['username']:<10}{row['bank']:<10}{row['queue']:<10}"
            f"{row['fairshare']:<11}{row['queue_priority']:<8}"
            f"{row['bank_priority']:<10}{row['urgency']:<8}{priority:<12}"
            f"{status:<10}{row['rejection_reason']}"
        )
    return "\n".join(lines)


def _format_estimate_verbose(rows):
    lines = []
    for row in rows:
        lines.append(f"{row['username']}/{row['bank']}")
        if not row["accepted"]:
            lines.append(f"rejected: {row['rejection_reason']}")
            continue
        lines.append(
            "= (FAIRSHARE * FAIRSHARE_WEIGHT) + "
            "(QUEUE_PRIORITY * QUEUE_WEIGHT) + "
            "(BANK_PRIORITY * BANK_WEIGHT) + "
            "(URGENCY_WEIGHT * (URGENCY - 16))"
        )
        lines.append(
            f"= ({row['fairshare']} * {row['w_fairshare']}) + "
            f"({row['queue_priority']} * {row['w_queue']}) + "
            f"({row['bank_priority']} * {row['w_bank']}) + "
            f"({row['w_urgency']} * ({row['urgency']} - 16))"
        )
        lines.append(f"= {row['priority']}")
    return "\n".join(lines)


def _estimate_notes():
    return "\n".join(
        [
            "",
            "Notes:",
            "  Accounting limits are not evaluated by this estimate.",
            "  Omitted queues and permitted unknown queues use queue priority 0.",
            "  Estimates use the current database and may lag the loaded plugin.",
        ]
    )


def construct_priority_calculation(job, priority_weights, queues, banks):
    """
    Construct a string detailing how a job's priority was calculated.

    Args:
        job: The job for which priority is being calculated.
        priority_weights: A dictionary containing the weight for each factor responsible
            for calculating a job's priority.
        queues: A dictionary containing queues and their associated priorities.
        banks: A dictionary containing banks and their associated priorities.
    """
    # a specific job ID or set of job IDs were passed; walk through calculation
    row = f"{JobID(job.id).f58}\n"
    # display header
    row += (
        "= (FAIRSHARE * FAIRSHARE_WEIGHT) + (QUEUE_PRIORITY * QUEUE_WEIGHT) + "
        "(BANK_PRIORITY * BANK_WEIGHT) + (URGENCY_WEIGHT * (URGENCY - 16))\n"
    )
    row += (
        f"= ({job.annotations.user.fairshare} * "
        f"{priority_weights.get('fairshare', 0)}) + "
        f"({getattr(queues.get(job.queue), 'priority', 0)} * "
        f"{priority_weights.get('queue', 0)}) + "
        f"({banks[job.bank].priority} * {priority_weights.get('bank', 0)}) + "
        f"({priority_weights.get('urgency', 0)} * ({job.urgency} - 16))\n"
    )
    # calculate individual factors
    comp_fshare = job.annotations.user.fairshare * priority_weights.get("fairshare", 0)
    comp_queue = getattr(queues.get(job.queue), "priority", 0) * priority_weights.get(
        "queue", 0
    )
    comp_bank = banks[job.bank].priority * priority_weights.get("bank", 0)
    comp_urgency = priority_weights.get("urgency", 0) * (job.urgency - 16)
    row += f"= {comp_fshare} + {comp_queue} + {comp_bank} + {comp_urgency}\n"
    # display final priority
    row += f"= {job.priority}\n"

    return row


###############################################################
#                                                             #
#                   Subcommand Functions                      #
#                                                             #
###############################################################
def view_factor(conn, factor, json_fmt=False, format_string=""):
    """
    View the integer weight for a particular priority factor in the plugin.

    Args:
        conn: the SQLite Connection object.
        factor: the name of the priority factor.
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM priority_factor_weight_table WHERE factor=?", (factor,))
    formatter = fmt.PriorityFactorFormatter(cur, factor)
    if format_string != "":
        return formatter.as_format_string(format_string)
    if json_fmt:
        return formatter.as_json()
    return formatter.as_table()


def edit_factor(conn, factor, weight):
    """
    Edit the integer weight for a particular priority factor in the plugin.

    Args:
        conn: the SQLite Connection object.
        factor: the name of the priority factor.
        weight: the new integer weight associated with the priority factor.
    """
    if factor not in constants.PRIORITY_FACTORS:
        raise ValueError(
            f"factor {factor} not found in priority_factor_weight_table; "
            f"available factors are {','.join(constants.PRIORITY_FACTORS)}"
        )
    cur = conn.cursor()
    cur.execute(
        "UPDATE priority_factor_weight_table SET weight=? WHERE factor=?",
        (
            weight,
            factor,
        ),
    )
    conn.commit()

    return 0


def list_factors(conn, cols=None, json_fmt=False, format_string=""):
    """
    List all factors in priority_factor_weight_table.

    Args:
        cols: a list of columns from the table to include in the output. By default, all
            columns are included.
        json_fmt: output data in JSON format. By default, the format of any returned data
            returned data is in a table format.
        format_string: a format string defining how each row should be formatted. Column
            names should be used as placeholders.
    """
    # use all column names if none are passed in
    cols = cols or schema.PRIORITY_FACTOR_WEIGHTS_TABLE

    cur = conn.cursor()

    sql.validate_columns(cols, schema.PRIORITY_FACTOR_WEIGHTS_TABLE)
    # construct SELECT statement
    select_stmt = f"SELECT {', '.join(cols)} FROM priority_factor_weight_table"
    cur.execute(select_stmt)

    # initialize AccountingFormatter object
    formatter = fmt.AccountingFormatter(cur)
    if format_string != "":
        return formatter.as_format_string(format_string)
    if json_fmt:
        return formatter.as_json()
    return formatter.as_table()


def reset_factors(conn):
    """
    Reset the configuration for the priority factors in priority_factor_weight_table
    by re-inserting the factors and their original weight back in the database.

    Args:
        conn: the SQLite Connection object.
    """
    cur = conn.cursor()

    cur.execute(
        f"INSERT INTO priority_factor_weight_table (factor, weight) "
        f"VALUES ('fairshare', {constants.FSHARE_WEIGHT_DEFAULT}) "
        f"ON CONFLICT(factor) DO UPDATE SET weight = excluded.weight;"
    )
    cur.execute(
        f"INSERT INTO priority_factor_weight_table (factor, weight) "
        f"VALUES ('queue', {constants.QUEUE_WEIGHT_DEFAULT}) "
        f"ON CONFLICT(factor) DO UPDATE SET weight = excluded.weight;"
    )
    cur.execute(
        f"INSERT INTO priority_factor_weight_table (factor, weight) "
        f"VALUES ('bank', {constants.BANK_WEIGHT_DEFAULT}) "
        f"ON CONFLICT(factor) DO UPDATE SET weight = excluded.weight;"
    )
    cur.execute(
        f"INSERT INTO priority_factor_weight_table (factor, weight) "
        f"VALUES ('urgency', {constants.URGENCY_WEIGHT_DEFAULT}) "
        f"ON CONFLICT(factor) DO UPDATE SET weight = excluded.weight;"
    )

    conn.commit()
    return 0


def estimate_priority(
    conn,
    username,
    bank=None,
    all_banks=False,
    queue=None,
    urgency=expression.URGENCY_DEFAULT,
    json_fmt=False,
    format_string="",
    verbose=False,
):
    """
    Estimate the priority a submitted job would receive from the plugin.

    Args:
        conn: the SQLite Connection object.
        username: the user whose association should be estimated.
        bank: the bank to charge.
        all_banks: estimate every association for the user.
        queue: optional queue name.
        urgency: Flux job urgency.
        json_fmt: output data in JSON format.
        format_string: optional custom format string.
        verbose: include calculation details.
    """
    urgency = int(urgency)
    if urgency < expression.URGENCY_HOLD or urgency > expression.URGENCY_EXPEDITE:
        raise ValueError("urgency must be between 0 and 31")

    cur = conn.cursor()
    associations = _fetch_associations(cur, username, bank, all_banks)
    weights = _priority_weights(conn)
    bank_priorities = _bank_priorities(cur)
    queue_priorities = _queue_priorities(cur)
    deny_unknown = _deny_unknown_queues(conn)
    rows = [
        _estimate_row(
            row,
            queue,
            urgency,
            weights,
            bank_priorities,
            queue_priorities,
            deny_unknown,
        )
        for row in associations
    ]

    if not all_banks and rows and not rows[0]["accepted"]:
        raise ValueError(rows[0]["rejection_reason"])

    if format_string:
        column_names = list(rows[0].keys()) if rows else []
        return as_format_string(
            column_names,
            [[row[column] for column in column_names] for row in rows],
            format_string,
        )
    if json_fmt:
        return json.dumps(rows)
    if verbose:
        return _format_estimate_verbose(rows) + _estimate_notes()
    return _format_estimate_table(rows) + _estimate_notes()


def job_priorities(
    conn,
    username,
    bank=None,
    queue=None,
    format_string=None,
    filters=None,
    max_entries=0,
    since="0.0",
    jobids=None,
    verbose=False,
):
    """
    List a breakdown for the priority calculation for every active job for a given
    username. Filter the user's jobs by bank and/or by queue.
    Args:
        conn: the SQLite Connection object.
        username: the username of the association.
        bank: filter jobs by a bank.
        queue: filter jobs by a queue.
        format_string: optional format string for custom output.
        states: filter jobs by specific states.
        max_entries: the maximum number of job records to return.
        since: filter jobs that have become inactive since a certain timestamp.
    """
    handle = flux.Flux()
    cur = conn.cursor()

    # initialize all associations that have the username passed in and the
    # priority associated with any banks or queues (if no bank or queue is
    # passed in, *every* bank and queue's priority will be fetched)
    associations = initialize_associations(cur, username, bank)
    banks = initialize_banks(cur, bank)
    queues = initialize_queues(cur, queue)
    # fetch integer weight associated with each priority factor
    factors = list_factors(conn, json_fmt=True)
    priority_weights = {item["factor"]: item["weight"] for item in json.loads(factors)}

    # convert a potential string timestamp (e.g. "2025-05-20 08:00:00") to a
    # seconds-since-epoch timestamp
    since = float(util.parse_timestamp(since))
    # ensure args.since is in the past
    if since > flux.util.parse_datetime("now").timestamp():
        raise ValueError(f"--since appears to be in the future: {since}")

    joblist = (
        flux.job.JobList(
            handle,
            max_entries=max_entries,
            user=username,
            queue=queue,
            since=since,
            ids=jobids,
        )
        if queue
        else flux.job.JobList(
            handle,
            max_entries=max_entries,
            user=username,
            since=since,
            ids=jobids,
        )
    )
    if filters:
        for filt in filters.split(","):
            joblist.add_filter(filt)
    jobs = list(joblist.jobs())

    if verbose:
        rows = ""
        for job in jobs:
            rows += construct_priority_calculation(job, priority_weights, queues, banks)
        return rows

    row_dicts = []
    for job in jobs:
        # the following if-condition will return all jobs (regardless of bank) if one
        # is not specified or will just filter jobs based on a specific bank since
        # JobList() does not filter on bank
        if bank is None or job.bank == bank:
            row = {
                "JOBID": JobID(job.id).f58,
                "USER": job.username,
                "BANK": job.bank,
                "BANKPRIO": banks[job.bank].priority,
                "BANKFACT": priority_weights.get("bank", 0),
                "QUEUE": job.queue,
                # if queues are not configured in flux-accounting, the "queues" dict
                # will have no values in it, so we need to make sure that this has a
                # default of 0 to indicate it is not affecting a job's priority
                "QPRIO": getattr(queues.get(job.queue), "priority", 0),
                "QFACT": priority_weights.get("queue", 0),
                "FAIRSHARE": job.annotations.user.fairshare,
                "FSFACTOR": priority_weights.get("fairshare", 0),
                "URGENCY": job.urgency,
                "URGFACT": priority_weights.get("urgency", 0),
                "PRIORITY": job.priority,
            }
            row_dicts.append(row)

    if format_string:
        column_names = list(row_dicts[0].keys()) if row_dicts else []
        rows = [list(row.values()) for row in row_dicts]
        return as_format_string(column_names, rows, format_string)

    # use default formatting
    header = (
        f"{'JOBID':<15}{'USER':<9}"
        f"{'BANK':<8}{'BANKPRIO':<10}{'BANKFACT':<10}"
        f"{'QUEUE':<8}{'QPRIO':<7}{'QFACT':<7}"
        f"{'FAIRSHARE':<10}{'FSFACTOR':<10}"
        f"{'URGENCY':<8}{'URGFACT':<8}"
        f"{'PRIORITY':<8}"
    )
    rows = [
        f"{r['JOBID']:<15}{r['USER']:<9}"
        f"{r['BANK']:<8}{r['BANKPRIO']:<10}{r['BANKFACT']:<10}"
        f"{r['QUEUE']:<8}{r['QPRIO']:<7}{r['QFACT']:<7}"
        f"{r['FAIRSHARE']:<10}{r['FSFACTOR']:<10}"
        f"{r['URGENCY']:<8}{r['URGFACT']:<8}"
        f"{r['PRIORITY']:<8}"
        for r in row_dicts
    ]
    return f"{header}\n" + "\n".join(rows)
