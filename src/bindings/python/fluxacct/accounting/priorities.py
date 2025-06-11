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
import flux
from flux.job.JobID import JobID


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


def initialize_associations(cur, username, bank=None):
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
    queues = {}
    s_queue_prio = "SELECT queue,priority FROM queue_table"

    if queue is not None:
        s_queue_prio += " WHERE queue=?"
        cur.execute(s_queue_prio, (queue,))
    else:
        cur.execute(s_queue_prio)

    result = cur.fetchall()
    if not result:
        raise ValueError(
            f"could not find entry for {queue} in queue_table "
            f"or queues not configured in flux-accounting"
        )

    for row in result:
        queues[row["queue"]] = Queue(name=row["queue"], priority=row["priority"])

    return queues


def list_job_priorities(conn, username, bank=None, queue=None, config=None):
    """
    List a breakdown for the priority calculation for every active job for a given
    username. Filter the user's jobs by bank and/or by queue.

    Args:
        conn: the SQLite Connection object.
        username: the username of the association.
        bank: filter jobs by a bank.
        queue: filter jobs by a queue.
    """
    handle = flux.Flux()
    cur = conn.cursor()
    priority_factors = {"fairshare": 100000, "queue": 10000, "bank": 0}

    # initialize all associations that have the username passed in and the
    # priority associated with any banks or queues if they are passed-in (if not,
    # priority information for *every* bank and *queue* will be fetched)
    associations = initialize_associations(cur, username, bank)
    banks = initialize_banks(cur, bank)
    queues = initialize_queues(cur, queue)

    header = (
        f"{'JOBID':<15}{'USER':<9}{'BANK':<8}{'BANKPRIO':<10}{'BANKFACT':<10}"
        f"{'QUEUE':<8}{'QPRIO':<7}{'QFACT':<7}{'FAIRSHARE':<10}{'FSFACTOR':<10}{'PRIORITY':<8}"
    )

    if queue is not None:
        # only fetch jobs that have been submitted under a certain queue
        joblist = flux.job.JobList(handle, max_entries=0, user=username, queue=queue)
    else:
        joblist = flux.job.JobList(handle, max_entries=0, user=username)
    jobs = list(joblist.jobs())

    rows = []
    for job in jobs:
        if bank is None or job.bank == bank:
            row = (
                f"{JobID(job.id).f58:<15}{job.username:<9}"
                f"{job.bank:<8}{getattr(banks.get(job.bank), 'priority', 0):<10}{priority_factors['bank']:<10}"
                f"{job.queue:<8}{getattr(queues.get(job.queue), 'priority', 0):<7}{priority_factors['queue']:<7}"
                f"{getattr(associations.get((job.username, job.bank)), 'fairshare', 0.0):<10}{priority_factors['fairshare']:<10}"
                f"{job.priority:<8}"
            )
            rows.append(row)

    return f"{header}\n" + "\n".join(rows)
