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
import csv

from fluxacct.accounting import bank_subcommands as b
from fluxacct.accounting import user_subcommands as u


def export_db_info(conn, users=None, banks=None):
    try:
        cur = conn.cursor()
        select_users_stmt = """
            SELECT username, userid, bank, shares, max_running_jobs, max_active_jobs,
            max_nodes, queues FROM association_table
        """
        cur.execute(select_users_stmt)
        table = cur.fetchall()

        # open a .csv file for writing
        users_filepath = users if users else "users.csv"
        users_file = open(users_filepath, "w")
        with users_file:
            writer = csv.writer(users_file)

            for row in table:
                writer.writerow(row)

        select_banks_stmt = """
            SELECT bank, parent_bank, shares FROM bank_table
        """
        cur.execute(select_banks_stmt)
        table = cur.fetchall()

        banks_filepath = banks if banks else "banks.csv"
        banks_file = open(banks_filepath, "w")
        with banks_file:
            writer = csv.writer(banks_file)

            for row in table:
                writer.writerow(row)
    except IOError as err:
        print(err)


def populate_db(conn, users=None, banks=None):
    if banks is not None:
        try:
            with open(banks) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")

                for row in csv_reader:
                    b.add_bank(
                        conn,
                        bank=row[0],
                        parent_bank=row[1],
                        shares=row[2],
                    )
        except IOError as err:
            print(err)

    if users is not None:
        try:
            with open(users) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=",")

                # assign default values to fields if
                # their slot is empty in the csv file
                for row in csv_reader:
                    username = row[0]
                    uid = row[1]
                    bank = row[2]
                    shares = row[3] if row[3] != "" else 1
                    max_running_jobs = row[4] if row[4] != "" else 5
                    max_active_jobs = row[5] if row[5] != "" else 7
                    max_nodes = row[6] if row[6] != "" else 2147483647
                    queues = row[7]

                    u.add_user(
                        conn,
                        username,
                        bank,
                        uid,
                        shares,
                        max_running_jobs,
                        max_active_jobs,
                        max_nodes,
                        queues,
                    )
        except IOError as err:
            print(err)


def get_db_info(conn):
    """
    Returns the user_version and journal_mode of the SQLite database.

    Args:
        conn: a sqlite3 Connection object to the database.
    """
    cur = conn.cursor()

    # get the user_version
    user_version = cur.execute("PRAGMA user_version").fetchone()[0]
    # get WAL status
    journal_mode = cur.execute("PRAGMA journal_mode").fetchone()[0]

    return f"DB version: {user_version}\nJournal Mode: {journal_mode}"
