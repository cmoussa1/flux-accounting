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
import sqlite3
import json

from fluxacct.accounting import bank_subcommands as b
from fluxacct.accounting import user_subcommands as u
from fluxacct.accounting.util import with_cursor


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
                        username=username,
                        bank=bank,
                        uid=uid,
                        shares=shares,
                        max_running_jobs=max_running_jobs,
                        max_active_jobs=max_active_jobs,
                        max_nodes=max_nodes,
                        queues=queues,
                    )
        except IOError as err:
            print(err)


# @with_cursor
# def init_plugin(conn, cursor, associations=False):
#     """
#     Return a JSON object of certain tables in the flux-accounting database.

#     Args:
#         associations: Create a JSON object of the association_table.
#     """
#     conn.row_factory = sqlite3.Row
#     bulk_user_data = []
#     db_data = ""

#     # fetch all rows from association_table (will print out tuples)
#     for row in cursor.execute(
#         """SELECT userid, bank, default_bank,
#         fairshare, max_running_jobs, max_active_jobs,
#         queues, active, projects, default_project, max_nodes, max_cores
#         FROM association_table"""
#     ):
#         # create a JSON payload with the results of the query
#         single_user_data = {
#             "userid": int(row["userid"]),
#             "bank": str(row["bank"]),
#             "def_bank": str(row["default_bank"]),
#             "fairshare": float(row["fairshare"]),
#             "max_running_jobs": int(row["max_running_jobs"]),
#             "max_active_jobs": int(row["max_active_jobs"]),
#             "queues": str(row["queues"]),
#             "active": int(row["active"]),
#             "projects": str(row["projects"]),
#             "def_project": str(row["default_project"]),
#             "max_nodes": int(row["max_nodes"]),
#             "max_cores": int(row["max_cores"]),
#         }
#         bulk_user_data.append(single_user_data)

#     db_data += f"associations={json.dumps(bulk_user_data)} "

#     return db_data


@with_cursor
def init_plugin(conn, cursor):
    """
    Return a JSON object of certain tables in the flux-accounting database.

    Args:
        conn: SQLite connection object
        cursor: SQLite cursor object

    Returns:
        A JSON string containing associations and queues data
    """
    conn.row_factory = sqlite3.Row
    bulk_user_data = []
    bulk_queue_data = []
    bulk_proj_data = []
    bulk_bank_data = []
    bulk_factor_data = []
    config = {}

    # fetch all rows from association_table
    for row in cursor.execute(
        """SELECT userid, bank, default_bank,
        fairshare, max_running_jobs, max_active_jobs,
        queues, active, projects, default_project, max_nodes, max_cores
        FROM association_table"""
    ):
        # create a JSON payload with the results of the query
        single_user_data = {
            "userid": int(row["userid"]),
            "bank": str(row["bank"]),
            "def_bank": str(row["default_bank"]),
            "fairshare": float(row["fairshare"]),
            "max_running_jobs": int(row["max_running_jobs"]),
            "max_active_jobs": int(row["max_active_jobs"]),
            "queues": str(row["queues"]),
            "active": int(row["active"]),
            "projects": str(row["projects"]),
            "def_project": str(row["default_project"]),
            "max_nodes": int(row["max_nodes"]),
            "max_cores": int(row["max_cores"]),
        }
        bulk_user_data.append(single_user_data)

    config["associations"] = bulk_user_data

    # fetch all rows from queue_table
    for row in cursor.execute("SELECT * FROM queue_table"):
        # create a JSON payload with the results of the query
        single_q_data = {
            "queue": str(row["queue"]),
            "min_nodes_per_job": int(row["min_nodes_per_job"]),
            "max_nodes_per_job": int(row["max_nodes_per_job"]),
            "max_time_per_job": int(row["max_time_per_job"]),
            "priority": int(row["priority"]),
            "max_running_jobs": int(row["max_running_jobs"]),
            "max_nodes_per_assoc": int(row["max_nodes_per_assoc"]),
        }
        bulk_queue_data.append(single_q_data)

    config["queues"] = bulk_queue_data

    # fetch all rows from project_table
    for row in cursor.execute("SELECT project FROM project_table"):
        # create a JSON payload with the results of the query
        single_project = {
            "project": str(row["project"]),
        }
        bulk_proj_data.append(single_project)

    config["projects"] = bulk_proj_data

    # fetch rows from bank_table
    for row in cursor.execute("SELECT bank, priority FROM bank_table"):
        single_bank = {
            "bank": str(row["bank"]),
            "priority": float(row["priority"]),
        }
        bulk_bank_data.append(single_bank)

    config["banks"] = bulk_bank_data

    # fetch rows from priority_factor_weight_table
    for row in cursor.execute("SELECT * FROM priority_factor_weight_table"):
        single_priority_factor = {
            "factor": str(row["factor"]),
            "weight": int(row["weight"]),
        }
        bulk_factor_data.append(single_priority_factor)

    config["priority_factors"] = bulk_factor_data

    # Return a single JSON object containing both associations and queues
    return json.dumps(config)
