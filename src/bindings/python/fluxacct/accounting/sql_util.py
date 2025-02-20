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
def validate_columns(columns, valid_columns):
    """
    Validate a list of of columns against a list of valid columns of a table
    in a flux-accounting database.

    Args:
        columns: a list of column names
        valid_columns: a list of valid column names

    Raises:
        ValueError: at least one of the columns passed in is not valid
    """
    invalid_columns = [column for column in columns if column not in valid_columns]
    if invalid_columns:
        raise ValueError(f"invalid fields: {', '.join(invalid_columns)}")


def toggle_wal_mode(conn):
    """
    Toggle Write-Ahead Logging (WAL) Mode on or off based on the current mode.

    If WAL mode is enabled, it switches back to DELETE mode.
    If DELETE mode is set, it enables WAL mode.

    Args:
        conn: a sqlite3 Connection object to the database.
    """
    cur = conn.cursor()
    current_mode = cur.execute("PRAGMA journal_mode").fetchone()[0].lower()

    if current_mode == "wal":
        # disable WAL
        new_mode = "DELETE"
    else:
        new_mode = "WAL"

    result = cur.execute(f"PRAGMA journal_mode = {new_mode}").fetchone()[0]

    return f"Journal mode changed from {current_mode.upper()} to {result.upper()}."
