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
import json


def execute_sql_statement(cursor, table_name, fields, inactive):
    """
    Execute a SQL statement to be executed on a table in the database.

    Args:
        cursor: SQLite Cursor object to execute the query.
        table_name: name of the table to query data from.
        fields: list of fields to include in the SELECT statement.

    Returns:
        rows: a list of rows representing what was queried from the table.
    """
    # construct SELECT statement
    select_fields = ", ".join(fields)
    query = f"SELECT {select_fields} FROM {table_name}"
    if not inactive:
        query += " WHERE active=1"

    cursor.execute(query)
    rows = cursor.fetchall()

    return rows


def list_data_table(cursor, rows):
    """
    List data from the specified table with the provided fields.

    Args:
        cursor: SQLite Cursor object used to execute the query.
        rows: the data retrieved from the query.

    Returns:
        table: the data from the query formatted as a string in table format.
    """
    # fetch column names and determine the width of each column
    col_names = [description[0] for description in cursor.description]
    col_widths = [
        max(len(str(value)) for value in [col] + [row[i] for row in rows])
        for i, col in enumerate(col_names)
    ]

    # format a row of data
    def format_row(row):
        return " | ".join(
            [f"{str(value).ljust(col_widths[i])}" for i, value in enumerate(row)]
        )

    # format the header, separator, and data rows
    header = format_row(col_names)
    separator = "-+-".join(["-" * width for width in col_widths])
    data_rows = "\n".join([format_row(row) for row in rows])

    table = f"{header}\n{separator}\n{data_rows}"

    return table


def list_data_json(cursor, rows):
    """
    Retrieve data from the specified table and format it as a JSON object.

    Args:
        cursor: SQLite Cursor object used to execute the query.
        rows: the data retrieved from the query.

    Returns:
        json_string: the data from the query formatted as a JSON string.
    """
    # fetch column names
    col_names = [description[0] for description in cursor.description]

    # create a list of dictionaries, one for each row
    table_data = [{col_names[i]: row[i] for i in range(len(col_names))} for row in rows]

    # convert the list of dictionaries to a JSON string
    json_string = json.dumps(table_data, indent=2)

    return json_string
