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
import sqlite3

import fluxacct.accounting
from fluxacct.accounting import user_subcommands as u
from fluxacct.util import formatter as fmt
from fluxacct.util import sql_util as sql

###############################################################
#                                                             #
#                      Helper Functions                       #
#                                                             #
###############################################################
def validate_parent_bank(cur, parent_bank):
    try:
        cur.execute("SELECT shares FROM bank_table WHERE bank=?", (parent_bank,))
        result = cur.fetchone()
        if result is None:
            raise ValueError(parent_bank)

        return 0
    except sqlite3.OperationalError as exc:
        raise sqlite3.OperationalError(f"an sqlite3.OperationalError occurred: {exc}")


def bank_is_active(cur, bank, parent_bank):
    """Check if the bank already exists and is active."""
    cur.execute(
        "SELECT active FROM bank_table WHERE bank=? AND parent_bank=?",
        (
            bank,
            parent_bank,
        ),
    )
    is_active = cur.fetchall()
    if len(is_active) > 0 and is_active[0][0] == 1:
        return True

    return False


def check_if_bank_disabled(cur, bank, parent_bank):
    """
    Check if the bank already exists but was disabled first. If so, just
    update the 'active' column in the already existing row.
    """
    cur.execute(
        "SELECT * FROM bank_table WHERE bank=? AND parent_bank=?",
        (bank, parent_bank),
    )
    result = cur.fetchall()
    if len(result) == 1:
        return True

    return False


def reactivate_bank(conn, cur, bank, parent_bank):
    """Re-enable the bank by setting 'active' to 1."""
    cur.execute(
        "UPDATE bank_table SET active=1 WHERE bank=? AND parent_bank=?",
        (
            bank,
            parent_bank,
        ),
    )
    conn.commit()


###############################################################
#                                                             #
#                   Subcommand Functions                      #
#                                                             #
###############################################################


def add_bank(conn, bank, shares, parent_bank=""):
    cur = conn.cursor()

    if parent_bank == "":
        # a root bank is trying to be added; check that one does not already exist
        cur.execute("SELECT * FROM bank_table WHERE parent_bank=''")
        if len(cur.fetchall()) > 0:
            raise ValueError(f"bank_table already has a root bank")

    # if the parent bank is not "", that means the bank trying
    # to be added wants to be placed under an existing parent bank
    try:
        if parent_bank != "":
            validate_parent_bank(cur, parent_bank)
    except ValueError as bad_parent_bank:
        raise ValueError(f"parent bank {bad_parent_bank} not found in bank table")
    except sqlite3.OperationalError as exc:
        raise sqlite3.OperationalError(exc)

    # check if bank already exists and is active in bank_table; if so, raise
    # a sqlite3.IntegrityError
    if bank_is_active(cur, bank, parent_bank):
        raise sqlite3.IntegrityError(f"bank {bank} already exists in bank_table")

    # if true, bank already exists in table but is not
    # active, so re-activate the bank and return
    if check_if_bank_disabled(cur, bank, parent_bank):
        reactivate_bank(conn, cur, bank, parent_bank)
        return 0

    # insert the bank values into the database
    try:
        conn.execute(
            """
            INSERT INTO bank_table (
                bank,
                parent_bank,
                shares
            )
            VALUES (?, ?, ?)
            """,
            (bank, parent_bank, shares),
        )
        # commit changes
        conn.commit()

        return 0
    # make sure entry is unique
    except sqlite3.IntegrityError:
        raise sqlite3.IntegrityError(f"bank {bank} already exists in bank_table")


def view_bank(
    conn, bank, tree=False, users=False, parsable=False, cols=None, format_string=""
):
    if tree and cols is not None:
        # tree format cannot be combined with custom formatting, so raise an Exception
        raise ValueError(f"--tree option does not support custom formatting")
    if parsable and not tree:
        # --parsable can only be called with --tree, so raise an Exception
        raise ValueError(f"-P/--parsable can only be passed with -t/--tree")

    # use all column names if none are passed in
    cols = cols or fluxacct.accounting.BANK_TABLE

    try:
        cur = conn.cursor()

        sql.validate_columns(cols, fluxacct.accounting.BANK_TABLE)
        # construct SELECT statement
        select_stmt = f"SELECT {', '.join(cols)} FROM bank_table WHERE bank=?"
        cur.execute(select_stmt, (bank,))

        # initialize BankFormatter object
        formatter = fmt.BankFormatter(cur, bank)

        if format_string != "":
            return formatter.as_format_string(format_string)
        if tree:
            if parsable:
                return formatter.as_parsable_tree(bank)
            return formatter.as_tree()
        if users:
            return formatter.with_users(bank)
        return formatter.as_json()
    except sqlite3.Error as err:
        raise sqlite3.Error(err)
    except ValueError as exc:
        raise ValueError(exc)


def delete_bank(conn, bank, force=False):
    """
    Deactivate a bank row in the bank_table by setting its 'active' status to 0.
    If force=True, actually remove the bank row from the bank_table. If the bank contains
    multiple sub-banks and associations, either disable or actually remove those rows as
    well.

    Args:
        conn: The SQLite Connection object
        bank: the name of the bank
        force: an option to actually remove the row from the bank_table instead of
            just setting the 'active' column to 0.
    """
    cursor = conn.cursor()
    if force:
        sql_stmt = "DELETE FROM bank_table WHERE bank=?"
    else:
        sql_stmt = "UPDATE bank_table SET active=0 WHERE bank=?"

    try:
        cursor.execute(sql_stmt, (bank,))

        # helper function to traverse the bank table and disable all of its sub banks
        def get_sub_banks(bank):
            select_stmt = "SELECT bank FROM bank_table WHERE parent_bank=?"
            cursor.execute(select_stmt, (bank,))
            result = cursor.fetchall()

            # we've reached a bank with no sub banks
            if len(result) == 0:
                select_assoc_stmt = """
                    SELECT username, bank
                    FROM association_table WHERE bank=?
                    """
                for assoc_row in cursor.execute(select_assoc_stmt, (bank,)):
                    u.delete_user(
                        conn,
                        username=assoc_row["username"],
                        bank=assoc_row["bank"],
                        force=force,
                    )
            # else, disable all of its sub banks and continue traversing
            else:
                for row in result:
                    cursor.execute(sql_stmt, (row["bank"],))
                    get_sub_banks(row["bank"])

        get_sub_banks(bank)
    # if an exception occurs while recursively deleting
    # the parent banks, then throw the exception and roll
    # back the changes made to the DB
    except sqlite3.OperationalError as exc:
        conn.rollback()
        raise sqlite3.OperationalError(f"an sqlite3.OperationalError occurred: {exc}")

    # commit changes
    conn.commit()
    return 0


def edit_bank(
    conn,
    bank=None,
    shares=None,
    parent_bank=None,
):
    cur = conn.cursor()
    params = locals()
    editable_fields = [
        "shares",
        "parent_bank",
    ]
    for field in editable_fields:
        if params[field] is not None:
            if field == "parent_bank":
                try:
                    validate_parent_bank(cur, params[field])
                except ValueError as bad_parent_bank:
                    raise ValueError(
                        f"parent bank {bad_parent_bank} not found in bank table"
                    )
                except sqlite3.OperationalError as exc:
                    raise sqlite3.OperationalError(exc)
            if field == "shares":
                if int(shares) <= 0:
                    raise ValueError("new shares amount must be >= 0")

            update_stmt = "UPDATE bank_table SET " + field

            update_stmt += "=? WHERE bank=?"
            tup = (
                params[field],
                bank,
            )
            conn.execute(update_stmt, tup)

    # commit changes
    conn.commit()

    return 0


def list_banks(
    conn,
    inactive=False,
    cols=None,
    table=False,
    format_string="",
):
    """
    List all banks in bank_table.

    Args:
        inactive: whether to include inactive banks. By default, only banks that are
            active will be included in the output.
        cols: a list of columns from the table to include in the output. By default, all
            columns are included.
        table: output data in bank_table in table format. By default, the format of any
            returned data is in JSON.
        format_string: a format string defining how each row should be formatted. Column
            names should be used as placeholders.
    """
    # use all column names if none are passed in
    cols = cols or fluxacct.accounting.BANK_TABLE

    try:
        cur = conn.cursor()

        sql.validate_columns(cols, fluxacct.accounting.BANK_TABLE)
        # construct SELECT statement
        select_stmt = f"SELECT {', '.join(cols)} FROM bank_table"
        if not inactive:
            select_stmt += " WHERE active=1"
        cur.execute(select_stmt)

        # initialize AccountingFormatter object
        formatter = fmt.AccountingFormatter(cur)
        if format_string != "":
            return formatter.as_format_string(format_string)
        if table:
            return formatter.as_table()
        return formatter.as_json()
    except sqlite3.Error as err:
        raise sqlite3.Error(err)
    except ValueError as exc:
        raise ValueError(exc)
