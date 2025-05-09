#!/usr/bin/python3

import sqlite3
import random
import sys

def update_usage(db_path, reset=False):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Begin transaction
        conn.execute("BEGIN TRANSACTION;")

        # Select all usernames and banks
        cursor.execute("SELECT username, bank FROM association_table;")
        rows = cursor.fetchall()

        for username, bank in rows:
            job_usage_value = 0 if reset else random.randint(1, 1000)
            fairshare_value = 0.5 if reset else None

            # Build the update statement
            if reset:
                cursor.execute("""
                    UPDATE association_table
                    SET job_usage = ?, fairshare = ?
                    WHERE username = ? AND bank = ?;
                """, (job_usage_value, fairshare_value, username, bank))
            else:
                cursor.execute("""
                    UPDATE association_table
                    SET job_usage = ?
                    WHERE username = ? AND bank = ?;
                """, (job_usage_value, username, bank))

        conn.commit()
        action = "reset" if reset else "updated"
        print(f"{action.capitalize()} job_usage{' and fairshare' if reset else ''} for {len(rows)} rows.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        print("Usage: ./update_random_job_usage.py <sqlite_db_path> [--reset]")
        sys.exit(1)

    db_path = sys.argv[1]
    reset_flag = len(sys.argv) == 3 and sys.argv[2] == "--reset"
    update_usage(db_path, reset=reset_flag)
