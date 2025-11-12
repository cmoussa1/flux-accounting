###############################################################
# Copyright 2022 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import signal
import sys
import sqlite3
import os
import argparse
import logging

import flux
import flux.constants
import fluxacct.data

from flux.constants import FLUX_MSGTYPE_REQUEST
from fluxacct.data import db_mgmt


def establish_sqlite_connection(path):
    # try to open database file; will exit with -1 if database file not found
    if not os.path.isfile(path):
        print(f"Database file does not exist: {path}", file=sys.stderr)
        sys.exit(1)

    db_uri = "file:" + path + "?mode=rw"
    try:
        conn = sqlite3.connect(db_uri, uri=True)
        # set foreign keys constraint
        conn.execute("PRAGMA foreign_keys = 1")
        conn.row_factory = sqlite3.Row
    except sqlite3.OperationalError as exc:
        print(f"Unable to open database file: {db_uri}", file=sys.stderr)
        print(f"Exception: {exc}")
        sys.exit(1)

    return conn


def background():
    pid = os.fork()
    if pid > 0:
        # exit first parent
        sys.exit(0)


# pylint: disable=broad-except
class DataService:
    def __init__(self, flux_handle, conn):

        self.handle = flux_handle
        self.conn = conn

        try:
            # register service with broker
            self.handle.service_register("data").get()
            print("registered data service", file=sys.stderr)
        except FileExistsError:
            LOGGER.error("flux-data service is already registered")

        # register signal watcher for SIGTERM to initiate shutdown
        self.handle.signal_watcher_create(signal.SIGTERM, self.shutdown).start()
        self.handle.signal_watcher_create(signal.SIGINT, self.shutdown).start()

        general_endpoints = [
            "hello",
        ]

        privileged_endpoints = [
            "shutdown_service",
        ]

        for name in general_endpoints:
            watcher = self.handle.msg_watcher_create(
                getattr(self, name), FLUX_MSGTYPE_REQUEST, f"data.{name}", self
            )
            self.handle.msg_handler_allow_rolemask(
                watcher.handle, flux.constants.FLUX_ROLE_USER
            )
            watcher.start()

        for name in privileged_endpoints:
            self.handle.msg_watcher_create(
                getattr(self, name), FLUX_MSGTYPE_REQUEST, f"data.{name}", self
            ).start()

    def shutdown(self, handle, watcher, signum, arg):
        print("Shutting down...", file=sys.stderr)
        self.conn.close()
        self.handle.service_unregister("data").get()
        self.handle.reactor_stop()

    # watches for a shutdown message
    def shutdown_service(self, handle, watcher, msg, arg):
        print("Shutting down...", file=sys.stderr)
        self.conn.close()
        self.handle.service_unregister("data").get()
        self.handle.reactor_stop()
        handle.respond(msg)

    # pylint: disable=no-self-use
    def hello(self, handle, watcher, msg, arg):
        try:
            val = db_mgmt.hello()
            payload = {"hello": val}
            handle.respond(msg, payload)
        except KeyError as exc:
            handle.respond_error(msg, 0, f"hello: missing key in payload: {exc}")
        except Exception as exc:
            handle.respond_error(msg, 0, f"hello: {type(exc).__name__}: {exc}")


LOGGER = logging.getLogger("flux-uri")


@flux.util.CLIMain(LOGGER)
def main():
    parser = argparse.ArgumentParser(prog="flux-uri")
    parser.add_argument(
        "-p", "--path", dest="path", help="specify location of database file"
    )
    parser.add_argument(
        "-t",
        "--test-background",
        action="store_true",
        dest="background",
        help="used for testing",
    )
    args = parser.parse_args()

    # try to connect to flux jobs database; if connection fails, exit
    # flux-data service
    db_path = args.path if args.path else fluxacct.data.DB_PATH
    conn = establish_sqlite_connection(db_path)

    handle = flux.Flux()
    server = DataService(handle, conn)

    if args.background:
        background()

    handle.reactor_run()


if __name__ == "__main__":
    main()
