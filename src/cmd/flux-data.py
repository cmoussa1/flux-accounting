###############################################################
# Copyright 2025 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import argparse
import sys
import logging

import flux
import fluxacct.data

from fluxacct.data import db_mgmt


def add_path_arg(parser):
    parser.add_argument(
        "-p", "--path", dest="path", help="specify location of database file"
    )


def add_create_db_arg(subparsers):
    subparser_create_db = subparsers.add_parser(
        "create-db",
        help="create the flux-data database",
        formatter_class=flux.util.help_formatter(),
    )
    subparser_create_db.set_defaults(func="create_db")


def hello(subparsers):
    subparsers_hello = subparsers.add_parser(
        "hello",
        help="test function",
        formatter_class=flux.util.help_formatter(),
    )
    subparsers_hello.set_defaults(func="hello")


def add_arguments_to_parser(parser, subparsers):
    add_path_arg(parser)
    add_create_db_arg(subparsers)
    hello(subparsers)


def set_db_location(args):
    path = args.path if args.path else fluxacct.data.DB_PATH
    return path


def select_function(args, parser):
    data = vars(args)

    # map each command to the corresponding data RPC call
    func_map = {
        "hello": "data.hello",
    }

    if args.func in func_map:
        return_val = flux.Flux().rpc(func_map[args.func], data).get()
    else:
        parser.print_usage()
        return

    if list(return_val.values())[0] != 0:
        print(list(return_val.values())[0])


LOGGER = logging.getLogger("flux-data")


@flux.util.CLIMain(LOGGER)
def main():
    parser = argparse.ArgumentParser(
        description="""
        Description: Translate command line arguments into SQLite instructions for the
        Flux jobs database.
        """
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="subcommand")
    subparsers.required = True

    add_arguments_to_parser(parser, subparsers)
    args = parser.parse_args()

    path = set_db_location(args)

    # if we are creating the DB for the first time, we need
    # to ONLY create the DB and then exit out successfully
    if args.func == "create_db":
        db_mgmt.create_db(path)
        sys.exit(0)

    select_function(args, parser)


if __name__ == "__main__":
    main()
