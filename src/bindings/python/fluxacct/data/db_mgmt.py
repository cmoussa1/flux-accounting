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
import sqlite3
import logging
import sys
import pathlib

import fluxacct.accounting

LOGGER = logging.getLogger(__name__)


def create_db(filepath):
    db_dir = pathlib.PosixPath(filepath).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    try:
        # open connection to database
        LOGGER.info("Creating Flux data DB")
        conn = sqlite3.connect("file:" + filepath + "?mode:rwc", uri=True)
        LOGGER.info("Created Flux data DB successfully")
    except sqlite3.OperationalError as exception:
        LOGGER.error(exception)
        sys.exit(1)

    # set version number of database
    conn.execute("PRAGMA user_version = %d" % (fluxacct.data.DB_SCHEMA_VERSION))

    LOGGER.info("created database")
    conn.commit()
    conn.close()


def hello():
    return "hello from fluxacct.data module"
