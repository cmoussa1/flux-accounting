#!/usr/bin/env python3

###############################################################
# Copyright 2022 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import toml

from fluxacct.accounting import queue_subcommands as q


###############################################################
#                                                             #
#                      Helper Functions                       #
#                                                             #
###############################################################
def initialize_queues(conn, queue_data):
    """
    Initialize queue_table with queue and their associated priority information.

    Args:
        conn: SQLite Connection object used to interact with the database.
        queue_data: a dictionary containing queues and their associated priority.
    """
    cur = conn.cursor()
    if queue_data is not None:
        for queue in queue_data:
            cur.execute("SELECT * FROM queue_table WHERE queue=?", (queue,))
            queue_exists = cur.fetchall()
            if queue_exists:
                # queue already exists; edit the queue_table
                q.edit_queue(conn, queue=queue, priority=queue_data[queue])
            else:
                q.add_queue(conn, queue=queue, priority=queue_data[queue])


###############################################################
#                                                             #
#                   Subcommand Functions                      #
#                                                             #
###############################################################
def initialize_from_toml(conn, flux_config, toml_file=None):
    if toml_file:
        # load TOML file into dictionary
        flux_config = toml.load(toml_file)

    if flux_config.get("accounting") is not None:
        # config file has some flux-accounting information in it
        queue_data = flux_config.get("accounting").get("queue-priorities")
        initialize_queues(conn, queue_data)
