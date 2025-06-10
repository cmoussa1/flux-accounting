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
import unittest
import os
import sqlite3

from unittest import mock

from fluxacct.accounting import create_db as c
from fluxacct.accounting import user_subcommands as u
from fluxacct.accounting import bank_subcommands as b
from fluxacct.accounting import queue_subcommands as q


class TestAccountingCLI(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # create test accounting database
        c.create_db("FluxAccountingTestPredictPriority.db")
        global conn
        global cur

        conn = sqlite3.connect("FluxAccountingTestPredictPriority.db")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # add banks
        b.add_bank(conn, "root", 1)
        b.add_bank(conn, bank="A", shares=1, parent_bank="root")
        b.add_bank(conn, bank="B", shares=1, parent_bank="root")

        # add associations
        u.add_user(conn, username="user1", bank="A", uid=50001)
        u.add_user(conn, username="user1", bank="B", uid=50001)

        # add queues
        q.add_queue(conn, queue="bronze", priority=0)
        q.add_queue(conn, queue="gold", priority=100)

    # a ValueError is raised if the username cannot be found
    def test_01_predict_priority_no_user(self):
        with self.assertRaises(ValueError):
            u.calc_job_priority(conn, username="foo")

    # a ValueError is also raised if the association cannot be found
    def test_02_predict_priority_no_user(self):
        with self.assertRaises(ValueError):
            u.calc_job_priority(conn, username="foo", bank="bar")

    def test_03_predict_priority_valid(self):
        result = u.calc_job_priority(conn, username="user1")
        print(result)

    def test_04_predict_priority_valid_optional_args_1(self):
        result = u.calc_job_priority(conn, username="user1", bank="B", queue="gold")
        print(result)

    # remove database and log file
    @classmethod
    def tearDownClass(self):
        conn.close()
        os.remove("FluxAccountingTestPredictPriority.db")


def suite():
    suite = unittest.TestSuite()

    return suite


if __name__ == "__main__":
    from pycotap import TAPTestRunner

    unittest.main(testRunner=TAPTestRunner())
