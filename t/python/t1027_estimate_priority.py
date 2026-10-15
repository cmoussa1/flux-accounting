#!/usr/bin/env python3

###############################################################
# Copyright 2026 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
import json
import os
import sqlite3
import time
import unittest

from fluxacct.database import create as c
from fluxacct.entities import associations as u
from fluxacct.entities import banks as b
from fluxacct.entities import queues as q
from fluxacct.policy import priorities as prio


class TestEstimatePriority(unittest.TestCase):
    def setUp(self):
        self.dbname = (
            f"TestDB_estprio_{self._testMethodName}_{round(time.time() * 1000)}.db"
        )
        c.create_db(self.dbname)
        self.conn = sqlite3.connect(self.dbname, timeout=60)
        self.conn.row_factory = sqlite3.Row
        b.add_bank(self.conn, "root", 1)
        b.add_bank(self.conn, "A", 1, "root", priority=2.0)
        b.add_bank(self.conn, "B", 1, "root", priority=4.0)
        q.add_queue(self.conn, "debug", priority=7)
        q.add_queue(self.conn, "batch", priority=3)
        u.add_user(
            self.conn,
            username="user1",
            uid=50001,
            bank="A",
            fairshare=0.5,
            queues="debug",
        )
        u.add_user(
            self.conn,
            username="user1",
            uid=50001,
            bank="B",
            fairshare=0.25,
            queues="batch",
        )

    def tearDown(self):
        self.conn.close()
        os.remove(self.dbname)

    def rows(self, **kwargs):
        output = prio.estimate_priority(self.conn, json_fmt=True, **kwargs)
        return json.loads(output)

    def test_01_default_bank_and_explicit_bank(self):
        row = self.rows(username="user1", queue="debug")[0]
        self.assertEqual(row["bank"], "A")
        self.assertEqual(row["queue_priority"], 7)
        self.assertEqual(row["priority"], 120000)

        row = self.rows(username="user1", bank="B", queue="batch")[0]
        self.assertEqual(row["bank"], "B")
        self.assertEqual(row["priority"], 55000)

    def test_02_all_banks_are_ordered_and_keep_rejections(self):
        rows = self.rows(username="user1", all_banks=True, queue="debug")
        self.assertEqual([row["bank"] for row in rows], ["A", "B"])
        self.assertTrue(rows[0]["accepted"])
        self.assertFalse(rows[1]["accepted"])
        self.assertIn("not valid for user", rows[1]["rejection_reason"])

    def test_03_single_bank_rejection_raises(self):
        with self.assertRaisesRegex(ValueError, "not valid for user"):
            prio.estimate_priority(self.conn, username="user1", bank="B", queue="debug")
        self.conn.execute(
            "UPDATE association_table SET active=0 WHERE username='user1' AND bank='A'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "disabled"):
            prio.estimate_priority(self.conn, username="user1", bank="A", queue="debug")

    def test_04_unknown_queue_policy(self):
        row = self.rows(username="user1", bank="A", queue="unknown")[0]
        self.assertTrue(row["accepted"])
        self.assertEqual(row["queue_priority"], 0)
        self.conn.execute(
            "UPDATE config_table SET value='true' WHERE key='deny_unknown_queues'"
        )
        self.conn.commit()
        with self.assertRaisesRegex(ValueError, "deny-unknown-queues"):
            prio.estimate_priority(
                self.conn, username="user1", bank="A", queue="unknown"
            )

    def test_05_weight_overlay_and_output_modes(self):
        self.conn.execute(
            "DELETE FROM priority_factor_weight_table WHERE factor='queue'"
        )
        self.conn.execute(
            "UPDATE priority_factor_weight_table SET weight=10 "
            "WHERE factor='fairshare'"
        )
        self.conn.commit()
        row = self.rows(username="user1", bank="A", queue="debug")[0]
        self.assertEqual(row["w_fairshare"], 10)
        self.assertEqual(row["w_queue"], 10000)
        self.assertEqual(row["priority"], 70005)

        table = prio.estimate_priority(self.conn, username="user1", queue="debug")
        self.assertIn("Notes:", table)
        self.assertIn("limits are not evaluated", table)
        custom = prio.estimate_priority(
            self.conn,
            username="user1",
            queue="debug",
            json_fmt=True,
            format_string="{username}:{bank}:{priority}",
        )
        self.assertIn("username:bank:priority", custom)
        self.assertNotIn("Notes:", custom)

    def test_06_omitted_queue_uses_zero(self):
        row = self.rows(username="user1", bank="A")[0]
        self.assertEqual(row["queue"], "")
        self.assertEqual(row["queue_priority"], 0)
        self.assertEqual(row["priority"], 50000)


def suite():
    suite = unittest.TestSuite()

    return suite


if __name__ == "__main__":
    from pycotap import TAPTestRunner

    unittest.main(testRunner=TAPTestRunner())
