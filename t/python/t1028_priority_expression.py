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
import math
import os
import unittest

from fluxacct.policy import expression

VARS = {
    "fairshare": 0.5,
    "queue_priority": 2,
    "bank_priority": 3.0,
    "urgency": 16,
    "w_fairshare": 100000,
    "w_queue": 10000,
    "w_bank": 0,
    "w_urgency": 1000,
}


class TestPriorityExpression(unittest.TestCase):
    def test_01_compile_whitelisted_grammar(self):
        compiled = expression.compile_expression(
            "-fairshare + +queue_priority * max(2, min(3, bank_priority)) / 4"
        )
        self.assertEqual(compiled["version"], 1)
        self.assertEqual(compiled["tree"]["op"], "+")

    def test_02_reject_unsupported_constructs_with_column(self):
        cases = [
            ("fairshare ** 2", "Pow", "column 0"),
            ("fairshare // 2", "FloorDiv", "column 0"),
            ("fairshare if urgency else 0", "IfExp", "column 0"),
            ("True", "bool", "column 0"),
            ("missing", "missing", "column 0"),
            ("min(fairshare, b=1)", "keyword", "column"),
        ]
        for source, construct, column in cases:
            with self.subTest(source=source):
                with self.assertRaisesRegex(ValueError, construct):
                    try:
                        expression.compile_expression(source)
                    except ValueError as exc:
                        self.assertIn(column, str(exc))
                        raise

    def test_03_reject_size_limits(self):
        with self.assertRaisesRegex(ValueError, "too large"):
            expression.compile_expression(str(2**53 + 1))
        deep = "fairshare"
        for _ in range(expression.MAX_DEPTH):
            deep = f"({deep} + 1)"
        with self.assertRaisesRegex(ValueError, "depth"):
            expression.compile_expression(deep)
        wide = "min(" + ", ".join(["fairshare"] * (expression.MAX_NODES + 1)) + ")"
        with self.assertRaisesRegex(ValueError, "node count"):
            expression.compile_expression(wide)

    def test_04_validate_envelope_and_variables(self):
        with self.assertRaises(expression.EvaluationError):
            expression.evaluate({"version": 2, "tree": {"num": 1.0}}, VARS)
        with self.assertRaises(expression.EvaluationError):
            expression.evaluate({"version": 1, "tree": {"num": 1.0, "x": 2}}, VARS)
        with self.assertRaises(expression.EvaluationError):
            expression.evaluate({"version": 1, "tree": {"num": 2**53 + 1}}, VARS)
        with self.assertRaises(expression.EvaluationError):
            expression.evaluate({"version": 1, "tree": {"var": "missing"}}, VARS)

    def test_05_evaluate_errors_and_fallback(self):
        div_zero = {
            "version": 1,
            "tree": {"op": "/", "args": [{"num": 1.0}, {"num": 0.0}]},
        }
        with self.assertRaises(expression.EvaluationError):
            expression.evaluate(div_zero, VARS)
        priority, fallback = expression.calculate_priority(16, VARS, div_zero)
        self.assertEqual(
            priority,
            expression.finalize(expression.default_formula(VARS)),
        )
        self.assertTrue(fallback)
        nonfinite = {
            "version": 1,
            "tree": {"op": "*", "args": [{"num": math.inf}, {"num": 1.0}]},
        }
        with self.assertRaises(expression.EvaluationError):
            expression.evaluate(nonfinite, VARS)

    def test_06_rounding_and_clamping(self):
        self.assertEqual(expression.finalize(2.5), 3)
        self.assertEqual(expression.finalize(3.5), 4)
        self.assertEqual(expression.finalize(-2.5), expression.PRIORITY_MIN)
        self.assertEqual(expression.finalize(0.49999999999999994), 0)
        self.assertEqual(
            expression.finalize(expression.PRIORITY_MAX + 1.0),
            expression.PRIORITY_MAX,
        )

    def test_07_default_expression_matches_handwritten_default(self):
        compiled = expression.compile_expression(expression.DEFAULT_EXPRESSION)
        compiled_priority, fallback = expression.calculate_priority(16, VARS, compiled)
        default_priority, default_fallback = expression.calculate_priority(16, VARS)
        self.assertEqual(compiled_priority, default_priority)
        self.assertFalse(fallback)
        self.assertFalse(default_fallback)

    def test_08_shared_fixtures(self):
        fixture_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "expected",
            "priority",
            "fixtures.json",
        )
        with open(fixture_path, encoding="utf-8") as fixtures_file:
            fixtures = json.load(fixtures_file)
        for fixture in fixtures:
            if fixture["expression"] is not None:
                self.assertEqual(
                    expression.compile_expression(fixture["expression"]),
                    fixture["tree"],
                    fixture["name"],
                )
            priority, fallback = expression.calculate_priority(
                fixture["urgency"], fixture["vars"], fixture["tree"]
            )
            self.assertEqual(priority, fixture["expected"], fixture["name"])
            self.assertEqual(fallback, fixture["fallback"], fixture["name"])


def suite():
    suite = unittest.TestSuite()

    return suite


if __name__ == "__main__":
    from pycotap import TAPTestRunner

    unittest.main(testRunner=TAPTestRunner())
