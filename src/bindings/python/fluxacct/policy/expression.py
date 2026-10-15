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
"""Priority expression compilation and evaluation."""

import ast
import math
import numbers

VARIABLES = frozenset(
    {
        "fairshare",
        "queue_priority",
        "bank_priority",
        "urgency",
        "w_fairshare",
        "w_queue",
        "w_bank",
        "w_urgency",
    }
)
DEFAULT_EXPRESSION = (
    "fairshare * w_fairshare + queue_priority * w_queue"
    " + bank_priority * w_bank + (urgency - 16) * w_urgency"
)
TREE_VERSION = 1
MAX_NODES = 256
MAX_DEPTH = 32
PRIORITY_MIN = 0
PRIORITY_MAX = 4294967295
URGENCY_HOLD = 0
URGENCY_DEFAULT = 16
URGENCY_EXPEDITE = 31


class EvaluationError(Exception):
    """Raised when a validated expression cannot be evaluated."""


class _Compiler:
    def __init__(self):
        self.nodes = 0

    def compile(self, node, depth=1):
        if depth > MAX_DEPTH:
            raise ValueError(f"expression exceeds maximum depth {MAX_DEPTH}")
        self.nodes += 1
        if self.nodes > MAX_NODES:
            raise ValueError(f"expression exceeds maximum node count {MAX_NODES}")

        if isinstance(node, ast.Constant):
            return self._compile_constant(node)
        if isinstance(node, ast.Name):
            return self._compile_name(node)
        if isinstance(node, ast.BinOp):
            return self._compile_binop(node, depth)
        if isinstance(node, ast.UnaryOp):
            return self._compile_unaryop(node, depth)
        if isinstance(node, ast.Call):
            return self._compile_call(node, depth)

        return self._unsupported(node)

    def _compile_constant(self, node):
        value = node.value
        if isinstance(value, bool):
            return self._unsupported(node, "bool")
        if isinstance(value, int) and abs(value) > 2**53:
            raise ValueError(f"integer at column {node.col_offset} is too large")
        if not isinstance(value, (int, float)):
            return self._unsupported(node, type(value).__name__)
        value = float(value)
        if not math.isfinite(value):
            raise ValueError(f"number at column {node.col_offset} is not finite")
        return {"num": value}

    @staticmethod
    def _compile_name(node):
        if node.id not in VARIABLES:
            raise ValueError(
                f"unsupported name {node.id!r} at column {node.col_offset}"
            )
        return {"var": node.id}

    def _compile_binop(self, node, depth):
        ops = {
            ast.Add: "+",
            ast.Sub: "-",
            ast.Mult: "*",
            ast.Div: "/",
        }
        operator = ops.get(type(node.op))
        if operator is None:
            return self._unsupported(node, type(node.op).__name__)
        return {
            "op": operator,
            "args": [
                self.compile(node.left, depth + 1),
                self.compile(node.right, depth + 1),
            ],
        }

    def _compile_unaryop(self, node, depth):
        if isinstance(node.op, ast.UAdd):
            self.nodes -= 1
            return self.compile(node.operand, depth)
        if isinstance(node.op, ast.USub):
            return {"op": "neg", "args": [self.compile(node.operand, depth + 1)]}
        return self._unsupported(node, type(node.op).__name__)

    def _compile_call(self, node, depth):
        if not isinstance(node.func, ast.Name) or node.func.id not in {"min", "max"}:
            return self._unsupported(node)
        if node.keywords:
            column = getattr(node.keywords[0], "col_offset", node.col_offset)
            raise ValueError(f"unsupported keyword argument at column {column}")
        if len(node.args) < 2:
            raise ValueError(
                f"{node.func.id} call at column {node.col_offset} "
                "requires at least two arguments"
            )
        return {
            "op": node.func.id,
            "args": [self.compile(arg, depth + 1) for arg in node.args],
        }

    @staticmethod
    def _unsupported(node, construct=None):
        if construct is None:
            construct = type(node).__name__
        column = getattr(node, "col_offset", None)
        raise ValueError(f"unsupported {construct} at column {column}")


def compile_expression(source):
    """Parse and validate source and return a versioned expression tree."""
    try:
        parsed = ast.parse(source, mode="eval")
    except SyntaxError as exc:
        column = None if exc.offset is None else exc.offset - 1
        raise ValueError(f"syntax error at column {column}") from exc

    compiler = _Compiler()
    return {"version": TREE_VERSION, "tree": compiler.compile(parsed.body)}


def evaluate(envelope, variables):
    """Evaluate a compiled priority expression envelope."""
    if not isinstance(envelope, dict) or set(envelope) != {"version", "tree"}:
        raise EvaluationError("expression envelope is malformed")
    if envelope["version"] != TREE_VERSION:
        raise EvaluationError(f"unsupported expression version {envelope['version']}")

    state = {"nodes": 0}
    value = _evaluate_node(envelope["tree"], variables, 1, state)
    if not math.isfinite(value):
        raise EvaluationError("expression result is not finite")
    return value


def default_formula(variables):
    """Return the unrounded built-in priority formula value."""
    return (
        variables["w_fairshare"] * variables["fairshare"]
        + variables["w_queue"] * variables["queue_priority"]
        + variables["w_bank"] * variables["bank_priority"]
        + variables["w_urgency"] * (variables["urgency"] - URGENCY_DEFAULT)
    )


def finalize(value):
    """Round half away from zero and clamp to Flux job priority bounds."""
    if not math.isfinite(value):
        raise EvaluationError("priority value is not finite")
    rounded = _round_half_away(value)
    if rounded < PRIORITY_MIN:
        return PRIORITY_MIN
    if rounded > PRIORITY_MAX:
        return PRIORITY_MAX
    return int(rounded)


def calculate_priority(urgency, variables, envelope=None):
    """Calculate a final priority and whether expression fallback was used."""
    if urgency == URGENCY_HOLD:
        return PRIORITY_MIN, False
    if urgency == URGENCY_EXPEDITE:
        return PRIORITY_MAX, False

    merged = dict(variables)
    merged["urgency"] = urgency
    if envelope is None:
        value = default_formula(merged)
        fallback_used = False
    else:
        try:
            value = evaluate(envelope, merged)
            fallback_used = False
        except EvaluationError:
            value = default_formula(merged)
            fallback_used = True
    return finalize(value), fallback_used


def _evaluate_node(node, variables, depth, state):
    if depth > MAX_DEPTH:
        raise EvaluationError(f"expression exceeds maximum depth {MAX_DEPTH}")
    state["nodes"] += 1
    if state["nodes"] > MAX_NODES:
        raise EvaluationError(f"expression exceeds maximum node count {MAX_NODES}")
    if not isinstance(node, dict):
        raise EvaluationError("expression node is malformed")

    if set(node) == {"num"}:
        return _finite_number(node["num"])
    if set(node) == {"var"}:
        return _variable_value(node["var"], variables)
    if set(node) != {"op", "args"}:
        raise EvaluationError("expression node is malformed")

    operator = node["op"]
    args = node["args"]
    if not isinstance(args, list):
        raise EvaluationError("expression args are malformed")
    if operator in {"+", "-", "*", "/"}:
        if len(args) != 2:
            raise EvaluationError(f"{operator} requires two arguments")
        left = _evaluate_node(args[0], variables, depth + 1, state)
        right = _evaluate_node(args[1], variables, depth + 1, state)
        if operator == "+":
            value = left + right
        elif operator == "-":
            value = left - right
        elif operator == "*":
            value = left * right
        else:
            if right == 0.0:
                raise EvaluationError("division by zero")
            value = left / right
        return _finite_number(value)
    if operator == "neg":
        if len(args) != 1:
            raise EvaluationError("neg requires one argument")
        return _finite_number(-_evaluate_node(args[0], variables, depth + 1, state))
    if operator in {"min", "max"}:
        if len(args) < 2:
            raise EvaluationError(f"{operator} requires at least two arguments")
        values = [_evaluate_node(arg, variables, depth + 1, state) for arg in args]
        return _finite_number(min(values) if operator == "min" else max(values))
    raise EvaluationError(f"unsupported operator {operator!r}")


def _variable_value(name, variables):
    if name not in VARIABLES:
        raise EvaluationError(f"unsupported variable {name!r}")
    if name not in variables:
        raise EvaluationError(f"missing variable {name!r}")
    return _finite_number(variables[name])


def _finite_number(value):
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise EvaluationError("expression value is not numeric")
    if isinstance(value, int) and abs(value) > 2**53:
        raise EvaluationError("expression integer value is too large")
    value = float(value)
    if not math.isfinite(value):
        raise EvaluationError("expression value is not finite")
    return value


def _round_half_away(value):
    truncated = math.trunc(value)
    if abs(value - truncated) >= 0.5:
        truncated += math.copysign(1.0, value)
    return truncated
