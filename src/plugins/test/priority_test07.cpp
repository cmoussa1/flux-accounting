/************************************************************\
 * Copyright 2026 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
}

#include <cstdlib>
#include <map>
#include <string>

#include "src/plugins/accounting.hpp"
#include "src/common/libtap/tap.h"

// accounting.cpp references this global
bool deny_unknown_queues = false;

// default priority factor weights, mirroring the plugin's defaults
static std::map<std::string, int> weights = {
    {"fairshare", DEFAULT_FSHARE_WEIGHT},
    {"queue", DEFAULT_QUEUE_WEIGHT},
    {"bank", DEFAULT_BANK_WEIGHT},
    {"urgency", DEFAULT_URGENCY_WEIGHT},
};

/*
 * A held job whose urgency is HOLD gets the minimum priority, short-circuiting
 * the weighted-sum calculation.
 */
static void hold_urgency_returns_min ()
{
    int64_t prio = calc_priority (0.5, 0, 0.0, FLUX_JOB_URGENCY_HOLD, weights);
    ok (prio == FLUX_JOB_PRIORITY_MIN,
        "HOLD urgency returns FLUX_JOB_PRIORITY_MIN");
}

/*
 * A held job whose urgency is EXPEDITE gets the maximum priority; this is how
 * an admin expedites a held job past earlier-submitted ones.
 */
static void expedite_urgency_returns_max ()
{
    int64_t prio =
        calc_priority (0.5, 0, 0.0, FLUX_JOB_URGENCY_EXPEDITE, weights);
    ok (prio == FLUX_JOB_PRIORITY_MAX,
        "EXPEDITE urgency returns FLUX_JOB_PRIORITY_MAX");
}

/*
 * A normal-urgency job's priority is the weighted sum of its factors. Two jobs
 * with the same factors but a higher queue factor sort higher.
 */
static void weighted_sum_orders_by_factors ()
{
    // fairshare only: 100000 * 0.5 == 50000
    int64_t base = calc_priority (0.5, 0, 0.0, FLUX_JOB_URGENCY_DEFAULT,
                                  weights);
    ok (base == 50000,
        "default-urgency priority is the weighted sum of its factors");

    // a higher queue factor increases priority: + 10000 * 5 == 100000
    int64_t higher_queue =
        calc_priority (0.5, 5, 0.0, FLUX_JOB_URGENCY_DEFAULT, weights);
    ok (higher_queue == 100000,
        "a higher queue factor yields a higher priority");
    ok (higher_queue > base,
        "job in a higher-priority queue sorts ahead of one in a lower queue");

    // urgency above default raises priority: + 1000 * (20 - 16) == 4000
    int64_t higher_urgency =
        calc_priority (0.5, 0, 0.0, 20, weights);
    ok (higher_urgency == 54000,
        "urgency above default raises priority");
    ok (higher_urgency > base,
        "job with higher urgency sorts ahead of a default-urgency job");
}

/*
 * A weighted sum that comes out negative clamps to the minimum priority rather
 * than returning a negative value.
 */
static void negative_sum_clamps_to_min ()
{
    // a negative queue weight with a positive queue factor drives the sum
    // negative: 0 + (-10000) * 5 == -50000
    std::map<std::string, int> neg_weights = {
        {"fairshare", 0},
        {"queue", -10000},
        {"bank", 0},
        {"urgency", 0},
    };
    int64_t prio =
        calc_priority (0.0, 5, 0.0, FLUX_JOB_URGENCY_DEFAULT, neg_weights);
    ok (prio == FLUX_JOB_PRIORITY_MIN,
        "a negative weighted sum clamps to FLUX_JOB_PRIORITY_MIN");
}

/*
 * Missing priority factor weights should not throw through the jobtap plugin.
 * Treat missing factors as default-weight contributions.
 */
static void missing_weights_default_to_plugin_defaults ()
{
    std::map<std::string, int> partial_weights = {
        {"queue", 1},
    };
    int64_t expected =
        (DEFAULT_FSHARE_WEIGHT / 2) +
        (1 * 5) +
        (DEFAULT_BANK_WEIGHT * 1) +
        (DEFAULT_URGENCY_WEIGHT * (20 - FLUX_JOB_URGENCY_DEFAULT));
    int64_t prio = calc_priority (0.5,
                                  5,
                                  1.0,
                                  20,
                                  partial_weights);
    ok (prio == expected,
        "missing priority factor weights use plugin defaults");
}

/*
 * A rounded value above FLUX_JOB_PRIORITY_MAX clamps before conversion to
 * int64_t, avoiding undefined behavior.
 */
static void upper_sum_clamps_to_max ()
{
    std::map<std::string, int> high_weights = {
        {"fairshare", 0},
        {"queue", DEFAULT_QUEUE_WEIGHT},
        {"bank", 0},
        {"urgency", 0},
    };
    int64_t prio = calc_priority (0.0,
                                  500000,
                                  0.0,
                                  FLUX_JOB_URGENCY_DEFAULT,
                                  high_weights);
    ok (prio == FLUX_JOB_PRIORITY_MAX,
        "a high weighted sum clamps to FLUX_JOB_PRIORITY_MAX");
}

static double fixture_number (json_t *vars, const char *name)
{
    return json_number_value (json_object_get (vars, name));
}

static int fixture_integer (json_t *object, const char *name)
{
    return static_cast<int> (json_integer_value (json_object_get (object,
                                                                  name)));
}

/*
 * Consume the shared default-formula fixtures used by the Python expression
 * tests. PR #1 fixtures do not include compiled trees, so C++ only needs the
 * resolved variables and expected result.
 */
static void shared_default_formula_fixtures ()
{
    const char *fixture_dir = getenv ("PRIORITY_FIXTURES_DIR");
    if (!fixture_dir) {
        ok (false, "PRIORITY_FIXTURES_DIR is set");
        return;
    }

    std::string path = std::string (fixture_dir) + "/fixtures.json";
    json_error_t error;
    json_t *fixtures = json_load_file (path.c_str (), 0, &error);
    if (!fixtures) {
        ok (false, "shared priority fixtures load");
        return;
    }

    size_t index;
    json_t *fixture;
    json_array_foreach (fixtures, index, fixture) {
        json_t *vars = json_object_get (fixture, "vars");
        const char *name = json_string_value (json_object_get (fixture,
                                                               "name"));
        std::map<std::string, int> fixture_weights = {
            {"fairshare",
             static_cast<int> (fixture_number (vars, "w_fairshare"))},
            {"queue", static_cast<int> (fixture_number (vars, "w_queue"))},
            {"bank", static_cast<int> (fixture_number (vars, "w_bank"))},
            {"urgency", static_cast<int> (fixture_number (vars, "w_urgency"))},
        };
        int64_t priority = calc_priority (
            fixture_number (vars, "fairshare"),
            static_cast<int> (fixture_number (vars, "queue_priority")),
            fixture_number (vars, "bank_priority"),
            fixture_integer (fixture, "urgency"),
            fixture_weights);

        ok (priority == json_integer_value (json_object_get (fixture,
                                                             "expected")),
            "%s", name);
        ok (json_is_false (json_object_get (fixture, "fallback")),
            "%s: fallback is false for default formula", name);
    }

    json_decref (fixtures);
}

int main (int argc, char *argv[])
{
    hold_urgency_returns_min ();
    expedite_urgency_returns_max ();
    weighted_sum_orders_by_factors ();
    negative_sum_clamps_to_min ();
    missing_weights_default_to_plugin_defaults ();
    upper_sum_clamps_to_max ();
    shared_default_formula_fixtures ();

    // indicate we are done testing
    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
