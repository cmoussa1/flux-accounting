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

#include <map>
#include <string>

#include "src/plugins/accounting.hpp"
#include "src/plugins/job.hpp"
#include "src/common/libtap/tap.h"

bool deny_unknown_queues = false;


static Job make_job (int nodes, int cores, const std::string &queue)
{
    Job job;

    job.queue = queue;
    job.resources["node"] = nodes;
    job.resources["core"] = cores;

    return job;
}


static void test_default_unlimited ()
{
    std::map<std::string, Queue> queues;
    std::map<std::string, int> queue_nodes;
    std::map<std::string, int> queue_cores;
    Job job = make_job (1, 1, "bronze");

    queues["bronze"].name = "bronze";

    ok (under_queue_total_max_sched_nodes (job,
                                           "bronze",
                                           queues,
                                           queue_nodes),
        "default queue total max_nodes is unlimited");
    ok (under_queue_total_max_sched_cores (job,
                                           "bronze",
                                           queues,
                                           queue_cores),
        "default queue total max_cores is unlimited");
}


static void test_configured_caps ()
{
    std::map<std::string, Queue> queues;
    std::map<std::string, int> queue_nodes;
    std::map<std::string, int> queue_cores;
    Job job = make_job (2, 4, "bronze");

    queues["bronze"].name = "bronze";
    queues["bronze"].max_nodes = 4;
    queues["bronze"].max_cores = 8;
    queue_nodes["bronze"] = 2;
    queue_cores["bronze"] = 4;

    ok (under_queue_total_max_sched_nodes (job,
                                           "bronze",
                                           queues,
                                           queue_nodes),
        "job fits within configured queue total max_nodes");
    ok (under_queue_total_max_sched_cores (job,
                                           "bronze",
                                           queues,
                                           queue_cores),
        "job fits within configured queue total max_cores");
}


static void test_load_queue_total_limits ()
{
    std::map<std::string, Queue> queues;
    std::string errmsg;
    json_t *data = json_pack (
        "[{s:s, s:i, s:i, s:i, s:i, s:i, s:i, s:i, s:i, s:i, s:i, s:i}]",
        "queue", "bronze",
        "min_nodes_per_job", 0,
        "max_nodes_per_job", 2147483647,
        "max_time_per_job", 2147483647,
        "priority", 0,
        "max_running_jobs", 2147483647,
        "max_nodes_per_assoc", 2147483647,
        "max_nodes", 3,
        "max_cores", 6,
        "max_sched_jobs", 2147483647,
        "max_sched_nodes_per_assoc", 2147483647,
        "max_sched_cores_per_assoc", 2147483647);

    ok (data != nullptr,
        "queue payload JSON is created");
    ok (load_queues (data, queues, &errmsg) == 0,
        "load_queues accepts queue total limits");
    ok (queues["bronze"].max_nodes == 3,
        "load_queues stores queue total max_nodes");
    ok (queues["bronze"].max_cores == 6,
        "load_queues stores queue total max_cores");

    json_decref (data);
}


static void test_missing_queue ()
{
    std::map<std::string, Queue> queues;
    std::map<std::string, int> queue_nodes;
    std::map<std::string, int> queue_cores;
    Job job = make_job (8, 16, "unknown");

    queue_nodes["unknown"] = 1024;
    queue_cores["unknown"] = 1024;

    ok (under_queue_total_max_sched_nodes (job,
                                           "unknown",
                                           queues,
                                           queue_nodes),
        "missing queue is unlimited for total max_nodes");
    ok (under_queue_total_max_sched_cores (job,
                                           "unknown",
                                           queues,
                                           queue_cores),
        "missing queue is unlimited for total max_cores");
}


static void test_blocked_totals ()
{
    std::map<std::string, Queue> queues;
    std::map<std::string, int> queue_nodes;
    std::map<std::string, int> queue_cores;
    Job job = make_job (1, 1, "bronze");

    queues["bronze"].name = "bronze";
    queues["bronze"].max_nodes = 4;
    queues["bronze"].max_cores = 8;
    queue_nodes["bronze"] = 4;
    queue_cores["bronze"] = 8;

    ok (!under_queue_total_max_sched_nodes (job,
                                            "bronze",
                                            queues,
                                            queue_nodes),
        "queue total max_nodes blocks over-limit job");
    ok (!under_queue_total_max_sched_cores (job,
                                            "bronze",
                                            queues,
                                            queue_cores),
        "queue total max_cores blocks over-limit job");
}


static void test_pending_counters ()
{
    std::map<std::string, Queue> queues;
    std::map<std::string, int> queue_nodes;
    std::map<std::string, int> queue_cores;
    Job job = make_job (2, 4, "bronze");

    queues["bronze"].name = "bronze";
    queues["bronze"].max_nodes = 4;
    queues["bronze"].max_cores = 8;
    queue_nodes["bronze"] = 0;
    queue_cores["bronze"] = 0;

    ok (under_queue_total_max_sched_nodes (job,
                                           "bronze",
                                           queues,
                                           queue_nodes,
                                           2),
        "pending nodes allow job that exactly fills queue total");
    ok (!under_queue_total_max_sched_nodes (job,
                                            "bronze",
                                            queues,
                                            queue_nodes,
                                            3),
        "pending nodes block job that would exceed queue total");
    ok (under_queue_total_max_sched_cores (job,
                                           "bronze",
                                           queues,
                                           queue_cores,
                                           4),
        "pending cores allow job that exactly fills queue total");
    ok (!under_queue_total_max_sched_cores (job,
                                            "bronze",
                                            queues,
                                            queue_cores,
                                            5),
        "pending cores block job that would exceed queue total");
}


static void test_inactive_decrement ()
{
    std::map<std::string, Queue> queues;
    std::map<std::string, int> queue_nodes;
    std::map<std::string, int> queue_cores;
    Job job = make_job (2, 4, "bronze");

    queues["bronze"].name = "bronze";
    queues["bronze"].max_nodes = 4;
    queues["bronze"].max_cores = 8;
    queue_nodes["bronze"] = 4;
    queue_cores["bronze"] = 8;

    ok (!under_queue_total_max_sched_nodes (job,
                                            "bronze",
                                            queues,
                                            queue_nodes),
        "full queue total max_nodes blocks job");
    ok (!under_queue_total_max_sched_cores (job,
                                            "bronze",
                                            queues,
                                            queue_cores),
        "full queue total max_cores blocks job");

    queue_nodes["bronze"] -= 2;
    queue_cores["bronze"] -= 4;

    ok (under_queue_total_max_sched_nodes (job,
                                           "bronze",
                                           queues,
                                           queue_nodes),
        "inactive decrement restores queue total max_nodes headroom");
    ok (under_queue_total_max_sched_cores (job,
                                           "bronze",
                                           queues,
                                           queue_cores),
        "inactive decrement restores queue total max_cores headroom");
}


int main (int argc, char *argv[])
{
    test_default_unlimited ();
    test_configured_caps ();
    test_load_queue_total_limits ();
    test_missing_queue ();
    test_blocked_totals ();
    test_pending_counters ();
    test_inactive_decrement ();

    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
