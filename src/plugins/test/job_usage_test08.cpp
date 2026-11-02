/************************************************************\
 * Copyright 2025 Lawrence Livermore National Security, LLC
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

#include "src/plugins/accounting.hpp"
#include "src/common/libtap/tap.h"

static Job make_job ()
{
    Job j;
    j.resources["node"] = 2;
    j.resources["core"] = 8;
    return j;
}


static bool empty_usage (const Association &a)
{
    if (a.cur_active_jobs || a.cur_run_jobs || a.cur_sched_jobs
        || a.cur_nodes || a.cur_cores)
        return false;
    for (const auto &entry : a.queue_usage) {
        const QueueUsage &q = entry.second;
        if (q.cur_run_jobs || q.cur_nodes || q.cur_sched_jobs
            || q.cur_sched_nodes || q.cur_sched_cores)
            return false;
    }
    return true;
}


static void test_cancel_before_data ()
{
    Association temporary = {};
    Association real = {};
    Job j = make_job ();

    ok (j.charge_active (&temporary), "charge active job without data");
    ok (!j.charge_active (&temporary), "do not charge active job twice");
    ok (j.charge_sched (&temporary, "debug"), "reconstruct SCHED usage");
    ok (j.release_active (), "cancel releases active charge");
    ok (!j.release_run (), "cancel does not release uncharged running usage");
    ok (j.release_sched_jobs (), "cancel releases SCHED job");
    ok (j.release_sched_resources (), "cancel releases SCHED resources");
    ok (!j.release_active () && !j.release_sched_jobs ()
        && !j.release_sched_resources (), "repeated release is harmless");
    ok (!j.move_usage (&real, "debug"), "late data cannot charge finished job");
    ok (empty_usage (temporary) && empty_usage (real),
        "both associations remain empty");
}


static void test_transfer_sched_and_run ()
{
    Association temporary = {};
    Association a = {};
    Association b = {};
    Job running = make_job ();
    Job pending = make_job ();

    running.charge_active (&temporary);
    running.charge_sched (&temporary, "debug");
    running.charge_run (&temporary, "debug");
    running.release_sched_jobs ();
    pending.charge_active (&temporary);
    pending.charge_sched (&temporary, "debug");
    ok (temporary.cur_active_jobs == 2 && temporary.cur_run_jobs == 1
        && temporary.cur_sched_jobs == 1
        && temporary.queue_usage["debug"].cur_sched_nodes == 4,
        "multiple jobs share temporary counters");

    ok (running.move_usage (&a, "debug"), "move running usage to bank A");
    ok (pending.move_usage (&b, "debug"), "move pending usage to bank B");
    ok (empty_usage (temporary), "all temporary charges transferred");
    ok (a.cur_active_jobs == 1 && a.cur_run_jobs == 1
        && a.cur_nodes == 2 && a.cur_cores == 8 && a.cur_sched_jobs == 0
        && a.queue_usage["debug"].cur_sched_nodes == 2
        && a.queue_usage["debug"].cur_sched_cores == 8,
        "running job keeps resource reservation but no SCHED job charge");
    ok (b.cur_active_jobs == 1 && b.cur_run_jobs == 0
        && b.cur_sched_jobs == 1 && b.cur_nodes == 0
        && b.queue_usage["debug"].cur_sched_cores == 8,
        "pending job keeps only active and SCHED charges");
    ok (!running.move_usage (&a, "debug")
        && !pending.move_usage (&b, "debug"), "repeated update is a no-op");
    ok (!running.charge_run (&a, "debug"), "RUN replay cannot double charge");

    running.release_active ();
    running.release_run ();
    running.release_sched_jobs ();
    running.release_sched_resources ();
    pending.release_active ();
    pending.release_run ();
    pending.release_sched_jobs ();
    pending.release_sched_resources ();
    ok (empty_usage (a) && empty_usage (b), "completion releases real charges");
}


static void test_move_preserves_charged_resources ()
{
    Association a = {};
    Association b = {};
    Job j = make_job ();

    j.charge_active (&a);
    j.charge_sched (&a, "old");
    j.charge_run (&a, "old");
    j.release_sched_jobs ();
    j.resources["node"] = 99;
    j.resources["core"] = 999;
    ok (j.move_usage (&b, "new"), "move bank and queue");
    ok (empty_usage (a), "original queue and bank have no remaining usage");
    ok (b.cur_nodes == 2 && b.cur_cores == 8
        && b.queue_usage["new"].cur_nodes == 2
        && b.queue_usage["new"].cur_run_jobs == 1
        && b.queue_usage["new"].cur_sched_nodes == 2
        && b.queue_usage["new"].cur_sched_cores == 8,
        "transfer uses charged resources, not a newer jobspec");
    j.release_active ();
    j.release_run ();
    j.release_sched_resources ();
    ok (empty_usage (b), "release uses transferred resource snapshot");
    ok (!j.release_run (), "running release is idempotent");
}


static void test_active_only_cleanup ()
{
    Association temporary = {};
    Association real = {};
    Job j = make_job ();

    j.charge_active (&temporary);
    ok (j.move_usage (&real, ""), "transfer an unallocated CLEANUP job");
    ok (real.cur_active_jobs == 1 && real.cur_run_jobs == 0
        && real.cur_sched_jobs == 0 && real.cur_nodes == 0
        && real.cur_cores == 0,
        "unallocated CLEANUP does not invent resource charges");
    j.release_active ();
    ok (empty_usage (temporary) && empty_usage (real),
        "unallocated CLEANUP releases exactly its active charge");
}


int main (int argc, char *argv[])
{
    plan (NO_PLAN);
    test_cancel_before_data ();
    test_transfer_sched_and_run ();
    test_move_preserves_charged_resources ();
    test_active_only_cleanup ();
    done_testing ();
    return 0;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
