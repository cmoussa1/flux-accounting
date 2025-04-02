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
    
#include "src/plugins/job.hpp"
#include "src/common/libtap/tap.h"


void test_job_default_initialization ()
{
    Job job;

    ok (job.id == 0, "job ID is set to a default value of 0");
    ok (job.nnodes == 0, "job nnodes count is set to a default value of 0");
    ok (job.ncores == 0, "job ncores count is set to a default value of 0");
    ok (job.deps.size () == 0, "job dependencies list is empty");
}


void test_job_parameterized_initialization ()
{
    Job job;
    job.id = 1;
    job.nnodes = 16;
    job.ncores = 8;
    job.deps.push_back ("dependency1");
    job.deps.push_back ("dependency2");

    ok (job.id == 1, "job ID is initialized on object construction");
    ok (job.nnodes == 16, "job nnodes can be defined");
    ok (job.ncores == 8, "job ncores can be defined");
    ok (job.deps.size () == 2, "job dependencies list has 2 dependencies");
    ok (job.deps[0] == "dependency1", "first dependency is dependency1");
    ok (job.deps[1] == "dependency2", "second dependency is dependency2");
}


void test_job_contains_dep_success ()
{
    Job job;
    job.id = 2;
    job.deps.push_back ("dependency1");
    
    ok (contains_dep (job, "dependency1") == true,
        "contains_dep () returns true on success");
}


void test_job_contains_dep_failure ()
{
    Job job;
    job.id = 3;
    
    ok (contains_dep (job, "foo") == false,
        "contains_dep () returns false on failure");
}


void test_job_remove_dep_success ()
{
    Job job;
    job.id = 4;
    job.deps.push_back ("dependency1");
    job.deps.push_back ("dependency2");
    job.deps.push_back ("dependency3");
    
    ok (job.deps.size () == 3, "job dependencies list has 3 dependencies");
    remove_dep (job, "dependency1");
    ok (job.deps.size () == 2, "job dependencies get successfully removed");
    ok (job.deps[0] == "dependency2", "dependency2 moves to first slot");
    ok (job.deps[1] == "dependency3", "dependency3 moves to second slot");
}


void test_job_remove_dep_failure ()
{
    Job job;
    job.id = 5;
    job.deps.push_back ("dependency1");
    
    ok (job.deps.size () == 1, "job dependencies list has 1 dependency");
    remove_dep (job, "foo");
    ok (job.deps.size () == 1,
        "job dependencies list in tact after trying to remove nonexistent dependency");
}


int main (int argc, char* argv[])
{
    test_job_default_initialization ();
    test_job_parameterized_initialization ();
    test_job_contains_dep_success ();
    test_job_contains_dep_failure ();
    test_job_remove_dep_success ();
    test_job_remove_dep_failure ();

    done_testing ();

    return EXIT_SUCCESS;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
