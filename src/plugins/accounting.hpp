/************************************************************\
 * Copyright 2024 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

// header file for the Accounting class

#ifndef ACCOUNTING_H
#define ACCOUNTING_H

#include <vector>
#include <string>
#include <map>
#include <iterator>
#include <algorithm>
#include <iostream>

// all attributes are per-user/bank
class Association {
public:
    std::string bank_name;           // name of bank
    double fairshare;                // fair share value
    int max_run_jobs;                // max number of running jobs
    int cur_run_jobs;                // current number of running jobs 
    int max_active_jobs;             // max number of active jobs
    int cur_active_jobs;             // current number of active jobs
    std::vector<long int> held_jobs; // list of currently held job ID's
    std::vector<std::string> queues; // list of accessible queues
    int queue_factor;                // priority factor associated with queue
    int active;                      // active status
};

// min_nodes_per_job, max_nodes_per_job, and max_time_per_job are not
// currently used or enforced in this plugin, so their values have no
// effect in queue limit enforcement
class Queue {
public:
    int min_nodes_per_job;
    int max_nodes_per_job;
    int max_time_per_job;
    int priority;
};

// UNKNOWN_QUEUE: a queue that flux-accounting does not know about
// NO_QUEUE_SPECIFIED: no queue was specified for the submitted job
// INVALID_QUEUE: user does not have access vto submit jobs to this queue
#define UNKNOWN_QUEUE 0
#define NO_QUEUE_SPECIFIED 0
#define INVALID_QUEUE -1

// get an Association object that points to user/bank in the users map;
// return nullptr on failure
Association* get_association (int userid,
                              const char *bank,
                              std::map<int, std::map<std::string, Association>>
                                &users,
                              std::map<int, std::string> &users_def_bank);

// validate a queue for a given Association; return the priority
// factor associated with the validated or invalidated queue
int get_queue_info (char *queue,
                    std::vector<std::string> permissible_queues,
                    std::map<std::string, Queue> queues);

#endif // ACCOUNTING_H
