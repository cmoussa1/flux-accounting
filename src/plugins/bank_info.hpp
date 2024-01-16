/************************************************************\
 * Copyright 2023 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

// header file for the bank_info class
extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/core.h>
#include <flux/jobtap.h>
#include <jansson.h>
}

#ifndef BANK_INFO_H
#define BANK_INFO_H

#include <vector>
#include <string>
#include <map>
#include <iterator>
#include <algorithm>

// a project was specified for a submitted job that flux-accounting does not
// know about or that the user/bank does not have permission to run jobs under
#define INVALID_PROJECT -6

// all attributes are per-user/bank
class user_bank_info {
public:
    // attributes
    std::string bank_name;             // name of bank
    double fairshare;                  // fair share value
    int max_run_jobs;                  // max number of running jobs
    int cur_run_jobs;                  // current number of running jobs
    int max_active_jobs;               // max number of active jobs
    int cur_active_jobs;               // current number of active jobs
    std::vector<long int> held_jobs;   // list of currently held job ID's
    std::vector<std::string> queues;   // list of accessible queues
    int queue_factor;                  // priority factor associated with queue
    int active;                        // active status
    std::vector<std::string> projects; // list of accessible projects
    std::string def_project;           // default project

    // methods
    std::string to_json () const;      // convert object to JSON string
};

// these data structures are defined in the priority plugin
extern std::map<int, std::map<std::string, user_bank_info>> users;
extern std::map<int, std::string> users_def_bank;
extern std::vector<std::string> projects;

// get a user_bank_info object that points to user/bank
// information in users map; return NULL on failure
user_bank_info* get_user_info (int userid, char *bank);

// scan the users map and look at each user's default bank to see if any one
// of them have a valid bank, i.e one that is not "DNE"; if any of the users do
// in fact have a valid bank, return false
bool check_map_for_dne_only ();

// iterate through the users map and construct a JSON object of each user/bank
json_t* map_to_json ();

// validate a specified project by checking if it exists in a user/bank's list
// of accessible projects
int validate_project (char *project, std::vector<std::string> user_projects);

#endif // BANK_INFO_H
