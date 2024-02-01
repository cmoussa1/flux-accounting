/************************************************************\
 * Copyright 2024 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#include "accounting.hpp"

Association* get_association (int userid,
                              const char *bank,
                              std::map<int, std::map<std::string, Association>>
                                &users,
                              std::map<int, std::string> &users_def_bank)
{
    auto it = users.find (userid);
    if (it == users.end ())
        // user could not be found
        return nullptr;

    std::string b;
    if (bank != NULL)
        b = bank;
    else
        // get the default bank of this user
        b = users_def_bank[userid];

    auto bank_it = it->second.find (b);
    if (bank_it == it->second.end ())
        // user does not have accounting information under the specified bank
        return nullptr;

    return &bank_it->second;
}


int get_queue_info (char *queue,
                    std::vector<std::string> permissible_queues,
                    std::map<std::string, Queue> queues)
{
    if (queue == NULL)
        // no queue was specified; just use the default queue factor
        return NO_QUEUE_SPECIFIED;

    // check #1) the queue passed in exists; if the queue cannot be found,
    // this means that flux-accounting does not know about the queue, and
    // thus should return a default queue factor
    auto q_it = queues.find (queue);
    if (q_it == queues.end ())
        return UNKNOWN_QUEUE;

    // check #2) the queue is a valid one for the user to submit jobs under
    auto vect_it = std::find (permissible_queues.begin (),
                              permissible_queues.end (),
                              queue);
    if (vect_it == permissible_queues.end ())
        return INVALID_QUEUE;

    // return the priority factor associated with validated queue
    return queues[queue].priority;
}
