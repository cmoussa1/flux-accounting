/************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/* mf_priority.cpp - custom basic job priority plugin
 *
 */

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
}
#include <flux/core.h>
#include <flux/jobtap.h>
#include <map>
#include <iterator>
#include <cmath>
#include <cassert>
#include <algorithm>
#include <cinttypes>
#include <sstream>
#include <vector>

std::map<int, std::map<std::string, struct bank_info>> users;
std::map<int, std::string> users_def_bank;
std::map<std::string, int> qos_map;

struct bank_info {
    double fairshare;
    int max_jobs;
    int current_jobs;
    int qos_factor;
    std::vector<std::string> avail_qos;
};

/******************************************************************************
 *                                                                            *
 *                           Helper Functions                                 *
 *                                                                            *
 *****************************************************************************/

/*
 * Calculate a user's job priority using the following factors:
 *
 * fairshare: the ratio between the amount of resources allocated vs. resources
 *     consumed.
 * qos: a Quality of Service (QoS) factor that can further increase or decrease
 *     the priority of a job based on the QoS passed in.
 * urgency: a user-controlled factor to prioritize their own jobs.
 */
int64_t priority_calculation (flux_plugin_t *p,
                              flux_plugin_arg_t *args,
                              int userid,
                              char *bank,
                              int urgency)
{
    double fshare_factor = 0.0, priority = 0.0;
    int qos_factor = 0;

    int fshare_weight, qos_weight;
    struct bank_info *b;

    fshare_weight = 100000;
    qos_weight = 1000;

    if (urgency == FLUX_JOB_URGENCY_HOLD)
        return FLUX_JOB_PRIORITY_MIN;

    if (urgency == FLUX_JOB_URGENCY_EXPEDITE)
        return FLUX_JOB_PRIORITY_MAX;

    b = static_cast<bank_info *> (flux_jobtap_job_aux_get (
                                                    p,
                                                    FLUX_JOBTAP_CURRENT_JOB,
                                                    "mf_priority:bank_info"));

    if (b == NULL) {
        flux_jobtap_raise_exception (p, FLUX_JOBTAP_CURRENT_JOB, "plugin",
                                     3, "mf_priority: bank info is missing; "
                                        "holding job");
        return 0;
    }

    // get factors for priority calculation from passed in bank_info struct
    fshare_factor = b->fairshare;
    qos_factor = b->qos_factor;

    priority = round ((fshare_weight * fshare_factor) +
                      (qos_weight * qos_factor) +
                      (urgency - 16));

    if (priority < 0)
        return FLUX_JOB_PRIORITY_MIN;

    return priority;
}


/******************************************************************************
 *                                                                            *
 *                               Callbacks                                    *
 *                                                                            *
 *****************************************************************************/

/*
 * Unpack a payload from an external bulk update service and place it in the
 * multimap datastructure.
 */
static void get_users_cb (flux_t *h,
                           flux_msg_handler_t *mh,
                           const flux_msg_t *msg,
                           void *arg)
{
    char *uid, *fshare, *bank, *default_bank, *max_jobs, *qos;
    std::stringstream s_stream;

    if (flux_request_unpack (msg, NULL, "{s:s, s:s, s:s, s:s, s:s, s:s}",
                             "userid", &uid,
                             "bank", &bank,
                             "default_bank", &default_bank,
                             "fairshare", &fshare,
                             "max_jobs", &max_jobs,
                             "qos", &qos) < 0) {
        flux_log_error (h, "failed to unpack custom_priority.trigger msg");
        goto error;
    }

    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "flux_respond");

    struct bank_info *b;
    b = &users[std::atoi(uid)][bank];

    b->fairshare = std::atof (fshare);
    b->max_jobs = std::atoi (max_jobs);

    s_stream << qos; // create string stream from the string
    while (s_stream.good ()) {
        std::string substr;
        getline (s_stream, substr, ','); // get first string delimited by comma
        b->avail_qos.push_back (substr);
    }

    users_def_bank[std::atoi (uid)] = default_bank;

    return;
error:
    flux_respond_error (h, msg, errno, flux_msg_last_error (msg));
}


/*
 * Unpack a QoS payload from an external bulk update service and place it in a
 * multimap datastructure.
 */
static void get_qos_cb (flux_t *h,
                        flux_msg_handler_t *mh,
                        const flux_msg_t *msg,
                        void *arg)
{
    char *qos, *priority;

    if (flux_request_unpack (msg, NULL, "{s:s, s:s}",
                             "qos", &qos,
                             "priority", &priority) < 0) {
        flux_log_error (h, "failed to unpack custom_priority.trigger msg");
        goto error;
    }

    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "flux_respond");

    qos_map[qos] = std::atoi (priority);

    return;
error:
    flux_respond_error (h, msg, errno, flux_msg_last_error (msg));
}


/*
 * Unpack the urgency and userid from a submitted job and call
 * priority_calculation (), which will return a new job priority to be packed.
 */
static int priority_cb (flux_plugin_t *p,
                        const char *topic,
                        flux_plugin_arg_t *args,
                        void *data)
{
    int urgency, userid;
    char *bank = NULL;
    int64_t priority;

    flux_t *h = flux_jobtap_get_flux (p);
    if (flux_plugin_arg_unpack (args,
                                FLUX_PLUGIN_ARG_IN,
                                "{s:i, s:i, s{s{s{s?s}}}}",
                                "urgency", &urgency,
                                "userid", &userid,
                                "jobspec", "attributes", "system",
                                "bank", &bank) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "flux_plugin_arg_unpack: %s",
                  flux_plugin_arg_strerror (args));
        return -1;
    }

    priority = priority_calculation (p, args, userid, bank, urgency);

    if (flux_plugin_arg_pack (args,
                              FLUX_PLUGIN_ARG_OUT,
                              "{s:I}",
                              "priority",
                              priority) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "flux_plugin_arg_pack: %s",
                  flux_plugin_arg_strerror (args));
        return -1;
    }
    return 0;
}


/*
 * Look up the userid of the submitted job in the multimap; if user is not found
 * in the map, reject the job saying the user wasn't found in the
 * flux-accounting database.
 */
static int validate_cb (flux_plugin_t *p,
                        const char *topic,
                        flux_plugin_arg_t *args,
                        void *data)
{
    int userid;
    char *bank = NULL;
    char *qos = NULL;
    int current_jobs, max_jobs = 0;
    double fairshare = 0.0;

    std::map<int, std::map<std::string, struct bank_info>>::iterator it;
    std::map<std::string, struct bank_info>::iterator bank_it;
    std::map<std::string, int>::iterator qos_it;

    flux_t *h = flux_jobtap_get_flux (p);
    if (flux_plugin_arg_unpack (args,
                                FLUX_PLUGIN_ARG_IN,
                                "{s:i, s{s{s{s?s, s?s}}}}",
                                "userid", &userid,
                                "jobspec", "attributes", "system",
                                "bank", &bank, "qos", &qos) < 0) {
        return flux_jobtap_reject_job (p, args, "unable to unpack arg(s)");
    }

    // make sure user belongs to flux-accounting DB
    it = users.find (userid);
    if (it == users.end ())
        return flux_jobtap_reject_job (p, args,
                                       "user not found in flux-accounting DB");

    // make sure user belongs to bank they specified; if no bank was passed in,
    // look up their default bank
    if (bank != NULL) {
        bank_it = it->second.find (std::string (bank));
        if (bank_it == it->second.end ())
            return flux_jobtap_reject_job (p, args,
                                     "user does not belong to specified bank");
    } else {
        bank = const_cast<char*> (users_def_bank[userid].c_str ());
        bank_it = it->second.find (std::string (bank));
        if (bank_it == it->second.end ())
            return flux_jobtap_reject_job (p, args,
                                     "user/default bank entry does not exist");
    }

    max_jobs = bank_it->second.max_jobs;
    current_jobs = bank_it->second.current_jobs;
    fairshare = bank_it->second.fairshare;

    // make sure that if a QoS is passed in, it 1) exists, and 2) is a valid
    // QoS for the user to pass in
    if (qos != NULL) {
        // checking 1) the QoS passed in exists in qos_map
        qos_it = qos_map.find (qos);
        if (qos_it == qos_map.end ())
            return flux_jobtap_reject_job (p, args, "QoS does not exist");

        // checking 2) the QoS passed in is a valid option to pass for user
        std::vector<std::string>::iterator vect_it;
        vect_it = std::find (bank_it->second.avail_qos.begin (),
                           bank_it->second.avail_qos.end (), qos);

        if (vect_it == bank_it->second.avail_qos.end ())
            return flux_jobtap_reject_job (p, args, "QoS not valid for user");
        else
            bank_it->second.qos_factor = qos_map[qos];
    }

    // if a user's fairshare value is 0, that means they shouldn't be able
    // to run jobs on a system
    if (fairshare == 0)
        return flux_jobtap_reject_job (p, args, "user fairshare value is 0");

    // make sure user has not already hit their max active jobs count
    if (max_jobs > 0 && current_jobs >= max_jobs)
        return flux_jobtap_reject_job (p, args,
                                       "user has max number of jobs submitted");

    if (flux_jobtap_job_aux_set (p,
                                 FLUX_JOBTAP_CURRENT_JOB,
                                 "mf_priority:bank_info",
                                 &bank_it->second,
                                 NULL) < 0)
        flux_log_error (h, "flux_jobtap_job_aux_set");

    bank_it->second.current_jobs++;

    return 0;
}


static int inactive_cb (flux_plugin_t *p,
                        const char *topic,
                        flux_plugin_arg_t *args,
                        void *data)
{
    int userid;
    struct bank_info *b;

    flux_t *h = flux_jobtap_get_flux (p);
    if (flux_plugin_arg_unpack (args,
                                FLUX_PLUGIN_ARG_IN,
                                "{s:i}",
                                "userid", &userid) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "flux_plugin_arg_unpack: %s",
                  flux_plugin_arg_strerror (args));
        return -1;
    }

    b = static_cast<bank_info *> (flux_jobtap_job_aux_get (
                                                    p,
                                                    FLUX_JOBTAP_CURRENT_JOB,
                                                    "mf_priority:bank_info"));

    if (b == NULL)
        flux_jobtap_raise_exception (p, FLUX_JOBTAP_CURRENT_JOB, "plugin",
                                     3, "mf_priority: bank info is missing");

    b->current_jobs--;

    return 0;
}


static const struct flux_plugin_handler tab[] = {
    { "job.validate", validate_cb, NULL },
    { "job.state.priority", priority_cb, NULL },
    { "job.priority.get", priority_cb, NULL },
    { "job.state.inactive", inactive_cb, NULL },
    { 0 },
};


extern "C" int flux_plugin_init (flux_plugin_t *p)
{
    if (flux_plugin_register (p, "mf_priority", tab) < 0
        || flux_jobtap_service_register (p, "get_users", get_users_cb, p) < 0
        || flux_jobtap_service_register (p, "get_qos", get_qos_cb, p) < 0)
        return -1;
    return 0;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
