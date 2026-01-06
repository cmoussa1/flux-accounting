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

import flux
from flux.job import JournalConsumer


def main():
    flux_handle = flux.Flux()
    # grab all events that have happened up to this point
    consumer = JournalConsumer(flux_handle, since=0.0).start()

    count = 0
    events = []
    try:
        while True:
            # read incoming job events
            event = consumer.poll()
            print(f"event: {event}")
            events.append(event)
        #     if event.name == "clean":
        #         count += 1
        #         if count == 12:
        #             break
        # consumer.stop()
    except KeyboardInterrupt:
        consumer.stop()

    # for e in events:
    #     print(f"event: {e}")


if __name__ == "__main__":
    main()
