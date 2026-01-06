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
import argparse
import signal
import sys

import flux
from flux.job import JournalConsumer
from flux.eventlog import EventLogFormatter


# pylint: disable=broad-except
def main():
    parser = argparse.ArgumentParser(description="stream Flux job events in real-time")
    parser.add_argument(
        "-s",
        "--since",
        type=float,
        default=0.0,
        help="Only show events after this timestamp (default: 0.0 for all history)",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "-t",
        "--timestamp-format",
        choices=["raw", "iso", "offset", "human"],
        default="raw",
        help="Timestamp format (default: raw)",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip historical events, only show new events",
    )
    parser.add_argument(
        "-S",
        "--show-sentinel",
        action="store_true",
        help="Show sentinel event marking end of historical data",
    )
    args = parser.parse_args()

    flux_handle = flux.Flux()

    # create formatter for output
    formatter = EventLogFormatter(
        format=args.format,
        timestamp_format=args.timestamp_format,
        color="never",
    )

    consumer = JournalConsumer(
        flux_handle,
        full=not args.no_history,
        since=args.since,
        include_sentinel=args.show_sentinel,
    ).start()

    # handle Ctrl+C gracefully
    def signal_handler(signum, frame):
        print("\nStopping consumer...", file=sys.stderr)
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            event = consumer.poll()

            # "None" indicates end of stream
            if event is None:
                break

            # handle sentinel event
            if event.is_empty():
                if args.show_sentinel:
                    print(event, flush=True)
                continue

            if args.format == "json":
                # for JSON format, include jobid
                print(f'{{"jobid":"{event.jobid.f58}",', end="")
                print(formatter.format(event)[1:], flush=True)
            else:
                # for text format, show jobid and formatted event
                print(f"{event.jobid.f58}: {formatter.format(event)}", flush=True)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        consumer.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
