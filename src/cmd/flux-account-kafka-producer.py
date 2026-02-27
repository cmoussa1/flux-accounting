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
import json

import flux
from flux.job import JournalConsumer
from flux.eventlog import EventLogFormatter
from confluent_kafka import Producer


def format_jobid(jobid_format, event):
    """Get job ID in the requested format."""
    if jobid_format == "f58":
        return event.jobid.f58
    return event.jobid.dec


def format_event_with_ms_epoch(event, output_format):
    """Format event with milliseconds since epoch timestamp.

    Args:
        event: The event to format
        output_format: Either "json" or "text"

    Returns:
        str: The formatted event string
    """
    ms_timestamp = int(event.timestamp * 1000)
    if output_format == "json":
        event_dict = dict(event)
        event_dict["timestamp"] = ms_timestamp
        if not event_dict.get("context"):
            event_dict.pop("context", None)
        return json.dumps(event_dict, separators=(",", ":"), ensure_ascii=False)
    context = ""
    for key, val in event.context.items():
        val_str = json.dumps(val, separators=(",", ":"), ensure_ascii=False)
        context += f" {key}={val_str}"
    return f"{ms_timestamp} {event.name}{context}"


def delivery_report(err, msg):
    """Kafka delivery callback for reporting message delivery status."""
    if err is not None:
        print(f"Message delivery failed: {err}", file=sys.stderr)
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}]", file=sys.stderr
        )


# pylint: disable=broad-except, too-many-statements
def main():
    parser = argparse.ArgumentParser(description="stream Flux job events to Kafka")
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
        default="json",
        help="Output format (default: json)",
    )
    parser.add_argument(
        "-t",
        "--timestamp-format",
        choices=["raw", "iso", "offset", "human", "ms-epoch"],
        default="raw",
        help="Timestamp format (default: raw)",
    )
    parser.add_argument(
        "-j",
        "--jobid-format",
        choices=["dec", "f58"],
        default="dec",
        help="Job ID format",
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
    parser.add_argument(
        "-b",
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "-T",
        "--topic",
        type=str,
        default="flux-job-events",
        help="Kafka topic name (default: flux-job-events)",
    )
    parser.add_argument(
        "--kafka-config",
        type=str,
        action="append",
        help="Additional Kafka configuration as key=value (can be specified multiple times)",
    )
    args = parser.parse_args()

    flux_handle = flux.Flux()

    # create formatter for output
    formatter_ts_format = (
        args.timestamp_format if args.timestamp_format != "ms-epoch" else "raw"
    )
    formatter = EventLogFormatter(
        format=args.format,
        timestamp_format=formatter_ts_format,
        color="never",
    )

    # configure Kafka producer
    kafka_conf = {
        "bootstrap.servers": args.bootstrap_servers,
    }

    # add additional Kafka configuration options
    if args.kafka_config:
        for config in args.kafka_config:
            key, value = config.split("=", 1)
            kafka_conf[key] = value

    producer = Producer(kafka_conf)

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
        # flush any pending messages
        print("Flushing pending Kafka messages...", file=sys.stderr)
        producer.flush()
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
                    print("Sentinel event received", file=sys.stderr)
                continue

            # get job ID in requested format
            jobid_str = format_jobid(args.jobid_format, event)

            # format the event
            if args.timestamp_format == "ms-epoch":
                if args.format == "json":
                    # build complete JSON message
                    message = f'{{"jobid":"{jobid_str}",{format_event_with_ms_epoch(event, args.format)[1:]}'
                else:
                    message = (
                        f"{jobid_str}: {format_event_with_ms_epoch(event, args.format)}"
                    )
            else:
                if args.format == "json":
                    # build complete JSON message
                    message = f'{{"jobid":"{jobid_str}",{formatter.format(event)[1:]}'
                else:
                    message = f"{jobid_str}: {formatter.format(event)}"

            # produce message to Kafka
            producer.produce(
                args.topic,
                value=message.encode("utf-8"),
                key=jobid_str.encode("utf-8"),
                callback=delivery_report,
            )

            # poll to handle delivery callbacks
            producer.poll(0)

    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        consumer.stop()
        producer.flush()
        sys.exit(1)

    # flush any remaining messages
    print("Flushing final Kafka messages...", file=sys.stderr)
    producer.flush()


if __name__ == "__main__":
    main()
