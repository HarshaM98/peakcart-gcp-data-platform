"""
Adds JSON parsing and explicit event-time timestamping to the pipeline.
Still no windowing or BigQuery writes, just proving we can turn raw
bytes into a structured record with the correct event time attached.
"""

import json
import time
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import TimestampedValue

PROJECT_ID = "harsha-data-platform"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-order-events-sub"


def parse_json(raw_bytes):
    return json.loads(raw_bytes.decode("utf-8"))


def attach_event_time(event: dict):
    """Converts the event's ISO 8601 timestamp string into a Unix epoch
    float, and wraps the event in a TimestampedValue so Beam uses this
    as the record's event time instead of processing time."""
    event_dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    event_dt = event_dt.replace(tzinfo=timezone.utc)
    epoch_seconds = event_dt.timestamp()
    return TimestampedValue(event, epoch_seconds)


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "ParseJson" >> beam.Map(parse_json)
            | "AttachEventTime" >> beam.Map(attach_event_time)
            | "PrintParsed" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
