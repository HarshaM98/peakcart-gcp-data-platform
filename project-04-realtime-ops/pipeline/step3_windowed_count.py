"""
Adds 1-minute fixed windowing and a per-zone order count.
Still printing results directly, no BigQuery write yet.

TEMPORARY: key_by_zone uses a defensive .get() fallback for events
missing warehouse_zone (simulator messiness). Proper dead-letter
handling for malformed events comes in the next slice.
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, TimestampedValue

PROJECT_ID = "harsha-data-platform"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-order-events-sub"


def parse_json(raw_bytes):
    return json.loads(raw_bytes.decode("utf-8"))


def attach_event_time(event: dict):
    event_dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    event_dt = event_dt.replace(tzinfo=timezone.utc)
    return TimestampedValue(event, event_dt.timestamp())


def key_by_zone(event: dict):
    # Temporary defensive fallback: some events deliberately lack this
    # field (simulator messiness). Proper handling, routing these to a
    # dead-letter destination instead of guessing, comes in the next slice.
    return (event.get("warehouse_zone", "unknown_zone"), event)


class FormatWindowedCount(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        zone, count = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "warehouse_zone": zone,
            "order_count": count,
        }


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "ParseJson" >> beam.Map(parse_json)
            | "AttachEventTime" >> beam.Map(attach_event_time)
            | "WindowInto1Min" >> beam.WindowInto(FixedWindows(60))
            | "KeyByZone" >> beam.Map(key_by_zone)
            | "CountPerZone" >> beam.combiners.Count.PerKey()
            | "FormatOutput" >> beam.ParDo(FormatWindowedCount())
            | "Print" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
