"""
Replaces step4's "Print" placeholder on the well-formed branch with a real
write to the orders_per_minute BigQuery table. The malformed branch is
unchanged from step4.
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, TimestampedValue

PROJECT_ID = "harsha-data-platform"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-order-events-sub"
MALFORMED_EVENTS_TABLE = f"{PROJECT_ID}:peakcart_streaming.malformed_events"
ORDERS_PER_MINUTE_TABLE = f"{PROJECT_ID}:peakcart_streaming.orders_per_minute"

REQUIRED_FIELDS = ["order_id", "customer_id", "event_type", "timestamp", "warehouse_zone"]

MALFORMED_EVENTS_SCHEMA = {
    "fields": [
        {"name": "raw_payload", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "source_subscription", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

ORDERS_PER_MINUTE_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "warehouse_zone", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "pipeline_processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseAndValidate(beam.DoFn):
    """Parses raw Pub/Sub bytes into an event dict, validating that it has
    everything the rest of the pipeline needs. Well-formed events go to the
    main output; anything that fails parsing or validation is tagged
    'malformed' with the reason, so it can be routed to BigQuery instead of
    crashing the pipeline or silently guessing a default."""

    def process(self, raw_bytes):
        raw_text = raw_bytes.decode("utf-8", errors="replace")

        try:
            event = json.loads(raw_text)
        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput(
                "malformed", self._error_record(raw_text, f"invalid_json: {e}")
            )
            return

        missing = [f for f in REQUIRED_FIELDS if f not in event]
        if missing:
            yield beam.pvalue.TaggedOutput(
                "malformed",
                self._error_record(raw_text, f"missing_fields: {missing}"),
            )
            return

        try:
            datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            yield beam.pvalue.TaggedOutput(
                "malformed", self._error_record(raw_text, f"invalid_timestamp: {e}")
            )
            return

        yield event

    def _error_record(self, raw_text, reason):
        return {
            "raw_payload": raw_text,
            "error_reason": reason,
            "source_subscription": SUBSCRIPTION,
            "processing_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


def attach_event_time(event: dict):
    event_dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    event_dt = event_dt.replace(tzinfo=timezone.utc)
    return TimestampedValue(event, event_dt.timestamp())


def key_by_zone(event: dict):
    return (event["warehouse_zone"], event)


class FormatWindowedCount(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        zone, count = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "warehouse_zone": zone,
            "order_count": count,
            "pipeline_processed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        parsed = (
            pipeline
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidate()).with_outputs(
                "malformed", main="parsed"
            )
        )

        (
            parsed.parsed
            | "AttachEventTime" >> beam.Map(attach_event_time)
            | "WindowInto1Min" >> beam.WindowInto(FixedWindows(60))
            | "KeyByZone" >> beam.Map(key_by_zone)
            | "CountPerZone" >> beam.combiners.Count.PerKey()
            | "FormatOutput" >> beam.ParDo(FormatWindowedCount())
            | "WriteOrdersPerMinuteToBigQuery" >> beam.io.WriteToBigQuery(
                ORDERS_PER_MINUTE_TABLE,
                schema=ORDERS_PER_MINUTE_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        (
            parsed.malformed
            | "WriteMalformedToBigQuery" >> beam.io.WriteToBigQuery(
                MALFORMED_EVENTS_TABLE,
                schema=MALFORMED_EVENTS_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
