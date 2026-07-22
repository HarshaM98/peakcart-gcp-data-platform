"""
Adds deduplication and late data handling to the orders branch, building
on step6. Delivery and inventory branches are unchanged (still print-only).

Deduplication: the simulator republishes the exact same event dict as a
duplicate (see event_simulator.py's run_producer -- it resends last_event
unchanged). A dedup key of order_id alone would be wrong, since one order
legitimately has multiple different events (placed, picked, packed...)
sharing the same order_id. The correct idempotency key is the combination
that identifies one specific event occurrence: order_id + event_type +
timestamp. beam.Distinct() groups by this key and keeps one copy per key,
within the current window.

Late data handling: the simulator's stale_iso() deliberately assigns event
timestamps a few minutes in the past (simulating late/out-of-order
delivery). Since Beam windows by event time, a stale event can arrive
after its window would have already closed and fired. allowed_lateness
keeps the window open for extra time to accept late data; ACCUMULATING
mode means a late arrival re-fires the window with an updated cumulative
count instead of being dropped or producing a confusing duplicate row.
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark, AfterCount
from apache_beam.transforms.window import FixedWindows, TimestampedValue

PROJECT_ID = "harsha-data-platform"

ORDER_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-order-events-sub"
DELIVERY_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-delivery-events-sub"
INVENTORY_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-inventory-events-sub"

MALFORMED_EVENTS_TABLE = f"{PROJECT_ID}:peakcart_streaming.malformed_events"
ORDERS_PER_MINUTE_TABLE = f"{PROJECT_ID}:peakcart_streaming.orders_per_minute"

ORDER_REQUIRED_FIELDS = ["order_id", "customer_id", "event_type", "timestamp", "warehouse_zone"]
DELIVERY_REQUIRED_FIELDS = ["delivery_id", "driver_id", "truck_id", "event_type", "timestamp"]
INVENTORY_REQUIRED_FIELDS = ["warehouse_id", "product_id", "event_type", "quantity_change", "timestamp"]

# How long to keep a window open for late-arriving data after it would
# normally close. The simulator's stale_iso() goes up to 5 minutes back,
# so 5 minutes of allowed lateness covers the worst case this test data
# produces.
ALLOWED_LATENESS_SECONDS = 5 * 60

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
    """Parses raw Pub/Sub bytes into an event dict and validates it against
    a caller-supplied list of required fields. Shared logic across all
    three event types -- only required fields and subscription name
    (for error attribution) differ per topic."""

    def __init__(self, required_fields, subscription_name):
        self.required_fields = required_fields
        self.subscription_name = subscription_name

    def process(self, raw_bytes):
        raw_text = raw_bytes.decode("utf-8", errors="replace")

        try:
            event = json.loads(raw_text)
        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput(
                "malformed", self._error_record(raw_text, f"invalid_json: {e}")
            )
            return

        missing = [f for f in self.required_fields if f not in event]
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
            "source_subscription": self.subscription_name,
            "processing_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


def attach_event_time(event: dict):
    event_dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    event_dt = event_dt.replace(tzinfo=timezone.utc)
    return TimestampedValue(event, event_dt.timestamp())


def dedup_key(event: dict):
    """Idempotency key: order_id + event_type + timestamp identifies one
    specific event occurrence. order_id alone is wrong here, since one
    order has many distinct events (placed, picked, packed...) that must
    NOT be collapsed together."""
    return (f"{event['order_id']}|{event['event_type']}|{event['timestamp']}", event)


def take_first(kv):
    """GroupByKey produces (key, iterable_of_events). Since all events
    sharing a key are exact duplicates (by our idempotency key), take
    just one to represent the group."""
    key, events = kv
    return next(iter(events))


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
        # --- Order events: now with dedup + late data handling ---
        order_parsed = (
            pipeline
            | "ReadOrders" >> beam.io.ReadFromPubSub(subscription=ORDER_SUBSCRIPTION)
            | "ValidateOrders" >> beam.ParDo(
                ParseAndValidate(ORDER_REQUIRED_FIELDS, ORDER_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        (
            order_parsed.parsed
            | "AttachEventTime" >> beam.Map(attach_event_time)
            | "WindowWithLateness" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
)
            | "KeyForDedup" >> beam.Map(dedup_key)
            | "GroupByIdempotencyKey" >> beam.GroupByKey()
            | "TakeFirstPerKey" >> beam.Map(take_first)
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

        # --- Delivery events: unchanged from step6, validated only ---
        delivery_parsed = (
            pipeline
            | "ReadDelivery" >> beam.io.ReadFromPubSub(subscription=DELIVERY_SUBSCRIPTION)
            | "ValidateDelivery" >> beam.ParDo(
                ParseAndValidate(DELIVERY_REQUIRED_FIELDS, DELIVERY_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        (
            delivery_parsed.parsed
            | "PrintDelivery" >> beam.Map(lambda e: print(f"[delivery] {e}"))
        )

        # --- Inventory events: unchanged from step6, validated only ---
        inventory_parsed = (
            pipeline
            | "ReadInventory" >> beam.io.ReadFromPubSub(subscription=INVENTORY_SUBSCRIPTION)
            | "ValidateInventory" >> beam.ParDo(
                ParseAndValidate(INVENTORY_REQUIRED_FIELDS, INVENTORY_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        (
            inventory_parsed.parsed
            | "PrintInventory" >> beam.Map(lambda e: print(f"[inventory] {e}"))
        )

        # --- Combine all three malformed branches into one BigQuery sink ---
        (
            (order_parsed.malformed, delivery_parsed.malformed, inventory_parsed.malformed)
            | "FlattenMalformed" >> beam.Flatten()
            | "WriteMalformedToBigQuery" >> beam.io.WriteToBigQuery(
                MALFORMED_EVENTS_TABLE,
                schema=MALFORMED_EVENTS_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()