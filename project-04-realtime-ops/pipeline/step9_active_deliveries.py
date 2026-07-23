"""
Adds active_deliveries aggregation, building on step8. Order and inventory
branches are unchanged (order: dedup + late data; inventory: dedup + windowed
sum into inventory_velocity).

active_deliveries: count of deliveries, among those with at least one event
in this 1-minute window, whose most recent event_type is not "completed" --
i.e. still departed/in_transit/arrived. This is an approximation of "how
many deliveries are currently in flight": a delivery that emitted its last
event several windows ago (and hasn't completed) won't be counted here,
since this only looks within a single window. A true point-in-time count
would need cross-window state (e.g. a stateful DoFn tracking every
delivery_id's last-known status indefinitely), which is a different,
heavier mechanism than this step's windowed-combine pattern -- deferred
unless the windowed approximation proves insufficient.

Unlike the order/inventory branches, this doesn't need a separate dedup
step: taking the *latest* event per delivery_id (by timestamp) already
absorbs exact duplicates for free, since a duplicate shares its original's
timestamp and either copy is equally correct to keep.

Count.Globally() needs .without_defaults() here: its default behavior is
to emit a 0 for an empty input, but that default is only defined for the
GlobalWindow -- with FixedWindows, Beam raises a ValueError at pipeline
construction unless told there's no meaningful default for a window with
no elements. Practical effect: a window where every delivery's latest
status happens to be "completed" (i.e. zero active deliveries) produces
no row at all for that window, the same omit-rather-than-zero behavior
orders_per_minute already has for zones with no orders in a window.
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
INVENTORY_VELOCITY_TABLE = f"{PROJECT_ID}:peakcart_streaming.inventory_velocity"
ACTIVE_DELIVERIES_TABLE = f"{PROJECT_ID}:peakcart_streaming.active_deliveries"

ORDER_REQUIRED_FIELDS = ["order_id", "customer_id", "event_type", "timestamp", "warehouse_zone"]
DELIVERY_REQUIRED_FIELDS = ["delivery_id", "driver_id", "truck_id", "event_type", "timestamp"]
INVENTORY_REQUIRED_FIELDS = ["warehouse_id", "product_id", "event_type", "quantity_change", "timestamp"]

COMPLETED_DELIVERY_STATUS = "completed"

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

INVENTORY_VELOCITY_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "warehouse_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "net_quantity_change", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "pipeline_processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

ACTIVE_DELIVERIES_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "active_delivery_count", "type": "INTEGER", "mode": "REQUIRED"},
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


def order_dedup_key(event: dict):
    """Idempotency key: order_id + event_type + timestamp identifies one
    specific event occurrence. order_id alone is wrong here, since one
    order has many distinct events (placed, picked, packed...) that must
    NOT be collapsed together."""
    return (f"{event['order_id']}|{event['event_type']}|{event['timestamp']}", event)


def inventory_dedup_key(event: dict):
    """Idempotency key mirroring order_dedup_key: warehouse_id + product_id
    + event_type + timestamp identifies one specific inventory event
    occurrence. Needed because a plain sum (unlike a print) silently
    double-counts an undetected duplicate."""
    key = (
        f"{event['warehouse_id']}|{event['product_id']}|"
        f"{event['event_type']}|{event['timestamp']}"
    )
    return (key, event)


def take_first(kv):
    """GroupByKey produces (key, iterable_of_events). Since all events
    sharing a key are exact duplicates (by our idempotency key), take
    just one to represent the group."""
    key, events = kv
    return next(iter(events))


def key_by_zone(event: dict):
    return (event["warehouse_zone"], event)


def key_by_warehouse_and_product(event: dict):
    return ((event["warehouse_id"], event["product_id"]), event["quantity_change"])


def key_by_delivery_id(event: dict):
    return (event["delivery_id"], event)


def take_latest_by_timestamp(kv):
    """GroupByKey produces (delivery_id, iterable_of_events) -- every event
    this delivery emitted in the current window, which may be several
    distinct lifecycle stages (departed, in_transit, arrived...), not just
    duplicates. Returns the one with the latest timestamp, since that's
    the delivery's current status. ISO 8601 UTC strings ('%Y-%m-%dT%H:%M:%SZ')
    sort correctly with plain string comparison, so max() by timestamp
    works without parsing back to datetime."""
    delivery_id, events = kv
    return max(events, key=lambda e: e["timestamp"])


def is_not_completed(event: dict) -> bool:
    return event["event_type"] != COMPLETED_DELIVERY_STATUS


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


class FormatInventoryVelocity(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        (warehouse_id, product_id), net_quantity_change = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "warehouse_id": warehouse_id,
            "product_id": product_id,
            "net_quantity_change": net_quantity_change,
            "pipeline_processed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }


class FormatActiveDeliveries(beam.DoFn):
    def process(self, active_delivery_count, window=beam.DoFn.WindowParam):
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "active_delivery_count": active_delivery_count,
            "pipeline_processed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        # --- Order events: unchanged from step8 (dedup + late data) ---
        order_parsed = (
            pipeline
            | "ReadOrders" >> beam.io.ReadFromPubSub(subscription=ORDER_SUBSCRIPTION)
            | "ValidateOrders" >> beam.ParDo(
                ParseAndValidate(ORDER_REQUIRED_FIELDS, ORDER_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        (
            order_parsed.parsed
            | "AttachOrderEventTime" >> beam.Map(attach_event_time)
            | "OrderWindowWithLateness" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
            )
            | "KeyForOrderDedup" >> beam.Map(order_dedup_key)
            | "GroupByOrderIdempotencyKey" >> beam.GroupByKey()
            | "TakeFirstOrderPerKey" >> beam.Map(take_first)
            | "KeyByZone" >> beam.Map(key_by_zone)
            | "CountPerZone" >> beam.combiners.Count.PerKey()
            | "FormatOrderOutput" >> beam.ParDo(FormatWindowedCount())
            | "WriteOrdersPerMinuteToBigQuery" >> beam.io.WriteToBigQuery(
                ORDERS_PER_MINUTE_TABLE,
                schema=ORDERS_PER_MINUTE_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        # --- Delivery events: now aggregated into active_deliveries ---
        delivery_parsed = (
            pipeline
            | "ReadDelivery" >> beam.io.ReadFromPubSub(subscription=DELIVERY_SUBSCRIPTION)
            | "ValidateDelivery" >> beam.ParDo(
                ParseAndValidate(DELIVERY_REQUIRED_FIELDS, DELIVERY_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        (
            delivery_parsed.parsed
            | "AttachDeliveryEventTime" >> beam.Map(attach_event_time)
            | "DeliveryWindowWithLateness" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
            )
            | "KeyByDeliveryId" >> beam.Map(key_by_delivery_id)
            | "GroupByDeliveryId" >> beam.GroupByKey()
            | "TakeLatestPerDelivery" >> beam.Map(take_latest_by_timestamp)
            | "FilterNotCompleted" >> beam.Filter(is_not_completed)
            | "CountActiveDeliveries" >> beam.combiners.Count.Globally().without_defaults()
            | "FormatActiveDeliveries" >> beam.ParDo(FormatActiveDeliveries())
            | "WriteActiveDeliveriesToBigQuery" >> beam.io.WriteToBigQuery(
                ACTIVE_DELIVERIES_TABLE,
                schema=ACTIVE_DELIVERIES_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        # --- Inventory events: unchanged from step8 (dedup + windowed sum) ---
        inventory_parsed = (
            pipeline
            | "ReadInventory" >> beam.io.ReadFromPubSub(subscription=INVENTORY_SUBSCRIPTION)
            | "ValidateInventory" >> beam.ParDo(
                ParseAndValidate(INVENTORY_REQUIRED_FIELDS, INVENTORY_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        (
            inventory_parsed.parsed
            | "AttachInventoryEventTime" >> beam.Map(attach_event_time)
            | "InventoryWindowWithLateness" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
            )
            | "KeyForInventoryDedup" >> beam.Map(inventory_dedup_key)
            | "GroupByInventoryIdempotencyKey" >> beam.GroupByKey()
            | "TakeFirstInventoryPerKey" >> beam.Map(take_first)
            | "KeyByWarehouseAndProduct" >> beam.Map(key_by_warehouse_and_product)
            | "SumQuantityChangePerKey" >> beam.CombinePerKey(sum)
            | "FormatInventoryVelocity" >> beam.ParDo(FormatInventoryVelocity())
            | "WriteInventoryVelocityToBigQuery" >> beam.io.WriteToBigQuery(
                INVENTORY_VELOCITY_TABLE,
                schema=INVENTORY_VELOCITY_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
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
