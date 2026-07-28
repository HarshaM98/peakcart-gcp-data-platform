"""
Adds avg_pick_time aggregation, building on step9. Delivery and inventory
branches are unchanged (delivery: active_deliveries; inventory: dedup +
windowed sum). The order branch keeps its existing dedup + late-data
windowed count into orders_per_minute, and gains a second, independent
branch computing avg_pick_time from the same validated order events.

avg_pick_time: average time (seconds) between an order's "placed" and
"picked" events, across orders whose pick completed in a given 1-minute
window. This needs event pairing across time, not a simple per-key
combine like orders_per_minute/inventory_velocity/active_deliveries --
the two events being paired can be minutes apart and land in different
fixed windows, so this branch uses a different windowing strategy:

  1. Key events by order_id and window with Sessions(gap=SESSION_GAP) --
     a session window merges all of one order_id's events as long as
     consecutive events are within gap seconds of each other, so one
     order's whole lifecycle (placed..delivered) collapses into a single
     window regardless of how long the lifecycle takes in wall-clock time.
  2. GroupByKey per order_id within its session, pick out that order's
     "placed" and "picked" events (if both are present), and compute the
     gap between them in seconds.
  3. Re-window that single scalar (assigned the "picked" event's real
     timestamp) into the same FixedWindows(60) used elsewhere, so
     avg_pick_time reports on the same 1-minute cadence as the other
     three metrics, then average + count via a single custom CombineFn.

This relies on event_simulator.py's OrderLifecycle (added alongside this
step) actually advancing one order_id through its stages over time --
previously each simulator call generated an unrelated one-off order_id,
so placed/picked pairs for the same real order essentially never existed
in live data.

The session window in step 1 deliberately uses Beam's default trigger
(a single AfterWatermark firing, allowed_lateness=0) rather than the
late-data-tolerant trigger used elsewhere in this pipeline. Adding a late
trigger with ACCUMULATING mode here would let one order's session refire
more than once (e.g. on a late/duplicate event), and each refire would
re-extract and re-emit the SAME placed/picked pair from the accumulated
group -- double-counting that order in the downstream average. The
count-based branches (orders_per_minute, active_deliveries,
inventory_velocity) don't have this problem because their post-GroupByKey
step (take_first/take_latest/sum) recomputes idempotently from the full
group every time; pairing two specific events is not naturally idempotent
under repeated extraction the same way. Trade-off: a single-fire session
window means this branch may produce output less often under DirectRunner's
slow/unpredictable watermark than the other three metrics (documented
since step4/step5's live tests) -- correctness over live-test convenience.
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark, AfterCount
from apache_beam.transforms.window import FixedWindows, Sessions, TimestampedValue

PROJECT_ID = "harsha-data-platform"

ORDER_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-order-events-sub"
DELIVERY_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-delivery-events-sub"
INVENTORY_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/peakcart-inventory-events-sub"

MALFORMED_EVENTS_TABLE = f"{PROJECT_ID}:peakcart_streaming.malformed_events"
ORDERS_PER_MINUTE_TABLE = f"{PROJECT_ID}:peakcart_streaming.orders_per_minute"
INVENTORY_VELOCITY_TABLE = f"{PROJECT_ID}:peakcart_streaming.inventory_velocity"
ACTIVE_DELIVERIES_TABLE = f"{PROJECT_ID}:peakcart_streaming.active_deliveries"
AVG_PICK_TIME_TABLE = f"{PROJECT_ID}:peakcart_streaming.avg_pick_time"

ORDER_REQUIRED_FIELDS = ["order_id", "customer_id", "event_type", "timestamp", "warehouse_zone"]
DELIVERY_REQUIRED_FIELDS = ["delivery_id", "driver_id", "truck_id", "event_type", "timestamp"]
INVENTORY_REQUIRED_FIELDS = ["warehouse_id", "product_id", "event_type", "quantity_change", "timestamp"]

COMPLETED_DELIVERY_STATUS = "completed"

# How long to keep a window open for late-arriving data after it would
# normally close. The simulator's stale_iso() goes up to 5 minutes back,
# so 5 minutes of allowed lateness covers the worst case this test data
# produces.
ALLOWED_LATENESS_SECONDS = 5 * 60

# Session gap for pairing one order's lifecycle events. Must comfortably
# exceed the simulator's largest per-stage delay (see
# ORDER_STAGE_DELAYS_SECONDS in event_simulator.py, currently up to ~40s
# per transition) so placed and picked always merge into the same
# session even with several stages between them -- but should otherwise
# be as SMALL as possible: a session can't close (and this branch can't
# produce output) until the watermark passes the order's LAST event plus
# this gap, so an oversized value directly adds latency to every order's
# result, not just tolerance for a rare edge case. 90s gives ~2x headroom
# over the ~40s worst case while keeping that wait short. (Deliberately
# not reusing ALLOWED_LATENESS_SECONDS here even though both are "how long
# to wait" constants -- that one bounds tolerance for late-arriving data
# on an already-fixed window, this one bounds how long a window stays open
# to begin with, so conflating them cost real turnaround time in testing:
# an earlier version of this file reused a 5-minute value here by default
# and needed far longer live test runs than necessary to see output.)
SESSION_GAP_SECONDS = 90

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

AVG_PICK_TIME_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "avg_pick_time_seconds", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "sample_count", "type": "INTEGER", "mode": "REQUIRED"},
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


def key_by_order_id(event: dict):
    return (event["order_id"], event)


def _parse_ts(timestamp: str) -> datetime:
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class ExtractPickTime(beam.DoFn):
    """Takes one order_id's session-windowed events (its whole lifecycle,
    if the session gap was wide enough to hold it) and, if both a
    "placed" and a "picked" event are present, emits the seconds between
    them, timestamped at the "picked" event's real time so it can be
    re-windowed into the same FixedWindows(60) cadence as the other
    metrics.

    Orders with no "picked" yet (still in an earlier stage), or where
    "placed" fell outside this session (e.g. it happened before the
    pipeline started consuming), don't yield anything -- an incomplete
    pair contributes no pick-time sample, rather than a wrong one."""

    def process(self, kv):
        order_id, events = kv
        placed = None
        picked = None
        for event in events:
            if event["event_type"] == "placed" and placed is None:
                placed = event
            elif event["event_type"] == "picked" and picked is None:
                picked = event
        if placed is None or picked is None:
            return

        placed_dt = _parse_ts(placed["timestamp"])
        picked_dt = _parse_ts(picked["timestamp"])
        pick_time_seconds = (picked_dt - placed_dt).total_seconds()
        if pick_time_seconds < 0:
            # Messiness (stale_iso backdating one of the two events
            # independently) can occasionally make "picked" appear to
            # precede "placed". Not a real sample -- discard rather than
            # let a nonsensical negative duration skew the average.
            return

        yield TimestampedValue(pick_time_seconds, picked_dt.timestamp())


class PickTimeStatsFn(beam.CombineFn):
    """Computes both the average and the sample count in one pass, since
    BigQuery's row needs both and a single custom accumulator avoids
    running two separate combines over the same windowed PCollection."""

    def create_accumulator(self):
        return (0.0, 0)  # (sum_seconds, count)

    def add_input(self, accumulator, input_value):
        total, count = accumulator
        return (total + input_value, count + 1)

    def merge_accumulators(self, accumulators):
        totals, counts = zip(*accumulators)
        return (sum(totals), sum(counts))

    def extract_output(self, accumulator):
        total, count = accumulator
        return {
            "avg_pick_time_seconds": total / count if count else 0.0,
            "sample_count": count,
        }


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


class FormatAvgPickTime(beam.DoFn):
    def process(self, stats, window=beam.DoFn.WindowParam):
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "avg_pick_time_seconds": stats["avg_pick_time_seconds"],
            "sample_count": stats["sample_count"],
            "pipeline_processed_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        order_parsed = (
            pipeline
            | "ReadOrders" >> beam.io.ReadFromPubSub(subscription=ORDER_SUBSCRIPTION)
            | "ValidateOrders" >> beam.ParDo(
                ParseAndValidate(ORDER_REQUIRED_FIELDS, ORDER_SUBSCRIPTION)
            ).with_outputs("malformed", main="parsed")
        )

        # --- Order branch 1: unchanged from step9 (dedup + late data -> orders_per_minute) ---
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

        # --- Order branch 2: new -- avg_pick_time via session-windowed pairing ---
        (
            order_parsed.parsed
            | "AttachOrderEventTimeForPickTime" >> beam.Map(attach_event_time)
            | "KeyByOrderId" >> beam.Map(key_by_order_id)
            | "SessionWindowPerOrder" >> beam.WindowInto(Sessions(SESSION_GAP_SECONDS))
            | "GroupByOrderId" >> beam.GroupByKey()
            | "ExtractPickTime" >> beam.ParDo(ExtractPickTime())
            | "FixedWindowForAvgPickTime" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
            )
            | "PickTimeStats" >> beam.CombineGlobally(PickTimeStatsFn()).without_defaults()
            | "FormatAvgPickTime" >> beam.ParDo(FormatAvgPickTime())
            | "WriteAvgPickTimeToBigQuery" >> beam.io.WriteToBigQuery(
                AVG_PICK_TIME_TABLE,
                schema=AVG_PICK_TIME_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        # --- Delivery events: unchanged from step9 (active_deliveries) ---
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

        # --- Inventory events: unchanged from step9 (dedup + windowed sum) ---
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
