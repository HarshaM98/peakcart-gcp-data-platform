"""
PeakCart Project 4: Event Simulator

Publishes realistic order, delivery, and inventory events to Pub/Sub,
deliberately including duplicates, out-of-order timestamps, and missing
optional fields, so the downstream Beam pipeline has real messiness to
handle, not just clean textbook data.

Order events model one continuous lifecycle per order_id (placed -> picked
-> packed -> shipped -> delivered, via OrderLifecycle) rather than
unrelated one-off events, so downstream metrics that pair two stages of
the same order (e.g. avg_pick_time) can find real pairs in live data.
Delivery and inventory events remain independent one-off events per call.
"""

import argparse
import asyncio
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

PROJECT_ID = "harsha-data-platform"
TOPIC_ORDER = "peakcart-order-events"
TOPIC_DELIVERY = "peakcart-delivery-events"
TOPIC_INVENTORY = "peakcart-inventory-events"

WAREHOUSE_ZONES = ["zone_a", "zone_b", "zone_c"]
ORDER_STAGES = ["placed", "picked", "packed", "shipped", "delivered"]
DELIVERY_STAGES = ["departed", "in_transit", "arrived", "completed"]
INVENTORY_STAGES = ["received", "picked", "adjusted"]

publisher = pubsub_v1.PublisherClient()


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stale_iso(max_minutes_ago=5):
    """Returns a timestamp claiming an event happened a few minutes in
    the past, simulating a message that arrived late or out of order."""
    delta = timedelta(minutes=random.uniform(1, max_minutes_ago))
    return (datetime.now(timezone.utc) - delta).strftime("%Y-%m-%dT%H:%M:%SZ")


def maybe_drop_field(event: dict, optional_fields: list, drop_rate: float):
    """Randomly removes one optional field to simulate incomplete source data."""
    if random.random() < drop_rate and optional_fields:
        field_to_drop = random.choice(optional_fields)
        event.pop(field_to_drop, None)
    return event


# Wall-clock delay range (seconds) before an in-flight order advances to
# the next lifecycle stage. Compressed well below realistic warehouse
# timings so a short simulator run produces several complete lifecycles --
# these are for testability, not a claim about real pick/pack/ship times.
ORDER_STAGE_DELAYS_SECONDS = {
    "placed": (5, 20),   # -> picked
    "picked": (5, 20),   # -> packed
    "packed": (10, 30),  # -> shipped
    "shipped": (15, 40),  # -> delivered
}


class OrderLifecycle:
    """Tracks in-flight orders so make_order_event can advance a real order
    through placed -> picked -> packed -> shipped -> delivered over time,
    instead of generating unrelated one-off events for a fresh random
    order_id on every call. Downstream metrics that pair two stages of the
    same order (e.g. avg_pick_time: time between placed and picked) need
    this continuity to find real pairs in live data.

    warehouse_zone and items_count are fixed at "placed" time and carried
    through every later event for that order -- an order's physical zone
    and item count don't change mid-fulfillment. maybe_drop_field can still
    independently omit either field from any single event, preserving the
    existing per-message data-quality messiness."""

    def __init__(self):
        self._orders = {}  # order_id -> stage_index, fixed attrs, ready_at

    def start_new_order(self) -> dict:
        order_id = f"O{random.randint(10000, 99999)}"
        self._orders[order_id] = {
            "stage_index": 0,
            "customer_id": f"C{random.randint(1000, 9999)}",
            "warehouse_zone": random.choice(WAREHOUSE_ZONES),
            "items_count": random.randint(1, 12),
            "ready_at": time.time() + random.uniform(*ORDER_STAGE_DELAYS_SECONDS[ORDER_STAGES[0]]),
        }
        return self._event_for(order_id)

    def next_ready_order_id(self):
        """Returns an in-flight order_id whose next-stage delay has
        elapsed, or None if every in-flight order is still waiting."""
        now = time.time()
        for order_id, order in self._orders.items():
            if now >= order["ready_at"]:
                return order_id
        return None

    def advance(self, order_id: str) -> dict:
        order = self._orders[order_id]
        order["stage_index"] += 1
        stage = ORDER_STAGES[order["stage_index"]]
        event = self._event_for(order_id)
        if stage == ORDER_STAGES[-1]:
            del self._orders[order_id]  # delivered: lifecycle complete
        else:
            order["ready_at"] = time.time() + random.uniform(*ORDER_STAGE_DELAYS_SECONDS[stage])
        return event

    def _event_for(self, order_id: str) -> dict:
        order = self._orders[order_id]
        return {
            "order_id": order_id,
            "customer_id": order["customer_id"],
            "event_type": ORDER_STAGES[order["stage_index"]],
            "warehouse_zone": order["warehouse_zone"],
            "items_count": order["items_count"],
        }


def make_order_event(tracker: "OrderLifecycle", messiness: float) -> dict:
    ready_order_id = tracker.next_ready_order_id()
    event = tracker.advance(ready_order_id) if ready_order_id else tracker.start_new_order()
    event["timestamp"] = stale_iso() if random.random() < messiness else now_iso()
    return maybe_drop_field(event, ["warehouse_zone", "items_count"], messiness)


def make_delivery_event(messiness: float) -> dict:
    event = {
        "delivery_id": f"D{random.randint(1000, 9999)}",
        "driver_id": f"DR{random.randint(100, 199)}",
        "truck_id": f"T{random.randint(1, 20):02d}",
        "event_type": random.choice(DELIVERY_STAGES),
        "lat": round(random.uniform(40.5, 40.9), 4),
        "lng": round(random.uniform(-74.3, -73.7), 4),
        "timestamp": stale_iso() if random.random() < messiness else now_iso(),
        "orders_on_truck": random.randint(1, 30),
    }
    return maybe_drop_field(event, ["lat", "lng", "orders_on_truck"], messiness)


def make_inventory_event(messiness: float) -> dict:
    event = {
        "warehouse_id": f"WH_{random.choice(['NEWYORK', 'BOSTON', 'PHILLY'])}",
        "product_id": f"P{random.randint(1000, 1999)}",
        "event_type": random.choice(INVENTORY_STAGES),
        "quantity_change": random.randint(-50, 200),
        "timestamp": stale_iso() if random.random() < messiness else now_iso(),
    }
    return event  # no optional fields to drop, all fields required per schema


def publish(topic_name: str, event: dict):
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future


async def run_producer(name: str, topic: str, generator_fn, messiness: float,
                        base_interval: float, end_time: float):
    """Publishes events on a variable interval until end_time (unix timestamp).
    Occasionally republishes the exact same event twice in a row, simulating
    an at-least-once duplicate delivery from an upstream system."""
    count = 0
    last_event = None
    while time.time() < end_time:
        event = generator_fn(messiness)

        publish(topic, event)
        count += 1

        if last_event and random.random() < messiness * 0.5:
            publish(topic, last_event)
            count += 1

        last_event = event

        hour = datetime.now().hour
        peak = 11 <= hour <= 13 or 17 <= hour <= 19
        interval = base_interval * (0.3 if peak else 1.0)
        await asyncio.sleep(random.uniform(interval * 0.5, interval * 1.5))

    print(f"[{name}] finished, published {count} messages")


async def main(duration_minutes: float, messiness: float):
    end_time = time.time() + duration_minutes * 60
    print(f"Running for {duration_minutes} minutes with messiness={messiness}")

    order_lifecycle = OrderLifecycle()

    await asyncio.gather(
        run_producer("order", TOPIC_ORDER,
                     lambda m: make_order_event(order_lifecycle, m),
                     messiness, 1.0, end_time),
        run_producer("delivery", TOPIC_DELIVERY, make_delivery_event, messiness, 3.0, end_time),
        run_producer("inventory", TOPIC_INVENTORY, make_inventory_event, messiness, 2.0, end_time),
    )

    print("Simulator finished cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PeakCart Project 4 event simulator")
    parser.add_argument("--duration", type=float, default=2.0,
                         help="How long to run, in minutes")
    parser.add_argument("--messiness", type=float, default=0.05,
                         help="Probability (0-1) of duplicates, stale timestamps, missing fields")
    args = parser.parse_args()

    asyncio.run(main(args.duration, args.messiness))
