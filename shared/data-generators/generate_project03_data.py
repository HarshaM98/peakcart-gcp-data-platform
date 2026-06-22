"""
PeakCart Project 03 - Customer 360 Data Generator
Generates four source datasets with realistic patterns,
referential integrity, and deliberate dirty data.

Output: shared/data-generators/output/project-03/
"""

import csv
import random
import uuid
import os
from datetime import datetime, timedelta, date

# ── Reproducibility ───────────────────────────────────────────────────────────
# Same seed = same data every time. Critical for debugging.
# If you change the seed, all downstream data changes too.
random.seed(42)

# ── Output directory ──────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(
    os.path.dirname(__file__),
    "output",
    "project-03"
)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Shared spine ──────────────────────────────────────────────────────────────
# Generate these ONCE. Every other generator references these lists.
# This is what prevents referential integrity failures across files.

NUM_CUSTOMERS = 1000
NUM_PRODUCTS  = 200
NUM_ORDERS    = 5000

CUSTOMER_IDS = [f"C{str(i).zfill(4)}" for i in range(1, NUM_CUSTOMERS + 1)]
PRODUCT_IDS  = [f"P{str(i).zfill(3)}"  for i in range(1, NUM_PRODUCTS  + 1)]

# ── Reference data ────────────────────────────────────────────────────────────
CITIES_STATES = [
    ("New York",     "NY"),
    ("Los Angeles",  "CA"),
    ("Chicago",      "IL"),
    ("Houston",      "TX"),
    ("Phoenix",      "AZ"),
    ("Philadelphia", "PA"),
    ("San Antonio",  "TX"),
    ("San Diego",    "CA"),
    ("Dallas",       "TX"),
    ("San Jose",     "CA"),
]

DELIVERY_WINDOWS    = ["morning", "afternoon", "evening"]
DIETARY_PREFERENCES = ["none", "none", "none", "vegetarian", "vegan", "gluten-free"]
# "none" appears 3x so it is picked ~50% of the time (realistic distribution)

ORDER_STATUSES = ["delivered", "delivered", "delivered", "cancelled", "in_transit"]
# delivered ~60%, cancelled ~20%, in_transit ~20%

EVENT_TYPES = [
    "page_view", "page_view", "page_view",
    "product_view", "product_view",
    "add_to_cart",
    "search",
    "purchase",
    "remove_from_cart",
]
# page_view is most common, purchase is rare (realistic funnel)

DEVICE_TYPES  = ["mobile", "mobile", "desktop", "tablet"]
# Mobile majority (realistic for grocery delivery app)

ISSUE_TYPES   = ["none", "none", "none", "none", "late", "damaged",
                 "wrong_items", "missing_items"]
# 50% no issue, 50% split across issue types


# ── Helper functions ──────────────────────────────────────────────────────────

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_timestamp_with_pattern(base_date: date) -> datetime:
    """
    Generate a timestamp with realistic time-of-day patterns.
    Peak traffic: 11 AM to 1 PM and 6 PM to 9 PM.
    Low traffic:  2 AM to 7 AM.
    This tests sessionization logic properly.
    """
    hour_weights = [
        1,  1,  1,  1,  1,  1,   # 00-05  (very low, overnight)
        2,  3,  5,  7,  9, 10,   # 06-11  (morning ramp up)
       10, 10,  8,  7,  6,  5,   # 12-17  (lunch peak, afternoon)
        9, 10, 10,  8,  5,  3,   # 18-23  (dinner peak, evening wind down)
    ]
    hour    = random.choices(range(24), weights=hour_weights, k=1)[0]
    minute  = random.randint(0, 59)
    second  = random.randint(0, 59)
    return datetime(
        base_date.year, base_date.month, base_date.day,
        hour, minute, second
    )


def write_csv(filename: str, rows: list[dict], fieldnames: list[str]) -> None:
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written: {filename}  ({len(rows):,} rows)")


# ── Generator 1: customer_profiles ───────────────────────────────────────────

def generate_customer_profiles() -> list[dict]:
    print("\nGenerating customer_profiles...")
    rows = []

    signup_start = date(2024, 1, 1)
    signup_end   = date(2025, 6, 1)

    first_names = [
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
        "Michael", "Linda", "William", "Barbara", "David", "Susan",
        "Harsha", "Priya", "Arjun", "Divya", "Rahul", "Anjali",
        "Wei",   "Mei",   "Carlos", "Maria",  "Ahmed", "Fatima",
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
        "Miller", "Davis", "Martinez", "Wilson", "Patel", "Kumar",
        "Chen",  "Wang",   "Lopez",    "Lee",   "Kim",   "Nguyen",
    ]

    for customer_id in CUSTOMER_IDS:
        city, state = random.choice(CITIES_STATES)

        # 2% null emails (deliberate dirty data, same as Project 1)
        email = (
            None if random.random() < 0.02
            else f"{customer_id.lower()}@example.com"
        )

        rows.append({
            "customer_id":               customer_id,
            "full_name":                 (f"{random.choice(first_names)} "
                                          f"{random.choice(last_names)}"),
            "email":                     email,
            "city":                      city,
            "state":                     state,
            "signup_date":               random_date(signup_start, signup_end),
            "preferred_delivery_window": random.choice(DELIVERY_WINDOWS),
            "dietary_preferences":       random.choice(DIETARY_PREFERENCES),
            "is_active":                 random.random() >= 0.05,  # 95% active
        })

    return rows


# ── Generator 2: order_history ────────────────────────────────────────────────

def generate_order_history() -> tuple[list[dict], dict]:
    """
    Returns:
        rows:          list of order item rows for the CSV
        order_meta:    dict of order_id -> {customer_id, order_date,
                       delivery_date, order_status} for use by
                       delivery_feedback generator (avoids duplication)
    """
    print("Generating order_history...")
    rows       = []
    order_meta = {}

    order_start = date(2025, 1, 1)
    order_end   = date(2025, 12, 31)

    # Give some customers many orders, most customers few orders.
    # This creates the realistic long-tail distribution needed for RFM.
    # Top 5% of customers get 10 to 20 orders. Bottom 50% get 1 to 3.
    customer_order_counts = {}
    for cid in CUSTOMER_IDS:
        r = random.random()
        if r < 0.05:
            customer_order_counts[cid] = random.randint(10, 20)  # high value
        elif r < 0.30:
            customer_order_counts[cid] = random.randint(4,  9)   # regular
        else:
            customer_order_counts[cid] = random.randint(1,  3)   # occasional

    order_num = 1

    for customer_id, order_count in customer_order_counts.items():
        for _ in range(order_count):
            order_id     = f"ORD{str(order_num).zfill(5)}"
            order_date   = random_date(order_start, order_end)
            delivery_date = order_date + timedelta(days=random.randint(1, 5))
            order_status = random.choice(ORDER_STATUSES)

            order_meta[order_id] = {
                "customer_id":   customer_id,
                "order_date":    order_date,
                "delivery_date": delivery_date,
                "order_status":  order_status,
            }

            # 1 to 5 items per order
            num_items = random.randint(1, 5)
            for item_num in range(1, num_items + 1):
                product_id = random.choice(PRODUCT_IDS)
                unit_price = round(random.uniform(1.99, 49.99), 2)
                discount   = round(random.choice(
                    [0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20, 0.30]
                ), 2)

                # 0.5% negative quantity (returns), same as Project 1
                quantity = (
                    random.randint(-3, -1) if random.random() < 0.005
                    else random.randint(1, 10)
                )

                # 20% of delivered orders have no rating
                delivery_rating = (
                    None
                    if order_status != "delivered" or random.random() < 0.20
                    else random.randint(1, 5)
                )

                rows.append({
                    "order_item_id":   f"{order_id}_ITEM{item_num}",
                    "order_id":        order_id,
                    "customer_id":     customer_id,
                    "product_id":      product_id,
                    "order_date":      order_date,
                    "delivery_date":   delivery_date,
                    "order_status":    order_status,
                    "quantity":        quantity,
                    "unit_price":      unit_price,
                    "discount":        discount,
                    "delivery_rating": delivery_rating,
                })

            order_num += 1

    return rows, order_meta


# ── Generator 3: clickstream_events ──────────────────────────────────────────

def generate_clickstream_events() -> list[dict]:
    print("Generating clickstream_events...")
    rows = []

    event_start = date(2025, 1, 1)
    event_end   = date(2025, 12, 31)

    pages = ["/home", "/search", "/cart", "/checkout", "/account"]
    category_pages = [f"/category/cat_{i}" for i in range(1, 11)]
    product_pages  = [f"/product/{pid}" for pid in PRODUCT_IDS]

    all_pages = pages + category_pages + product_pages

    search_queries = [
        "organic milk", "whole wheat bread", "chicken breast",
        "fresh salmon", "greek yogurt", "baby spinach",
        "orange juice", "almond butter", "brown rice", "frozen pizza",
    ]

    # Generate sessions per customer
    # Each session is a burst of events within a short time window
    for customer_id in CUSTOMER_IDS:

        # 5% anonymous sessions (no customer_id)
        effective_customer_id = (
            None if random.random() < 0.05 else customer_id
        )

        # Each customer has 5 to 30 sessions across the year
        num_sessions = random.randint(5, 30)

        for _ in range(num_sessions):
            session_id       = str(uuid.uuid4())
            session_date     = random_date(event_start, event_end)
            session_start_ts = random_timestamp_with_pattern(session_date)

            # 5 to 25 events per session
            num_events   = random.randint(5, 25)
            current_ts   = session_start_ts
            cart_has_item = False

            for event_num in range(num_events):
                event_type = random.choice(EVENT_TYPES)

                # Product ID only relevant for product interactions
                product_id = (
                    random.choice(PRODUCT_IDS)
                    if event_type in ("product_view", "add_to_cart",
                                      "purchase", "remove_from_cart")
                    else None
                )

                # Page URL follows event type logically
                if event_type == "page_view":
                    page_url = random.choice(all_pages)
                elif event_type in ("product_view", "add_to_cart",
                                    "purchase", "remove_from_cart"):
                    page_url = f"/product/{product_id}" if product_id else "/cart"
                elif event_type == "search":
                    page_url = "/search"
                else:
                    page_url = "/home"

                search_query = (
                    random.choice(search_queries)
                    if event_type == "search" else None
                )

                if event_type == "add_to_cart":
                    cart_has_item = True
                if event_type == "remove_from_cart":
                    cart_has_item = False

                rows.append({
                    "event_id":        str(uuid.uuid4()),
                    "customer_id":     effective_customer_id,
                    "session_id":      session_id,
                    "event_type":      event_type,
                    "product_id":      product_id,
                    "page_url":        page_url,
                    "search_query":    search_query,
                    "event_timestamp": current_ts.strftime(
                                           "%Y-%m-%dT%H:%M:%SZ"),
                    "device_type":     random.choice(DEVICE_TYPES),
                })

                # Advance timestamp by 10 seconds to 3 minutes per event
                # Occasionally inject a 35-minute gap to create a new
                # session boundary (tests sessionization logic)
                if random.random() < 0.03:   # 3% chance of long gap
                    current_ts += timedelta(minutes=random.randint(35, 90))
                else:
                    current_ts += timedelta(seconds=random.randint(10, 180))

    return rows


# ── Generator 4: delivery_feedback ───────────────────────────────────────────

def generate_delivery_feedback(order_meta: dict) -> list[dict]:
    """
    Uses order_meta from generate_order_history() to ensure every
    feedback row references a real, delivered order.
    """
    print("Generating delivery_feedback...")
    rows = []

    feedback_num = 1

    for order_id, meta in order_meta.items():
        # Only delivered orders can have feedback
        if meta["order_status"] != "delivered":
            continue

        # 70% of delivered orders get feedback
        if random.random() > 0.70:
            continue

        driver_rating   = random.randint(1, 5)
        delivery_rating = random.randint(1, 5)
        overall         = (driver_rating + delivery_rating) / 2

        issue_type = (
            random.choice(ISSUE_TYPES)
            if delivery_rating <= 3
            else "none"
        )

        comments = [
            "Great service!", "On time and friendly.",
            "Package was a bit damaged.", "Wrong items in my order.",
            "Driver was very helpful.", "Delivery was late.",
            "Everything was perfect.", "Missing a few items.",
            None, None, None,  # 40% null comments (realistic)
        ]

        rows.append({
            "feedback_id":      f"FB{str(feedback_num).zfill(5)}",
            "order_id":         order_id,
            "customer_id":      meta["customer_id"],
            "delivery_date":    meta["delivery_date"],
            "driver_rating":    driver_rating,
            "delivery_rating":  delivery_rating,
            "comment":          random.choice(comments),
            "issue_type":       issue_type,
            "would_recommend":  overall >= 4.0,
        })

        feedback_num += 1

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PeakCart Project 03 Data Generator")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 60)

    # Step 1: Generate customer profiles
    profiles = generate_customer_profiles()
    write_csv(
        "customer_profiles.csv",
        profiles,
        ["customer_id", "full_name", "email", "city", "state",
         "signup_date", "preferred_delivery_window",
         "dietary_preferences", "is_active"]
    )

    # Step 2: Generate order history
    # order_meta is passed to delivery_feedback to ensure referential integrity
    orders, order_meta = generate_order_history()
    write_csv(
        "order_history.csv",
        orders,
        ["order_item_id", "order_id", "customer_id", "product_id",
         "order_date", "delivery_date", "order_status",
         "quantity", "unit_price", "discount", "delivery_rating"]
    )

    # Step 3: Generate clickstream (largest file, will take a moment)
    events = generate_clickstream_events()
    write_csv(
        "clickstream_events.csv",
        events,
        ["event_id", "customer_id", "session_id", "event_type",
         "product_id", "page_url", "search_query",
         "event_timestamp", "device_type"]
    )

    # Step 4: Generate delivery feedback using order_meta
    feedback = generate_delivery_feedback(order_meta)
    write_csv(
        "delivery_feedback.csv",
        feedback,
        ["feedback_id", "order_id", "customer_id", "delivery_date",
         "driver_rating", "delivery_rating", "comment",
         "issue_type", "would_recommend"]
    )

    print("\n" + "=" * 60)
    print("Generation complete. Next steps:")
    print("  1. Inspect each CSV in VS Code before loading")
    print("  2. Run upload_to_gcs.sh to push files to GCS")
    print("  3. Run load_bronze.sh to load Bronze tables")
    print("=" * 60)


if __name__ == "__main__":
    main()