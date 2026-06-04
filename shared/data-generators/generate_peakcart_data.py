"""
PeakCart Sample Data Generator
Generates realistic CSV datasets for the PeakCart data platform.
Includes deliberate data quality issues to test pipeline robustness.
"""

import csv
import random
import os
from datetime import datetime, timedelta

# ─── Configuration ───────────────────────────────────────────────────────────

SEED = 42
random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_SUPPLIERS     = 20
NUM_PRODUCTS      = 200
NUM_CUSTOMERS     = 1000
NUM_ORDERS        = 5000
NUM_ORDER_ITEMS   = 15000
NUM_INV_SNAPSHOTS = 3000

START_DATE = datetime(2025, 1, 1)
END_DATE   = datetime(2026, 6, 1)

# ─── Helpers ─────────────────────────────────────────────────────────────────

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def random_email(name):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
    clean = name.lower().replace(" ", ".")
    return f"{clean}@{random.choice(domains)}"

def intentional_nulls(value, null_rate=0.03):
    """Introduce NULL values at a controlled rate to test quality checks."""
    return None if random.random() < null_rate else value

def intentional_duplicate(rows, dupe_rate=0.01):
    """Introduce duplicate rows at a controlled rate."""
    dupes = random.sample(rows, max(1, int(len(rows) * dupe_rate)))
    return rows + dupes

# ─── Generators ──────────────────────────────────────────────────────────────

def generate_suppliers():
    regions = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
    rows = []
    for i in range(1, NUM_SUPPLIERS + 1):
        rows.append({
            "supplier_id":    i,
            "name":           f"Supplier_{i:03d}",
            "region":         random.choice(regions),
            "lead_time_days": random.randint(1, 14),
            "is_active":      random.choice([True, True, True, False]),
        })
    return rows

def generate_products(supplier_ids):
    categories = {
        "Produce":    ["Apples", "Bananas", "Carrots", "Spinach", "Tomatoes"],
        "Dairy":      ["Milk", "Cheese", "Yogurt", "Butter", "Cream"],
        "Meat":       ["Chicken", "Beef", "Pork", "Turkey", "Lamb"],
        "Bakery":     ["Bread", "Muffins", "Bagels", "Croissants", "Rolls"],
        "Beverages":  ["Water", "Juice", "Soda", "Coffee", "Tea"],
        "Frozen":     ["Pizza", "Ice Cream", "Waffles", "Burritos", "Nuggets"],
        "Snacks":     ["Chips", "Cookies", "Granola", "Popcorn", "Crackers"],
        "Household":  ["Detergent", "Soap", "Shampoo", "Toothpaste", "Tissue"],
    }
    rows = []
    product_id = 1
    for category, items in categories.items():
        for item in items:
            for variant in range(1, 6):
                rows.append({
                    "product_id":   product_id,
                    "name":         f"{item} Variant {variant}",
                    "category":     category,
                    "subcategory":  item,
                    "price":        round(random.uniform(0.99, 49.99), 2),
                    "supplier_id":  intentional_nulls(
                                        random.choice(supplier_ids), 0.02
                                    ),
                    "is_active":    random.choice([True, True, True, False]),
                })
                product_id += 1
                if product_id > NUM_PRODUCTS:
                    return rows
    return rows

def generate_customers():
    segments   = ["Premium", "Standard", "Budget", "New"]
    cities     = ["Chicago", "New York", "Los Angeles", "Houston", "Phoenix"]
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                   "Michael", "Linda", "William", "Barbara", "Harsha", "Priya"]
    last_names  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                   "Miller", "Davis", "Wilson", "Taylor", "Kumar", "Patel"]
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        name       = f"{random.choice(first_names)} {random.choice(last_names)}"
        signup     = random_date(START_DATE, END_DATE)
        rows.append({
            "customer_id":  i,
            "name":         name,
            "email":        intentional_nulls(random_email(name), 0.02),
            "city":         random.choice(cities),
            "state":        intentional_nulls(
                                random.choice(["IL","NY","CA","TX","AZ"]), 0.01
                            ),
            "signup_date":  signup.date(),
            "segment":      random.choice(segments),
        })
    return rows

def generate_orders(customer_ids):
    statuses = ["placed", "confirmed", "packed", "shipped", "delivered",
                "cancelled", "returned"]
    rows = []
    for i in range(1, NUM_ORDERS + 1):
        order_date    = random_date(START_DATE, END_DATE)
        delivery_days = random.randint(1, 7)
        delivery_date = order_date + timedelta(days=delivery_days)

        # Deliberate quality issue: 1% of orders have delivery before order date
        if random.random() < 0.01:
            delivery_date = order_date - timedelta(days=random.randint(1, 3))

        rows.append({
            "order_id":       i,
            "customer_id":    intentional_nulls(random.choice(customer_ids), 0.01),
            "order_date":     order_date.date(),
            "delivery_date":  delivery_date.date(),
            "status":         random.choice(statuses),
            "total_amount":   round(random.uniform(5.00, 300.00), 2),
        })

    # Deliberate quality issue: introduce duplicate orders
    rows = intentional_duplicate(rows, dupe_rate=0.01)
    return rows

def generate_order_items(order_ids, product_ids):
    rows = []
    for i in range(1, NUM_ORDER_ITEMS + 1):
        unit_price = round(random.uniform(0.99, 49.99), 2)
        quantity   = random.randint(1, 10)

        # Deliberate quality issue: 0.5% of items have negative quantity
        if random.random() < 0.005:
            quantity = -quantity

        rows.append({
            "order_item_id": i,
            "order_id":      random.choice(order_ids),
            "product_id":    random.choice(product_ids),
            "quantity":      quantity,
            "unit_price":    unit_price,
            "discount":      round(random.uniform(0, 0.3), 2),
        })
    return rows

def generate_inventory_snapshots(product_ids):
    warehouses = ["WH_CHICAGO", "WH_NEWYORK", "WH_LA", "WH_HOUSTON"]
    rows = []
    for i in range(NUM_INV_SNAPSHOTS):
        qty_on_hand   = random.randint(0, 500)
        qty_reserved  = random.randint(0, qty_on_hand)
        snapshot_date = random_date(START_DATE, END_DATE)
        rows.append({
            "snapshot_id":    i + 1,
            "warehouse_id":   random.choice(warehouses),
            "product_id":     random.choice(product_ids),
            "snapshot_date":  snapshot_date.date(),
            "qty_on_hand":    qty_on_hand,
            "qty_reserved":   qty_reserved,
        })
    return rows

# ─── Writers ─────────────────────────────────────────────────────────────────

def write_csv(filename, rows):
    if not rows:
        print(f"WARNING: no rows for {filename}")
        return
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Written {len(rows):>6} rows --> {path}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Generating PeakCart sample data...\n")

    suppliers  = generate_suppliers()
    write_csv("suppliers.csv", suppliers)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    products   = generate_products(supplier_ids)
    write_csv("products.csv", products)
    product_ids = [p["product_id"] for p in products]

    customers  = generate_customers()
    write_csv("customers.csv", customers)
    customer_ids = [c["customer_id"] for c in customers]

    orders     = generate_orders(customer_ids)
    write_csv("orders.csv", orders)
    order_ids = [o["order_id"] for o in orders]

    order_items = generate_order_items(order_ids, product_ids)
    write_csv("order_items.csv", order_items)

    inventory  = generate_inventory_snapshots(product_ids)
    write_csv("inventory_snapshots.csv", inventory)

    print("\nDone. All files written to shared/data-generators/output/")

if __name__ == "__main__":
    main()