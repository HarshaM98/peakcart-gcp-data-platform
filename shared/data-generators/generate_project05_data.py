"""
PeakCart Project 5: Supply Chain ML Sample Data Generator
Generates a daily demand + inventory simulation with genuine causal
structure (day-of-week seasonality, price elasticity, and stockouts that
actually depend on lead time and demand velocity), unlike the purely
uniform-random fields in generate_peakcart_data.py.

This is a SEPARATE script from generate_peakcart_data.py -- projects 1
and 3 depend on that script's exact row counts and quality-issue rates
(see root CLAUDE.md), so it's never touched here. This script instead
reads its already-generated products.csv/suppliers.csv as fixed inputs,
to keep product/supplier identities consistent across the whole repo.

Why inject real signal at all: a model trained on genuinely uncorrelated
random features (as in the other generators, deliberately -- they exist
to test pipeline robustness, not ML) would just fit noise. For project 5
to be a meaningful demonstration of BigQuery ML / Vertex AI deployment
mechanics, the stockout label needs to actually depend on the features a
real supply-chain model would use (lead time, recent demand velocity),
or evaluating the model would be theater.
"""

import csv
import os
import random
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)

INPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output", "project-05")
os.makedirs(OUTPUT_DIR, exist_ok=True)

SIM_START = datetime(2025, 1, 1)
SIM_DAYS = 365

WAREHOUSES = ["WH_CHICAGO", "WH_NEWYORK", "WH_LA", "WH_HOUSTON"]

# Baseline daily demand rate per category, before any modifiers. Roughly
# proportional to real turnover -- perishables (Produce/Dairy/Bakery/Meat)
# sell faster than shelf-stable goods (Household/Snacks).
CATEGORY_BASE_DEMAND = {
    "Produce": 14.0,
    "Dairy": 12.0,
    "Meat": 8.0,
    "Bakery": 10.0,
    "Beverages": 9.0,
    "Frozen": 6.0,
    "Snacks": 7.0,
    "Household": 4.0,
}

# Weekend multiplier applied to every category alike -- grocery demand is
# genuinely higher on weekends. Kept category-independent so the signal
# stays simple and explainable, rather than modeling a separate curve
# per category.
WEEKEND_MULTIPLIER = 1.35

# Safety stock factor: reorder point = avg_daily_demand * lead_time_days *
# this factor. A factor of 1.0 means "just enough to survive the lead
# time at average demand" -- any demand spike during the lead time window
# causes a real stockout, which is exactly the mechanism that makes
# lead_time_days and recent demand velocity genuine predictive features.
# Deliberately under-provisioned (< 1.0): with a comfortable buffer,
# stockouts become too rare (<1%) and too noise-dominated for the
# lead-time/velocity relationship to show up cleanly in a first-pass
# model -- this value was tuned down after checking the actual
# stockout-rate-by-bucket breakdown produced a weak/inconsistent signal.
SAFETY_STOCK_FACTOR = 0.85

REORDER_QUANTITY_DAYS_OF_SUPPLY = 21  # each reorder covers ~3 weeks of avg demand


def load_products():
    with open(os.path.join(INPUT_DIR, "products.csv")) as f:
        return list(csv.DictReader(f))


def load_suppliers():
    with open(os.path.join(INPUT_DIR, "suppliers.csv")) as f:
        return {row["supplier_id"]: row for row in csv.DictReader(f)}


def price_elasticity_multiplier(price: float, category_prices: list) -> float:
    """Products priced above their category's median sell more slowly,
    below it sell faster -- a real (if simplified) price-elasticity
    relationship, rather than price being an uncorrelated random field."""
    sorted_prices = sorted(category_prices)
    median = sorted_prices[len(sorted_prices) // 2]
    if median == 0:
        return 1.0
    ratio = price / median
    # Higher price -> lower multiplier. Elasticity exponent of -0.6 keeps
    # the effect present but not overwhelming relative to seasonality/noise.
    return ratio ** -0.6


def simulate_product(product, category_prices, supplier_lead_time, warehouse):
    category = product["category"]
    price = float(product["price"])
    base_demand = CATEGORY_BASE_DEMAND.get(category, 8.0)
    elasticity = price_elasticity_multiplier(price, category_prices)
    avg_daily_demand = base_demand * elasticity

    lead_time_days = supplier_lead_time if supplier_lead_time is not None else 7
    reorder_point = avg_daily_demand * lead_time_days * SAFETY_STOCK_FACTOR
    reorder_quantity = avg_daily_demand * REORDER_QUANTITY_DAYS_OF_SUPPLY

    stock = reorder_point + reorder_quantity  # start fully stocked
    pending_reorders = []  # list of (arrival_day_index, quantity)

    demand_rows = []
    inventory_rows = []
    recent_demand = []  # rolling window for velocity feature

    for day_index in range(SIM_DAYS):
        date = SIM_START + timedelta(days=day_index)
        is_weekend = date.weekday() >= 5
        multiplier = WEEKEND_MULTIPLIER if is_weekend else 1.0
        # Demand noise: present but secondary to the real signal above.
        noisy_demand = avg_daily_demand * multiplier * random.uniform(0.75, 1.35)
        units_sold = max(0, round(noisy_demand))

        # Receive any reorders arriving today.
        arriving = [qty for arrival_day, qty in pending_reorders if arrival_day == day_index]
        qty_received = sum(arriving)
        pending_reorders = [(d, q) for d, q in pending_reorders if d != day_index]
        stock += qty_received

        # Fulfill today's demand (can't sell what isn't there).
        fulfilled = min(units_sold, stock)
        stock -= fulfilled
        stockout_flag = 1 if stock <= 0 else 0

        recent_demand.append(units_sold)
        if len(recent_demand) > 7:
            recent_demand.pop(0)
        rolling_7d_demand = sum(recent_demand) / len(recent_demand)

        # Reorder logic: once stock drops to the reorder point and there's
        # no reorder already in flight, place one -- arrives after the
        # supplier's real lead time.
        reorder_in_flight = len(pending_reorders) > 0
        if stock <= reorder_point and not reorder_in_flight:
            pending_reorders.append((day_index + lead_time_days, reorder_quantity))

        demand_rows.append({
            "product_id": product["product_id"],
            "date": date.date().isoformat(),
            "units_sold": units_sold,
        })
        inventory_rows.append({
            "product_id": product["product_id"],
            "warehouse_id": warehouse,
            "date": date.date().isoformat(),
            "qty_on_hand": round(stock, 1),
            "qty_received": round(qty_received, 1),
            "rolling_7d_avg_demand": round(rolling_7d_demand, 2),
            "lead_time_days": lead_time_days,
            "stockout_flag": stockout_flag,
        })

    return demand_rows, inventory_rows


def main():
    products = load_products()
    suppliers = load_suppliers()

    # Median price per category, needed for the elasticity calculation.
    category_prices = {}
    for p in products:
        category_prices.setdefault(p["category"], []).append(float(p["price"]))

    all_demand_rows = []
    all_inventory_rows = []

    for product in products:
        if product["supplier_id"] == "" or product["is_active"] == "False":
            continue  # skip products with no supplier or discontinued -- no real reorder story
        supplier = suppliers.get(product["supplier_id"])
        lead_time = int(supplier["lead_time_days"]) if supplier else 7
        warehouse = random.choice(WAREHOUSES)

        demand_rows, inventory_rows = simulate_product(
            product, category_prices[product["category"]], lead_time, warehouse
        )
        all_demand_rows.extend(demand_rows)
        all_inventory_rows.extend(inventory_rows)

    write_csv("product_demand_daily.csv", all_demand_rows)
    write_csv("inventory_daily.csv", all_inventory_rows)

    stockout_rate = sum(r["stockout_flag"] for r in all_inventory_rows) / len(all_inventory_rows)
    print(f"Generated {len(all_demand_rows)} demand rows, {len(all_inventory_rows)} inventory rows")
    print(f"Overall stockout rate: {stockout_rate:.2%}")


def write_csv(filename, rows):
    if not rows:
        print(f"WARNING: no rows for {filename}")
        return
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
