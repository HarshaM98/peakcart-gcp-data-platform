# Fixtures

Committed CSV fixtures. Unlike everything in `../output/` (which is generated
and gitignored), these files are tracked in git and copied into `../output/`
by `generate_peakcart_data.py` on every run.

## `product_price_history.csv`

359 rows. One or more price periods per product for all 200 products, with
effective-date ranges and a `change_reason`.

**Why this is a fixture rather than generated output:**

1. It drives the **SCD Type 2** chain in `dim_products`. `fact_orders` joins on
   `order_date BETWEEN valid_from AND valid_to` to attribute the price that was
   actually active at purchase time, so the date ranges have to be internally
   consistent and stable.
2. Its **row count is asserted as a data-quality gate** by project-02's
   Composer DAG (`validate_migration_counts` expects exactly 359 rows). A
   regenerated file would drift and break that gate on every change.
3. Roughly a third of rows deliberately carry a **NULL `end_date`**, which the
   staging model converts to a `9999-12-31` sentinel. NULL end dates silently
   break `BETWEEN` joins, so this file is what exercises that logic.

Because it is byte-stable across clones, everyone gets the same SCD Type 2
results and the same test outcomes.

## Schema

| Column | Type | Notes |
| --- | --- | --- |
| `price_id` | INTEGER | Unique, 1-359 |
| `product_id` | INTEGER | FK to `products.csv` (1-200, all present) |
| `price` | NUMERIC | |
| `effective_date` | DATE | Start of the price period |
| `end_date` | DATE | NULL for the currently-active price |
| `change_reason` | STRING | `annual_review`, `competitive_adjustment`, `seasonal_promotion`, `supplier_cost_increase` |

Matches `project-01-data-warehouse/infrastructure/schemas/bronze_product_price_history.json`.

The data is synthetic, like everything else in this repository.
