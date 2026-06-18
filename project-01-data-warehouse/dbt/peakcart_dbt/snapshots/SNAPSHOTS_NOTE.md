# Snapshot Backdating Note

## What happened

The `snap_customers` snapshot was initialized after historical order data
already existed in the Bronze layer. Order history starts `2025-01-01` but
the snapshot was first run in June 2026.

dbt snapshots initialize `dbt_valid_from` to the timestamp of the first
snapshot run, not to any date in the source data. This is correct and
expected dbt behavior.

## The problem this caused

`fact_orders` joins to `dim_customers` using:

```sql
on o.customer_id = c.customer_id
and o.order_date between c.dbt_valid_from and c.dbt_valid_to
```

With `dbt_valid_from` set to June 2026, every order placed in 2025 had
no matching customer dimension row. The join produced zero results for
all historical orders.

## How it was fixed

A one-time manual BigQuery UPDATE was applied to backdate `dbt_valid_from`
for all currently active customer rows:

```sql
UPDATE peakcart_snapshots.snap_customers
SET dbt_valid_from = TIMESTAMP('2025-01-01')
WHERE dbt_valid_to IS NULL
```

This aligns the snapshot history with the start of order history, making
all historical joins resolve correctly.

## Why this is a portfolio-only workaround

In production this would never happen because snapshots are initialized
before any source data exists. The correct production sequence is:

Day 0: provision infrastructure (Terraform)

Day 0: run dbt snapshot before any data is loaded

Day 1+: load historical data

Day 1+: run dbt snapshot on schedule (daily)

This ensures `dbt_valid_from` naturally reflects true history from the
very first data load with no manual intervention needed.
