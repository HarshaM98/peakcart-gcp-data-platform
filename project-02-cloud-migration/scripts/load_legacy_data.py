"""Loads the shared PeakCart CSVs into the legacy Cloud SQL PostgreSQL
instance, representing the on-prem OLTP database this project migrates
from. Connects via the Cloud SQL Auth Proxy (expected running on
localhost:5432 -- see COMMANDS.md).
"""

import csv
import os

import psycopg2

DATA_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "shared", "data-generators", "output"
)

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "peakcart_legacy"
DB_USER = "peakcart_app"
DB_PASSWORD = os.environ["PEAKCART_LEGACY_DB_PASSWORD"]

TABLES = {
    "suppliers": {
        "ddl": """
            create table if not exists suppliers (
                supplier_id integer primary key,
                name text,
                region text,
                lead_time_days integer,
                is_active boolean
            )
        """,
        "csv": "suppliers.csv",
    },
    "customers": {
        "ddl": """
            create table if not exists customers (
                customer_id integer primary key,
                name text,
                email text,
                city text,
                state text,
                signup_date date,
                segment text
            )
        """,
        "csv": "customers.csv",
    },
    "products": {
        "ddl": """
            create table if not exists products (
                product_id integer primary key,
                name text,
                category text,
                subcategory text,
                price numeric,
                supplier_id integer,
                is_active boolean
            )
        """,
        "csv": "products.csv",
    },
    "orders": {
        "ddl": """
            create table if not exists orders (
                order_id integer primary key,
                customer_id integer,
                order_date date,
                delivery_date date,
                status text,
                total_amount numeric
            )
        """,
        "csv": "orders.csv",
    },
    "order_items": {
        "ddl": """
            create table if not exists order_items (
                order_item_id integer primary key,
                order_id integer,
                product_id integer,
                quantity integer,
                unit_price numeric,
                discount numeric
            )
        """,
        "csv": "order_items.csv",
    },
    "inventory_snapshots": {
        "ddl": """
            create table if not exists inventory_snapshots (
                snapshot_id integer primary key,
                warehouse_id text,
                product_id integer,
                snapshot_date date,
                qty_on_hand integer,
                qty_reserved integer
            )
        """,
        "csv": "inventory_snapshots.csv",
    },
    "product_price_history": {
        "ddl": """
            create table if not exists product_price_history (
                price_id integer primary key,
                product_id integer,
                price numeric,
                effective_date date,
                end_date date,
                change_reason text
            )
        """,
        "csv": "product_price_history.csv",
    },
}

# Load order matters for foreign-key-shaped data even without real FK
# constraints -- suppliers/customers before products/orders that reference them.
LOAD_ORDER = [
    "suppliers",
    "customers",
    "products",
    "orders",
    "order_items",
    "inventory_snapshots",
    "product_price_history",
]


# Primary key column per table, used to dedupe during the staging->merge
# load (see main()).
PRIMARY_KEYS = {
    "suppliers": "supplier_id",
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "inventory_snapshots": "snapshot_id",
    "product_price_history": "price_id",
}


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    for table in LOAD_ORDER:
        spec = TABLES[table]
        pk = PRIMARY_KEYS[table]
        staging_table = f"{table}_staging"

        print(f"Creating table {table}...")
        cur.execute(spec["ddl"])

        # Stage-then-merge: land the raw CSV in an unconstrained staging
        # table first, then dedupe into the constrained final table. A real
        # legacy OLTP source wouldn't have duplicate primary keys -- any
        # found here are a flat-file export artifact, not something an
        # actual live database would produce, so they're resolved here
        # rather than by relaxing the target schema's constraints.
        cur.execute(f"drop table if exists {staging_table}")
        cur.execute(
            spec["ddl"]
            .replace(f"create table if not exists {table}", f"create table {staging_table}")
            .replace("primary key", "")
        )

        csv_path = os.path.join(DATA_DIR, spec["csv"])
        print(f"Loading {csv_path} into {staging_table}...")
        with open(csv_path, "r") as f:
            cur.copy_expert(
                f"copy {staging_table} from stdin with (format csv, header true, null '')",
                f,
            )

        cur.execute(f"select count(*) from {staging_table}")
        staged_count = cur.fetchone()[0]

        cur.execute(f"select count(distinct {pk}) from {staging_table}")
        distinct_count = cur.fetchone()[0]
        duplicates = staged_count - distinct_count
        if duplicates:
            print(
                f"  WARNING: {table} source data has {duplicates} duplicate "
                f"'{pk}' rows (staged {staged_count}, {distinct_count} distinct) "
                f"-- deduping before merge into the constrained table"
            )

        cur.execute(f"truncate table {table}")
        cur.execute(
            f"insert into {table} select distinct on ({pk}) * from {staging_table} "
            f"order by {pk}"
        )
        cur.execute(f"drop table {staging_table}")

        cur.execute(f"select count(*) from {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count} rows loaded")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
