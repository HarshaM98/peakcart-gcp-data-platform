"""Simulates ongoing writes against the legacy PostgreSQL instance, so the
Datastream CDC stream has real INSERT/UPDATE/DELETE changes to capture.
The bulk-migrated data is static (generated once), so without this there's
nothing for incremental sync to actually pick up.

Connects via the Cloud SQL Auth Proxy (see COMMANDS.md) on 127.0.0.1:5432,
same as load_legacy_data.py.
"""

import os

import psycopg2

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "peakcart_legacy"
DB_USER = "peakcart_app"
DB_PASSWORD = os.environ["PEAKCART_LEGACY_DB_PASSWORD"]


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # INSERT: a brand new order Datastream should see as a new row.
    cur.execute(
        """
        insert into orders (order_id, customer_id, order_date, delivery_date, status, total_amount)
        values (900001, 1, current_date, current_date + interval '5 days', 'placed', 42.50)
        """
    )
    print("Inserted new order 900001")

    # UPDATE: an existing order's status changes -- Datastream should
    # capture this as a row update, not a fresh insert.
    cur.execute("update orders set status = 'delivered' where order_id = 1")
    print("Updated order 1 status to delivered")

    # DELETE: verifies Datastream captures deletes too (not just inserts/
    # updates) -- something a naive polling-based sync would need special
    # handling for, but native CDC gets for free.
    cur.execute("delete from orders where order_id = 2")
    print("Deleted order 2")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
