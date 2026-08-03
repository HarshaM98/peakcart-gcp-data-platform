"""PySpark bulk migration job: reads all 7 legacy tables from Cloud SQL
PostgreSQL via JDBC over its private IP (the Dataproc cluster has no
general internet egress -- an org policy blocks external IPs on its VMs --
so it connects directly over the shared VPC rather than through the Cloud
SQL Auth Proxy; see NOTES.md) and writes each into the
peakcart_migration_bronze BigQuery dataset via the Spark BigQuery
connector.

Run with:
  gcloud dataproc jobs submit pyspark bulk_migrate.py \
    --cluster=peakcart-migration-cluster --region=us-central1 \
    --jars=gs://<bucket>/postgresql-42.7.4.jar \
    -- --project=harsha-data-platform --staging-bucket=peakcart-dataproc-staging-2026 \
       --db-host=<cloud-sql-private-ip>
"""

import argparse
import subprocess

from pyspark.sql import SparkSession

DB_USER = "peakcart_app"
DB_DRIVER = "org.postgresql.Driver"

TABLES = [
    "suppliers",
    "customers",
    "products",
    "orders",
    "order_items",
    "inventory_snapshots",
    "product_price_history",
]


def fetch_db_password() -> str:
    # Fetched at runtime via gcloud (preinstalled on all Dataproc images)
    # rather than passed as a plaintext job argument, which would otherwise
    # be visible in job history/logs.
    return subprocess.check_output(
        [
            "gcloud",
            "secrets",
            "versions",
            "access",
            "latest",
            "--secret=peakcart-legacy-db-password",
        ]
    ).decode("utf-8").strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--staging-bucket", required=True)
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--bq-dataset", default="peakcart_migration_bronze")
    args = parser.parse_args()

    db_url = f"jdbc:postgresql://{args.db_host}:5432/peakcart_legacy"

    spark = SparkSession.builder.appName("peakcart-bulk-migration").getOrCreate()

    db_password = fetch_db_password()

    for table in TABLES:
        print(f"Reading {table} from legacy PostgreSQL via JDBC...")
        df = (
            spark.read.format("jdbc")
            .option("url", db_url)
            .option("dbtable", table)
            .option("user", DB_USER)
            .option("password", db_password)
            .option("driver", DB_DRIVER)
            .load()
        )
        row_count = df.count()
        print(f"  {table}: {row_count} rows read")

        print(f"Writing {table} to BigQuery {args.bq_dataset}.{table}...")
        (
            df.write.format("bigquery")
            .option("table", f"{args.project}.{args.bq_dataset}.{table}")
            .option("temporaryGcsBucket", args.staging_bucket)
            .mode("overwrite")
            .save()
        )
        print(f"  {table}: migration complete")

    spark.stop()
    print("Bulk migration done.")


if __name__ == "__main__":
    main()
