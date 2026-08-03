"""
PeakCart Project 2: Legacy Migration Bulk-Load DAG
====================================================

Orchestrates the Dataproc/Spark bulk migration job (scripts/bulk_migrate.py)
with an ephemeral cluster lifecycle -- create -> submit job -> delete --
rather than reusing a standing cluster. This is the standard real-world
Composer+Dataproc pattern: a cluster that only exists for the duration of
one job costs nothing the rest of the day, unlike the persistent cluster
used for interactive development in Phase 2.

Datastream (the CDC/incremental-sync half of this project) is deliberately
NOT part of this DAG -- it's an always-on managed stream, not a batch job
to trigger periodically, the same reason project-04's rollup DAG doesn't
try to manage the Beam streaming pipeline's lifecycle either.

The cluster connects to Cloud SQL over its private IP directly (both are
in the same VPC) -- no Cloud SQL Auth Proxy needed for this path, unlike
the manual Phase 2 cluster's now-unused init action.

Schedule: manual/on-demand only (schedule_interval=None) -- this is a
one-off bulk migration, not a recurring rollup like project-04's DAG.
Owner: data-engineering@peakcart.com
"""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

PROJECT_ID = "harsha-data-platform"
REGION = "us-central1"
ZONE = "us-central1-a"
CLUSTER_NAME = "peakcart-migration-dag-cluster"
STAGING_BUCKET = "peakcart-dataproc-staging-2026"
BRONZE_DATASET = "peakcart_migration_bronze"
CLOUD_SQL_PRIVATE_IP = "10.90.1.3"
DATAPROC_SERVICE_ACCOUNT = (
    "peakcart-dataproc-migration@harsha-data-platform.iam.gserviceaccount.com"
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-engineering@peakcart.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# Same shape as the Terraform-managed Phase 2 cluster (single-node,
# e2-standard-2, internal_ip_only -- this project's org policy blocks
# external IPs on VMs) but created/destroyed per DAG run instead of left
# standing.
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "software_config": {
        "properties": {"dataproc:dataproc.allow.zero.workers": "true"},
    },
    "gce_cluster_config": {
        "service_account": DATAPROC_SERVICE_ACCOUNT,
        "internal_ip_only": True,
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform",
        ],
        "zone_uri": f"projects/{PROJECT_ID}/zones/{ZONE}",
    },
    "config_bucket": STAGING_BUCKET,
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{STAGING_BUCKET}/pyspark/bulk_migrate.py",
        "jar_file_uris": [f"gs://{STAGING_BUCKET}/jars/postgresql-42.7.4.jar"],
        "args": [
            f"--project={PROJECT_ID}",
            f"--staging-bucket={STAGING_BUCKET}",
            f"--db-host={CLOUD_SQL_PRIVATE_IP}",
        ],
    },
}

# One combined check rather than 7 separate tasks -- MIN() across all the
# per-table pass/fail flags catches a silent partial migration (e.g. one
# table truncated by a retried job) as a single gate, same style as
# project-04's BigQueryCheckOperator quality gates.
VALIDATE_COUNTS_SQL = f"""
SELECT MIN(IF(actual = expected, 1, 0)) = 1 AS all_counts_match
FROM (
  SELECT 'suppliers' AS t, (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.suppliers`) AS actual, 20 AS expected
  UNION ALL SELECT 'customers', (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.customers`), 1000
  UNION ALL SELECT 'products', (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.products`), 200
  UNION ALL SELECT 'orders', (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.orders`), 5000
  UNION ALL SELECT 'order_items', (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.order_items`), 15000
  UNION ALL SELECT 'inventory_snapshots', (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.inventory_snapshots`), 3000
  UNION ALL SELECT 'product_price_history', (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.product_price_history`), 359
)
"""

with DAG(
    dag_id="project02_migration_bulk_load",
    default_args=default_args,
    description="Ephemeral-cluster Dataproc bulk migration of legacy Postgres data to BigQuery",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["project-02", "migration", "dataproc"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_bulk_migration_job = DataprocSubmitJobOperator(
        task_id="submit_bulk_migration_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",
    )

    validate_migration_counts = BigQueryCheckOperator(
        task_id="validate_migration_counts",
        sql=VALIDATE_COUNTS_SQL,
        use_legacy_sql=False,
        location=REGION,
    )

    create_cluster >> submit_bulk_migration_job >> delete_cluster >> validate_migration_counts
