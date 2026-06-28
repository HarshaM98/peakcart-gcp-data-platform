"""
PeakCart Customer 360 Pipeline - Production DAG
================================================
Tier 1 production pattern: dbt Core + BashOperator

Composer 2.9.7 pre-installs:
  dbt-bigquery==1.8.2
  dbt-core==1.8.7
  No additional installation needed.

Architecture:
  - SQL transformations live in dbt models (never in this file)
  - BashOperator runs real dbt CLI commands
  - BigQueryCheckOperator for data quality gates
  - BigQueryToGCSOperator for native GCS export
  - PythonOperator for final validation and notification

Schedule:  Daily at 2 AM UTC
SLA:       Complete by 6 AM UTC (4 hour window)
Owner:     data-engineering@peakcart.com
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ID   = "harsha-data-platform"
DATASET_BZ   = "peakcart_bronze"
DATASET_GD   = "peakcart_gold"
GCS_BUCKET   = "peakcart-data-lake-2026"
GCS_PREFIX   = "project-03/alloydb-sync"
LOCATION     = "US-CENTRAL1"

# Composer GCS bucket is mounted at /home/airflow/gcs/
DBT_PROJECT  = "/home/airflow/gcs/data/peakcart_dbt"
DBT_PROFILES = "/home/airflow/gcs/data"

# ── SLA miss callback ─────────────────────────────────────────────────────────
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"""
    ================================================
    SLA MISS ALERT - {dag.dag_id}
    ================================================
    Missed tasks:   {task_list}
    Blocking tasks: {blocking_task_list}
    Time:           {datetime.utcnow()} UTC
    Action:         Investigate task logs immediately.
    Impact:         Marketing campaigns may use stale data.
    ================================================
    """)

# ── Default arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email":             ["data-engineering@peakcart.com"],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_360_pipeline",
    description="Daily Customer 360: Bronze -> Silver -> Gold -> GCS",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["customer_360", "project_03", "daily"],
    dagrun_timeout=timedelta(hours=4),
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ── Task 1: Validate Bronze freshness ─────────────────────────────────────
    validate_bronze = BigQueryCheckOperator(
        task_id="validate_bronze_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{DATASET_BZ}.bronze_customer_profiles`
            WHERE _loaded_at >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(), INTERVAL 48 HOUR
            )
        """,
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        sla=timedelta(minutes=5),
    )

    # ── Task 2: Debug + dbt staging ───────────────────────────────────────────
    # Debug lines find dbt location on the worker.
    # These can be removed once confirmed working.
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"""
            set -e

            echo "=== Finding dbt ==="
            which dbt || true
            find / -name "dbt" -type f 2>/dev/null | head -5 || true
            dbt --version

            echo "=== Checking GCS mount ==="
            ls -la /home/airflow/gcs/data/ || echo "dbt folder not found"
            ls -la /home/airflow/gcs/data/peakcart_dbt/ || echo "peakcart_dbt not found"

            echo "=== Running dbt staging ==="
            cd {DBT_PROJECT}
            dbt deps \
                --profiles-dir {DBT_PROFILES} \
                --target prod
            dbt build \
                --select customer_360.staging \
                --profiles-dir {DBT_PROFILES} \
                --target prod \
                --no-use-colors
            echo "dbt staging complete at $(date)"
        """,
        sla=timedelta(minutes=20),
    )

    # ── Task 3: dbt intermediate ──────────────────────────────────────────────
    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"""
            set -e
            echo "Starting dbt intermediate at $(date)"
            cd {DBT_PROJECT}
            dbt build \
                --select customer_360.intermediate \
                --profiles-dir {DBT_PROFILES} \
                --target prod \
                --no-use-colors
            echo "dbt intermediate complete at $(date)"
        """,
        sla=timedelta(minutes=35),
    )

    # ── Task 4: dbt Gold ──────────────────────────────────────────────────────
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command=f"""
            set -e
            echo "Starting dbt Gold at $(date)"
            cd {DBT_PROJECT}
            dbt build \
                --select customer_360.gold \
                --profiles-dir {DBT_PROFILES} \
                --target prod \
                --no-use-colors
            echo "dbt Gold complete at $(date)"
        """,
        sla=timedelta(minutes=50),
    )

    # ── Task 5: Validate Gold ─────────────────────────────────────────────────
    validate_gold = BigQueryCheckOperator(
        task_id="validate_gold_quality",
        sql=f"""
            SELECT
                COUNT(*) BETWEEN 900 AND 1100
            FROM `{PROJECT_ID}.{DATASET_GD}.customer_360`
            WHERE customer_id IS NOT NULL
              AND rfm_segment IS NOT NULL
              AND model_updated_at >= TIMESTAMP_SUB(
                  CURRENT_TIMESTAMP(), INTERVAL 1 HOUR
              )
        """,
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        sla=timedelta(minutes=55),
    )

    # ── Task 6: Export Gold to GCS ────────────────────────────────────────────
    export_gold_to_gcs = BigQueryToGCSOperator(
        task_id="export_gold_to_gcs",
        source_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET_GD}.customer_360"
        ),
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/{GCS_PREFIX}/customer_360_daily.csv"
        ],
        export_format="CSV",
        print_header=True,
        gcp_conn_id="google_cloud_default",
        sla=timedelta(minutes=60),
    )

    # ── Task 7: AlloyDB sync (documented mock) ────────────────────────────────
    sync_to_alloydb = BashOperator(
        task_id="sync_to_alloydb",
        bash_command="""
            set -e
            echo "================================================"
            echo "AlloyDB Sync - Portfolio Environment"
            echo "================================================"
            echo "Production: SSHOperator -> jump host -> psql COPY"
            echo "AlloyDB validated in Phase 6:"
            echo "  Point lookup:  2.577ms vs BigQuery 3492ms"
            echo "  Speedup:       1350x faster for app serving"
            echo "Cluster deleted after validation to save costs."
            echo "================================================"
        """,
        sla=timedelta(minutes=65),
    )

    # ── Task 8: Validate and notify ───────────────────────────────────────────
    def validate_and_notify(**context):
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        stats = list(client.query(f"""
            SELECT
                COUNT(*)                          AS total_customers,
                COUNTIF(rfm_segment = 'champion') AS champions,
                COUNTIF(rfm_segment = 'at_risk')  AS at_risk,
                COUNTIF(has_no_feedback = true)   AS no_feedback,
                ROUND(AVG(total_spend), 2)         AS avg_spend,
                MAX(model_updated_at)              AS last_updated
            FROM `{PROJECT_ID}.{DATASET_GD}.customer_360`
        """).result())[0]

        print(f"""
        ================================================
        Customer 360 Pipeline - RUN COMPLETE
        ================================================
        Run ID:          {context['run_id']}
        Logical date:    {context['logical_date']}
        ------------------------------------------------
        Total customers:  {stats.total_customers}
        Champions:        {stats.champions}
        At risk:          {stats.at_risk}
        No feedback:      {stats.no_feedback}
        Avg spend:        ${stats.avg_spend}
        Last updated:     {stats.last_updated}
        ------------------------------------------------
        Export: gs://{GCS_BUCKET}/{GCS_PREFIX}/customer_360_daily.csv
        Status: SUCCESS
        Marketing team: Fresh data available.
        ================================================
        """)

    notify_and_validate = PythonOperator(
        task_id="notify_and_validate",
        python_callable=validate_and_notify,
        provide_context=True,
        sla=timedelta(minutes=70),
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        validate_bronze
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_gold
        >> validate_gold
        >> export_gold_to_gcs
        >> sync_to_alloydb
        >> notify_and_validate
    )
