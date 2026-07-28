"""
PeakCart Project 4: Streaming Rollup DAG
=========================================
Batch rollup pattern over a streaming pipeline's output.

The Beam pipeline (pipeline/step10_avg_pick_time.py) writes per-1-minute
rows to peakcart_streaming.* using late-data-tolerant triggers
(AfterWatermark with a late trigger, ACCUMULATING mode). That means a
single window can legitimately appear as MULTIPLE rows over time -- each
refire re-emits the window's current cumulative value, not a delta. Any
rollup MUST dedupe to the latest row per window (by pipeline_processed_at)
before aggregating, or every late-data refire gets double-counted as if
it were new data. Confirmed this concretely against live data before
writing this DAG: a naive SUM(order_count) for 2026-07-28 gave
zone_a=753/zone_b=250/zone_c=1041 -- the deduped version gives
zone_a=74/zone_b=48/zone_c=88. The correct numbers are much smaller
than the naive query would suggest.

Why a DAG at all, given the pipeline already runs continuously: Airflow
isn't a good fit for managing an always-on streaming job (Dataflow
already handles that job's own scaling/retries), but it's the natural
fit for the thing a streaming pipeline actually needs downstream --
periodic, dedup-aware batch rollups of noisy per-minute data into clean
daily summaries. Mirrors project-03's customer_360_dag.py conventions
(default_args, BigQueryCheckOperator gates, tags) even though there's no
dbt project here -- transformations are plain SQL via
BigQueryInsertJobOperator since project-04 has no dbt models.

Schedule:  Daily at 3 AM UTC (after customer_360_pipeline's 2 AM slot,
           to avoid resource contention on the same Composer environment)
Owner:     data-engineering@peakcart.com
"""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

PROJECT_ID = "harsha-data-platform"
DATASET = "peakcart_streaming"
LOCATION = "US-CENTRAL1"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-engineering@peakcart.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

# Each rollup script: CREATE TABLE IF NOT EXISTS (idempotent first run),
# then DELETE + INSERT scoped to {{ ds }} (idempotent reruns/backfills for
# the same day -- a retry or manual backfill doesn't duplicate rows). The
# inner ROW_NUMBER() ... WHERE rn = 1 subquery is what collapses each
# window's multiple ACCUMULATING-mode firings down to its final value
# before aggregating.

ORDERS_DAILY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.orders_daily` (
  rollup_date DATE NOT NULL,
  warehouse_zone STRING NOT NULL,
  total_orders INT64 NOT NULL
);

DELETE FROM `{PROJECT_ID}.{DATASET}.orders_daily`
WHERE rollup_date = DATE('{{{{ ds }}}}');

INSERT INTO `{PROJECT_ID}.{DATASET}.orders_daily`
  (rollup_date, warehouse_zone, total_orders)
SELECT
  DATE('{{{{ ds }}}}') AS rollup_date,
  warehouse_zone,
  SUM(order_count) AS total_orders
FROM (
  SELECT
    warehouse_zone,
    order_count,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, warehouse_zone
      ORDER BY pipeline_processed_at DESC
    ) AS rn
  FROM `{PROJECT_ID}.{DATASET}.orders_per_minute`
  WHERE DATE(window_start) = DATE('{{{{ ds }}}}')
)
WHERE rn = 1
GROUP BY warehouse_zone;
"""

INVENTORY_VELOCITY_DAILY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.inventory_velocity_daily` (
  rollup_date DATE NOT NULL,
  warehouse_id STRING NOT NULL,
  product_id STRING NOT NULL,
  net_quantity_change INT64 NOT NULL
);

DELETE FROM `{PROJECT_ID}.{DATASET}.inventory_velocity_daily`
WHERE rollup_date = DATE('{{{{ ds }}}}');

INSERT INTO `{PROJECT_ID}.{DATASET}.inventory_velocity_daily`
  (rollup_date, warehouse_id, product_id, net_quantity_change)
SELECT
  DATE('{{{{ ds }}}}') AS rollup_date,
  warehouse_id,
  product_id,
  SUM(net_quantity_change) AS net_quantity_change
FROM (
  SELECT
    warehouse_id,
    product_id,
    net_quantity_change,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, warehouse_id, product_id
      ORDER BY pipeline_processed_at DESC
    ) AS rn
  FROM `{PROJECT_ID}.{DATASET}.inventory_velocity`
  WHERE DATE(window_start) = DATE('{{{{ ds }}}}')
)
WHERE rn = 1
GROUP BY warehouse_id, product_id;
"""

ACTIVE_DELIVERIES_DAILY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.active_deliveries_daily` (
  rollup_date DATE NOT NULL,
  avg_active_deliveries FLOAT64 NOT NULL,
  max_active_deliveries INT64 NOT NULL,
  windows_observed INT64 NOT NULL
);

DELETE FROM `{PROJECT_ID}.{DATASET}.active_deliveries_daily`
WHERE rollup_date = DATE('{{{{ ds }}}}');

INSERT INTO `{PROJECT_ID}.{DATASET}.active_deliveries_daily`
  (rollup_date, avg_active_deliveries, max_active_deliveries, windows_observed)
SELECT
  DATE('{{{{ ds }}}}') AS rollup_date,
  AVG(active_delivery_count) AS avg_active_deliveries,
  MAX(active_delivery_count) AS max_active_deliveries,
  COUNT(*) AS windows_observed
FROM (
  SELECT
    active_delivery_count,
    ROW_NUMBER() OVER (
      PARTITION BY window_start
      ORDER BY pipeline_processed_at DESC
    ) AS rn
  FROM `{PROJECT_ID}.{DATASET}.active_deliveries`
  WHERE DATE(window_start) = DATE('{{{{ ds }}}}')
)
WHERE rn = 1;
"""

# avg_pick_time_seconds is already an average (per window); rolling it up
# to a daily average needs a weighted mean (weighted by each window's
# sample_count), not a plain AVG() of averages, or a window with 1 sample
# would count as much as one with 20.
AVG_PICK_TIME_DAILY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.avg_pick_time_daily` (
  rollup_date DATE NOT NULL,
  avg_pick_time_seconds FLOAT64 NOT NULL,
  total_samples INT64 NOT NULL
);

DELETE FROM `{PROJECT_ID}.{DATASET}.avg_pick_time_daily`
WHERE rollup_date = DATE('{{{{ ds }}}}');

INSERT INTO `{PROJECT_ID}.{DATASET}.avg_pick_time_daily`
  (rollup_date, avg_pick_time_seconds, total_samples)
SELECT
  DATE('{{{{ ds }}}}') AS rollup_date,
  SUM(avg_pick_time_seconds * sample_count) / SUM(sample_count) AS avg_pick_time_seconds,
  SUM(sample_count) AS total_samples
FROM (
  SELECT
    avg_pick_time_seconds,
    sample_count,
    ROW_NUMBER() OVER (
      PARTITION BY window_start
      ORDER BY pipeline_processed_at DESC
    ) AS rn
  FROM `{PROJECT_ID}.{DATASET}.avg_pick_time`
  WHERE DATE(window_start) = DATE('{{{{ ds }}}}')
)
WHERE rn = 1 AND sample_count > 0;
"""

# malformed_events has no windowing/refire concept (each row is one
# validation failure, written once) -- no dedup subquery needed here,
# just a category rollup. error_reason's prefix before ':' is the stable
# category (missing_fields / invalid_json / invalid_timestamp); the full
# string includes per-event detail (e.g. which fields) that would
# fragment the grouping.
MALFORMED_EVENTS_DAILY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.malformed_events_daily` (
  rollup_date DATE NOT NULL,
  source_subscription STRING NOT NULL,
  error_category STRING NOT NULL,
  event_count INT64 NOT NULL
);

DELETE FROM `{PROJECT_ID}.{DATASET}.malformed_events_daily`
WHERE rollup_date = DATE('{{{{ ds }}}}');

INSERT INTO `{PROJECT_ID}.{DATASET}.malformed_events_daily`
  (rollup_date, source_subscription, error_category, event_count)
SELECT
  DATE('{{{{ ds }}}}') AS rollup_date,
  source_subscription,
  SPLIT(error_reason, ':')[OFFSET(0)] AS error_category,
  COUNT(*) AS event_count
FROM `{PROJECT_ID}.{DATASET}.malformed_events`
WHERE DATE(processing_time) = DATE('{{{{ ds }}}}')
GROUP BY source_subscription, error_category;
"""


def log_rollup_summary(**context):
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    ds = context["ds"]
    stats = list(
        client.query(
            f"""
            SELECT
              (SELECT COALESCE(SUM(total_orders), 0)
               FROM `{PROJECT_ID}.{DATASET}.orders_daily`
               WHERE rollup_date = DATE('{ds}'))               AS total_orders,
              (SELECT ROUND(avg_active_deliveries, 2)
               FROM `{PROJECT_ID}.{DATASET}.active_deliveries_daily`
               WHERE rollup_date = DATE('{ds}'))                AS avg_active_deliveries,
              (SELECT ROUND(avg_pick_time_seconds, 2)
               FROM `{PROJECT_ID}.{DATASET}.avg_pick_time_daily`
               WHERE rollup_date = DATE('{ds}'))                 AS avg_pick_time_seconds,
              (SELECT COALESCE(SUM(event_count), 0)
               FROM `{PROJECT_ID}.{DATASET}.malformed_events_daily`
               WHERE rollup_date = DATE('{ds}'))                AS malformed_events
            """
        ).result()
    )[0]

    print(f"""
    ================================================
    Project-04 Streaming Rollup - RUN COMPLETE
    ================================================
    Rollup date:            {ds}
    ------------------------------------------------
    Total orders:           {stats.total_orders}
    Avg active deliveries:  {stats.avg_active_deliveries}
    Avg pick time (s):      {stats.avg_pick_time_seconds}
    Malformed events:       {stats.malformed_events}
    ------------------------------------------------
    Status: SUCCESS
    ================================================
    """)


with DAG(
    dag_id="project04_streaming_rollup",
    description="Daily dedup-aware batch rollup of project-04 streaming tables",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["project_04", "realtime_ops", "daily", "rollup"],
    dagrun_timeout=timedelta(hours=1),
) as dag:

    validate_source_freshness = BigQueryCheckOperator(
        task_id="validate_source_freshness",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{DATASET}.orders_per_minute`
            WHERE DATE(window_start) = DATE('{{{{ ds }}}}')
        """,
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    rollup_orders = BigQueryInsertJobOperator(
        task_id="rollup_orders_daily",
        configuration={
            "query": {"query": ORDERS_DAILY_SQL, "useLegacySql": False}
        },
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    rollup_inventory_velocity = BigQueryInsertJobOperator(
        task_id="rollup_inventory_velocity_daily",
        configuration={
            "query": {"query": INVENTORY_VELOCITY_DAILY_SQL, "useLegacySql": False}
        },
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    rollup_active_deliveries = BigQueryInsertJobOperator(
        task_id="rollup_active_deliveries_daily",
        configuration={
            "query": {"query": ACTIVE_DELIVERIES_DAILY_SQL, "useLegacySql": False}
        },
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    rollup_avg_pick_time = BigQueryInsertJobOperator(
        task_id="rollup_avg_pick_time_daily",
        configuration={
            "query": {"query": AVG_PICK_TIME_DAILY_SQL, "useLegacySql": False}
        },
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    rollup_malformed_events = BigQueryInsertJobOperator(
        task_id="rollup_malformed_events_daily",
        configuration={
            "query": {"query": MALFORMED_EVENTS_DAILY_SQL, "useLegacySql": False}
        },
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    notify = PythonOperator(
        task_id="log_rollup_summary",
        python_callable=log_rollup_summary,
    )

    # The five rollups are independent of each other (different source
    # tables, no shared state) -- only the freshness gate before and the
    # summary after are real dependencies.
    validate_source_freshness >> [
        rollup_orders,
        rollup_inventory_velocity,
        rollup_active_deliveries,
        rollup_avg_pick_time,
        rollup_malformed_events,
    ] >> notify
