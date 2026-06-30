# Project 3: Customer 360 and Personalization Platform

## Overview

A unified customer intelligence platform that combines order history,
clickstream behavior, delivery feedback, and demographics into a single
analytical model. Built on GCP using BigQuery, dbt, AlloyDB, and
Cloud Composer.

## Architecture

GCS (raw CSVs)

|

BigQuery Bronze (stage-and-replace load)

|

dbt Silver (4 staging models, 40 tests)

|

dbt Intermediate (sessionization, order metrics, satisfaction)

|

dbt Gold (customer_360, 1,000 rows, 1 per customer)

|

|---> Looker Studio Dashboard (analytics)

|---> AlloyDB (operational serving, 1,350x faster point lookups)

|

Cloud Composer DAG (daily at 2 AM UTC, SLA 6 AM)

## Key Technical Decisions

### Why Three dbt Layers

Staging models clean and type-cast one source table each. Intermediate
models compute per-customer aggregates independently (order metrics,
session metrics, satisfaction). Gold joins everything into one wide
row per customer. This keeps each layer independently testable and
the Gold model readable.

### Sessionization Pattern

Clickstream events are sessionized using LAG() to calculate inter-event
gaps and cumulative SUM() to generate session numbers. Session boundary
is 30 minutes of inactivity. Session IDs are MD5 hashes of
customer_id + session_number for stability across reruns.

### Why AlloyDB Alongside BigQuery

BigQuery point lookups take 2-3 seconds minimum due to query compilation
overhead. AlloyDB returns the same row in 2.577ms (1,350x faster).
BigQuery serves analytical queries across all customers. AlloyDB serves
the app layer for real-time customer profile lookups.

### Cloud Composer Architecture

Tier 1 pattern: dbt Core + BashOperator. dbt 1.8.7 is pre-installed
in Composer 2.9.7. dbt project uploaded to Composer data/ bucket folder.
Two BigQueryCheckOperator quality gates: Bronze freshness before pipeline,
Gold row count and freshness after rebuild. BigQueryToGCSOperator for
native export without CLI commands.

## Pipeline Results

| Metric                | Value                |
| --------------------- | -------------------- |
| Total customers       | 1,000                |
| Active customers      | 952 (95.2%)          |
| Champions             | 51 (5.1%)            |
| At risk               | 311 (31.1%)          |
| Avg customer spend    | $916.65              |
| Top customer spend    | $6,128.64            |
| Total sessions        | 23,283               |
| Cart abandonment rate | 19%                  |
| dbt tests             | 64 (63 pass, 1 warn) |

## AlloyDB Performance

| Query                        | Latency |
| ---------------------------- | ------- |
| AlloyDB point lookup (C0001) | 2.577ms |
| BigQuery same query          | 3,492ms |
| Speedup                      | 1,350x  |

AlloyDB cluster deleted after validation to avoid ongoing costs.
See screenshots/ for performance proof.

## Dashboard

Live Looker Studio dashboard:
https://datastudio.google.com/reporting/41a29bbc-6347-4716-bd12-a51ee08996ed

## How to Run

### Prerequisites

- GCP project with BigQuery, GCS, Composer APIs enabled
- dbt Core 1.8+ with BigQuery adapter
- ~/.dbt/profiles.yml configured for peakcart_dbt

### Generate Data

```bash
python shared/data-generators/generate_project03_data.py
```

### Load Bronze

```bash
bash project-03-customer-360/infrastructure/load_bronze.sh
```

### Run dbt Pipeline

```bash
cd project-01-data-warehouse/dbt/peakcart_dbt
dbt-activate
dbt build --select customer_360
```

### Deploy to Composer

```bash
# Upload dbt project
gcloud storage cp -r project-01-data-warehouse/dbt/peakcart_dbt \
  gs://YOUR_COMPOSER_BUCKET/data/

# Upload DAG
gcloud storage cp project-03-customer-360/dags/customer_360_dag.py \
  gs://YOUR_COMPOSER_BUCKET/dags/
```

## Files

project-03-customer-360/

infrastructure/

load_bronze.sh Bronze load script

schemas/ BigQuery schema JSON files

dags/

customer_360_dag.py Cloud Composer DAG

profiles.yml dbt prod profiles for Composer

screenshots/ Portfolio evidence

alloydb_point_lookup_2ms.png

alloydb_row_count_1000_verified.png

alloydb_index_scan_0079ms.png

alloydb_console_cluster_healthy.png

bigquery_vs_alloydb_latency.png

composer_dag_graph_all_green.png

composer_dag_list_success.png

composer_dag_gantt_final.png

composer_notify_log.png

looker_studio_dashboard.png
project-01-data-warehouse/dbt/peakcart_dbt/models/customer_360/

staging/

sources.yml Bronze table definitions

schema.yml 40 tests

stg_customer_profiles.sql

stg_order_history.sql

stg_clickstream_events.sql

stg_delivery_feedback.sql

intermediate/

schema.yml 10 tests

int_clickstream_sessions.sql

int_customer_order_metrics.sql

int_customer_satisfaction.sql

gold/

schema.yml 8 tests

customer_360.sql
