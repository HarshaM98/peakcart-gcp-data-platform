# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository overview

A portfolio of six independent GCP data-engineering projects built around "PeakCart," a fictional
grocery delivery company. Each `project-0N-*` directory is largely self-contained (own README,
infrastructure, and pipeline code), but several share the same underlying dataset and dbt project.
GCP project id: `harsha-data-platform`.

| #   | Project                      | Status      | Key tech                                                   |
| --- | ---------------------------- | ----------- | ---------------------------------------------------------- |
| 1   | `project-01-data-warehouse`  | Built       | BigQuery, GCS, dbt Core, Terraform, Medallion architecture |
| 2   | `project-02-cloud-migration` | Not started | Dataflow, Dataproc, Cloud Composer, PostgreSQL             |
| 3   | `project-03-customer-360`    | Built       | AlloyDB, dbt, Cloud Composer, Looker Studio                |
| 4   | `project-04-realtime-ops`    | Built       | Pub/Sub, Dataflow (Apache Beam) streaming, BigQuery        |
| 5   | `project-05-supply-chain-ml` | Built       | Dataform, BigQuery ML, Vertex AI                           |
| 6   | `project-06-governance-ai`   | Built       | Dataplex, IAM, GenAI workflows                             |

## Commands

### Data generation (run from repo root)

```bash
python3.11 shared/data-generators/generate_peakcart_data.py     # core dataset: customers, products, orders, order_items, suppliers, inventory_snapshots, product_price_history
python3.11 shared/data-generators/generate_project03_data.py    # customer_360 dataset: customer_profiles, order_history, clickstream_events, delivery_feedback
```

Both scripts use a fixed `SEED = 42` and write CSVs to `shared/data-generators/output/` (project-03 under `output/project-03/`). Deliberate data-quality issues (NULL emails, NULL supplier_id, negative quantities) are seeded intentionally — this is expected test data, not a bug.

### dbt (project-01, also used by project-03's `customer_360` models)

All dbt work happens in `project-01-data-warehouse/dbt/peakcart_dbt/` — there is only one dbt project in this repo, shared across projects 1 and 3.

```bash
cd project-01-data-warehouse/dbt/peakcart_dbt
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in your GCP project id first time
dbt debug                                     # verify connection
dbt deps                                      # install dbt_utils
dbt snapshot                                  # SCD Type 2 customer history — run BEFORE dbt run
dbt run                                       # build staging/mart/customer_360 models
dbt test                                      # run all tests
dbt build --select customer_360               # build only the project-03 customer_360 chain
dbt run --full-refresh                        # full rebuild after schema changes
dbt docs generate && dbt docs serve
```

Bronze loading precedes dbt: `bash project-01-data-warehouse/infrastructure/load_bronze.sh` (or `project-03-customer-360/infrastructure/load_bronze.sh` for the customer_360 sources).

Run a single test: `dbt test --select <model_name>` or `dbt test --select test_type:singular` for the two singular tests in `tests/`.

### Terraform (per-project infrastructure)

Each project's infra lives at `project-0N-*/infrastructure/terraform/`. Standard flow:

```bash
cd project-0N-*/infrastructure/terraform
./bootstrap.sh        # project-01 only, first time: creates remote state bucket
terraform init
terraform plan
terraform apply
```

Remote state bucket: `gs://peakcart-terraform-state-2026`, prefixed per project (e.g. `project-04/dev`).

### Project 4 streaming pipeline

```bash
cd project-04-realtime-ops
pip install -r simulator/requirements.txt   # google-cloud-pubsub
pip install -r pipeline/requirements.txt    # apache-beam[gcp]

python simulator/event_simulator.py         # publishes order/delivery/inventory events to Pub/Sub topics
python pipeline/step10_avg_pick_time.py     # current Beam streaming pipeline: all 3 topics, all 4 metrics
```

### CI

`.github/workflows/dbt-ci.yml` runs on push/PR touching `project-01-data-warehouse/dbt/**`: `dbt deps` → `dbt compile` → `dbt build` (full build; slim CI via `state:modified+` is a known TODO, see "Production Considerations" in `project-01-data-warehouse/README.md`).

## Architecture

### Medallion pipeline (projects 1 and 3 share this)

```
GCS (raw CSVs) → load_bronze.sh (bq load, stage-and-replace) → BigQuery Bronze (raw, NULLABLE)
  → dbt staging (Silver views, cleaned/typed/deduplicated) → dbt marts (Gold facts/dims, incremental)
```

- **Stage-and-replace pattern**: `bq load` can't add computed columns, so `load_bronze.sh` loads CSVs into a staging table, then `CREATE OR REPLACE TABLE` adds `_loaded_at` / `_source_file` metadata before dropping the staging table. Schema JSON files under `infrastructure/schemas/` intentionally contain only source columns, not these metadata columns.
- **SCD Type 2 on `dim_products`**: driven by a `product_price_history` source table with effective-date ranges; NULL end dates become a `9999-12-31` sentinel so `fact_orders` can join on `order_date BETWEEN valid_from AND valid_to` to attribute the price active at purchase time.
- **SCD Type 2 on `dim_customers`**: via dbt snapshots (`snapshots/snap_customers.sql`) — always run `dbt snapshot` before `dbt run` in a fresh pipeline execution, since marts depend on the snapshot table existing.
- **Incremental fact tables**: 3-day lookback window catches late-arriving data; use `dbt run --full-refresh` after schema changes.
- Schema name generation is overridden in `macros/generate_schema_name.sql`.

### project-03-customer-360 layering

Adds an intermediate layer on top of the same dbt project: staging (`models/customer_360/staging/`) → intermediate (sessionization, order metrics, satisfaction — each computed independently for testability) → gold (`customer_360.sql`, one row per customer). Sessionization uses `LAG()` for inter-event gaps and cumulative `SUM()` for session numbering, with a 30-minute inactivity boundary; session IDs are MD5(`customer_id + session_number`) for rerun stability.

Beyond BigQuery, project-03 also loads into **AlloyDB** for low-latency operational lookups (point lookups ~2.5ms vs ~3.5s in BigQuery) — this is a deliberately separate serving path, not a replacement for BigQuery analytics. Orchestration is a Cloud Composer DAG (`dags/customer_360_dag.py`) using BashOperator to invoke dbt directly (dbt is pre-installed in the Composer image), with `BigQueryCheckOperator` quality gates before/after and `BigQueryToGCSOperator` for export.

### project-04-realtime-ops streaming pipeline

```
event_simulator.py → Pub/Sub topics (order/delivery/inventory, each with a DLQ subscription)
  → Apache Beam pipeline (pipeline/step*.py) → BigQuery (orders_per_minute, inventory_velocity,
    active_deliveries, avg_pick_time, malformed_events)
```

- Terraform module `infrastructure/terraform/modules/pubsub_topic_with_dlq/` is reused per topic (order/delivery/inventory events), each configured with `max_delivery_attempts = 5` and exactly-once delivery. The Pub/Sub service agent email is computed directly from `data.google_project.current.number` (a fixed, documented Google pattern) rather than depending on `google_project_service_identity`'s output — this was a deliberate fix to avoid IAM bindings being destroyed/recreated on every apply (see `main.tf` comments).
- `simulator/event_simulator.py` deliberately injects duplicates, out-of-order/stale timestamps, and randomly-dropped optional fields (`maybe_drop_field`) so the Beam pipeline has to handle real messiness rather than clean data.
- Pipeline scripts under `pipeline/` are numbered increments, each building on the last: `step1_read_and_print.py` → `step2_parse_and_timestamp.py` → `step3_windowed_count.py` (windowed per-zone order counts, printed only, defensive `.get()` fallback for missing `warehouse_zone`) → `step4_dead_letter.py` (real validation via a `ParseAndValidate` DoFn using Beam's `TaggedOutput`/`.with_outputs()`, malformed events routed to a `malformed_events` BigQuery table instead of crashing or guessing defaults) → `step5_write_to_bigquery.py` (well-formed branch also writes `orders_per_minute` to BigQuery, not just prints) → `step6_all_topics.py` (generalizes `ParseAndValidate` to take `required_fields`/`subscription_name` per topic, so all three topics — order/delivery/inventory — are read and validated; delivery and inventory are validated-and-printed only, not yet aggregated) → `step7_dedup_late_data.py` (adds dedup on the order branch, keyed on `order_id + event_type + timestamp`, plus late-data handling via `allowed_lateness=300s` and `ACCUMULATING` mode) → `step8_inventory_velocity.py` (inventory branch aggregated: windowed sum of `quantity_change` per warehouse+product, with the same dedup pattern reused since a sum, unlike a print, is corrupted by an undetected duplicate) → `step9_active_deliveries.py` (delivery branch aggregated: per window, latest event per `delivery_id` via `take_latest_by_timestamp`, filtered to exclude `completed`, counted with `Count.Globally().without_defaults()` — the `.without_defaults()` is required for non-`GlobalWindow` combines) → `step10_avg_pick_time.py` (current step: adds `avg_pick_time` — average seconds between an order's `placed` and `picked` events, paired via `Sessions(gap=90s)` windowing + `GroupByKey` per `order_id`, since this needs event pairing across time rather than a per-key combine; deliberately uses Beam's default single-fire trigger, not the late-tolerant pattern used elsewhere, since a session refire would double-count an already-paired order). `pipeline/check_dedup.py` is a standalone script demonstrating the order dedup key logic in isolation; `test_step10_avg_pick_time.py` (plus test_step7/8/9) hold the current unit tests.
- `simulator/event_simulator.py`'s `OrderLifecycle` (added for step10) advances each order through `placed → picked → packed → shipped → delivered` with a real randomized delay between stages, so downstream metrics can pair two stages of the same order in live data — order events are no longer one-off/memoryless like delivery and inventory events still are.
- JSON Schema definitions and example payloads for all three event types live in `schemas/` — check these before changing event shapes in the simulator or pipeline.
- All three originally-deferred aggregations (`avg_pick_time`, `active_deliveries`, `inventory_velocity`) are now implemented. `avg_pick_time` initially couldn't be confirmed live against DirectRunner — it appears not to fire merging (Session) windows against an unbounded source within practical test durations, even though non-merging `FixedWindows` fire fine under the same watermark (see NOTES.md 2026-07-24 entries) — but this was confirmed to be a DirectRunner-specific limitation, not a code defect, once verified on real Dataflow (see below).
- **Deployed and verified on real Dataflow** (2026-07-27): Terraform in `infrastructure/terraform/main.tf` provisions the Dataflow API, a dedicated staging bucket (`peakcart-dataflow-staging-2026`), and a scoped `peakcart-dataflow-worker` service account (not the default Compute Engine SA). Launch with `--runner=DataflowRunner --project=harsha-data-platform --region=us-central1 --temp_location=gs://peakcart-dataflow-staging-2026/temp --staging_location=gs://peakcart-dataflow-staging-2026/staging --service_account_email=peakcart-dataflow-worker@harsha-data-platform.iam.gserviceaccount.com`; add `--worker_zone=us-central1-a` if worker startup fails with `ZONE_RESOURCE_POOL_EXHAUSTED` (a transient capacity issue in a specific zone, seen on the first attempt). A verification run confirmed all five BigQuery tables get correct output on Dataflow, including `avg_pick_time` firing correctly (unlike on DirectRunner). The job was cancelled after verification — nothing is currently deployed/running or billing.
- **Cloud Composer orchestration** (2026-07-28): `dags/project04_streaming_rollup_dag.py` is a daily (3 AM UTC) batch rollup DAG — dedupes and aggregates the five streaming tables into daily summary tables (`orders_daily`, `inventory_velocity_daily`, `active_deliveries_daily`, `avg_pick_time_daily`, `malformed_events_daily`), gated by a `BigQueryCheckOperator` freshness check. Chosen over streaming-job lifecycle management or a test-harness DAG since it's the idiomatic use of Airflow (scheduled batch tasks over an already-running stream, not babysitting a long-running Dataflow job). Every rollup query dedupes to the latest row per window first (`ROW_NUMBER() ... WHERE rn = 1`) before aggregating — the streaming tables can have multiple rows per window from late-data refires under `ACCUMULATING` mode, and a naive `SUM()`/`AVG()` over raw rows is ~10x wrong. Verified live on a temporary Composer environment (created, DAG triggered, all 7 tasks succeeded with results exactly matching manual verification, then deleted — see NOTES.md 2026-07-28 entries for the worker-pod-churn troubleshooting story). No Composer environment is currently running.
- **project-04 status**: pipeline (steps 1-10), Dataflow deployment, and Composer rollup orchestration are all built and verified. Nothing is currently deployed/running in GCP for this project beyond the persistent Terraform-managed resources (Pub/Sub topics + DLQs, Dataflow staging bucket, Dataflow worker SA).

### Shared data generators

`shared/data-generators/` is the single source of sample data for projects 1 and 3. Both scripts use `SEED = 42` for reproducibility — do not remove the seed, since row counts and specific data-quality issue rates documented in project READMEs (e.g., "2% of customers have NULL emails") depend on it.

## Lessons & Guardrails

- **Secrets**: never embed credentials inline in YAML (e.g. `keyfile_json` with a JSON blob). Always write secrets to a file and reference by path (`keyfile: /tmp/gcp-key.json`). An inline-JSON approach caused a real credential leak in project-03's CI (private key printed in plaintext logs) — see that project's study notes for the full incident.
- **Version pinning**: dbt-core must be pinned to the exact same version (1.8.7) across local dev, GitHub Actions CI, and Cloud Composer. Never let `pip install dbt-bigquery==X` resolve dbt-core unpinned — it previously resolved to an alpha pre-release with incompatible YAML validation. Composer's pre-installed version is the constraint every other environment matches, not the other way around.
- **`CLOUDSDK_PYTHON`**: must stay pinned in `~/.zshrc`. Activating a Python virtualenv (dbt-env or streaming-env) can hijack `gcloud`'s Python resolution if this isn't explicitly set.
- **Data files**: generated CSVs/sample data always go in `shared/data-generators/output/`, never inside the dbt project folder or any `project-0N-*` pipeline directory.
- **Single source of truth for dbt config**: all materialization and schema settings live in `dbt_project.yml` only. Never add `{{ config(...) }}` blocks in individual model SQL files.
- **Workflow preference**: explain reasoning and trade-offs before running commands or writing code, especially for infrastructure changes (Terraform, IAM, GCP resource creation). Prefer single incremental steps over large multi-file changes, and pause for confirmation between steps when the task is non-trivial.
  "Maintain project-04-realtime-ops/NOTES.md with a dated entry after each meaningful task, per the structure already established in that file."
  "Maintain project-04-realtime-ops/COMMANDS.md as a running reference of the actual commands used for this project (simulator, pipeline testing, Terraform, Dataflow deployment, Composer, BigQuery verification) — add to it the first time a new command/workflow is used, rather than letting it drift out of date."
