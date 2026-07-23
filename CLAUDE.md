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
| 4   | `project-04-realtime-ops`    | In progress | Pub/Sub, Dataflow (Apache Beam) streaming, BigQuery        |
| 5   | `project-05-supply-chain-ml` | Not started | Dataform, BigQuery ML, Vertex AI                           |
| 6   | `project-06-governance-ai`   | Not started | Dataplex, IAM, GenAI workflows                             |

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
python pipeline/step7_dedup_late_data.py    # current Beam streaming pipeline: all 3 topics, dedup + late data on orders
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
  → Apache Beam pipeline (pipeline/step*.py) → BigQuery (orders_per_minute)
```

- Terraform module `infrastructure/terraform/modules/pubsub_topic_with_dlq/` is reused per topic (order/delivery/inventory events), each configured with `max_delivery_attempts = 5` and exactly-once delivery. The Pub/Sub service agent email is computed directly from `data.google_project.current.number` (a fixed, documented Google pattern) rather than depending on `google_project_service_identity`'s output — this was a deliberate fix to avoid IAM bindings being destroyed/recreated on every apply (see `main.tf` comments).
- `simulator/event_simulator.py` deliberately injects duplicates, out-of-order/stale timestamps, and randomly-dropped optional fields (`maybe_drop_field`) so the Beam pipeline has to handle real messiness rather than clean data.
- Pipeline scripts under `pipeline/` are numbered increments, each building on the last: `step1_read_and_print.py` → `step2_parse_and_timestamp.py` → `step3_windowed_count.py` (windowed per-zone order counts, printed only, defensive `.get()` fallback for missing `warehouse_zone`) → `step4_dead_letter.py` (real validation via a `ParseAndValidate` DoFn using Beam's `TaggedOutput`/`.with_outputs()`, malformed events routed to a `malformed_events` BigQuery table instead of crashing or guessing defaults) → `step5_write_to_bigquery.py` (well-formed branch also writes `orders_per_minute` to BigQuery, not just prints) → `step6_all_topics.py` (generalizes `ParseAndValidate` to take `required_fields`/`subscription_name` per topic, so all three topics — order/delivery/inventory — are read and validated; delivery and inventory are validated-and-printed only, not yet aggregated, since each needs different logic — event pairing, session windows — than a simple count) → `step7_dedup_late_data.py` (current step: adds dedup on the order branch, keyed on `order_id + event_type + timestamp` since `order_id` alone would wrongly collapse an order's distinct lifecycle events, plus late-data handling via `allowed_lateness=300s` and `ACCUMULATING` mode to match the simulator's up-to-5-minute-stale timestamps; delivery/inventory branches remain unchanged from step6). `pipeline/check_dedup.py` is a standalone script demonstrating the dedup key logic in isolation; `test_step7_all_topics.py` holds the current unit tests for `ParseAndValidate` across all three event types.
- JSON Schema definitions and example payloads for all three event types live in `schemas/` — check these before changing event shapes in the simulator or pipeline.
- Delivery/inventory aggregation (`avg_pick_time`, `active_deliveries`, `inventory_velocity`) is the next unfinished step — deferred because it needs event-pairing/session-window logic rather than the simple per-key count used for orders.

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
