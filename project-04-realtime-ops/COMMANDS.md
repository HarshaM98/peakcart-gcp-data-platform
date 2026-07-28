# Project 4 Command Reference

A running log of the actual commands used to build, test, and deploy this project.
Not a tutorial — see NOTES.md for the reasoning behind decisions. This file is
just "what do I type" for each workflow. Keep it updated as new commands get used
for the first time; don't let it drift like NOTES.md/CLAUDE.md have in the past.

---

## Setup

```bash
cd project-04-realtime-ops
pip install -r simulator/requirements.txt   # google-cloud-pubsub
pip install -r pipeline/requirements.txt    # apache-beam[gcp]
```

---

## Simulator (publishes to Pub/Sub)

```bash
# Default: 2 minutes, low messiness
python3.11 simulator/event_simulator.py

# Explicit duration/messiness -- messiness controls duplicate/stale-timestamp/
# dropped-field rates. Higher messiness = more malformed_events + more
# duplicate-dedup exercise; lower messiness = cleaner data for verifying a
# specific metric (e.g. avg_pick_time benefits from lower messiness since
# stale timestamps can produce discarded negative-delta samples).
python3.11 simulator/event_simulator.py --duration 2 --messiness 0.2
```

Run in the background with output redirected when you need to keep working
while it runs:
```bash
python3.11 simulator/event_simulator.py --duration 2 --messiness 0.2 > /tmp/sim.log 2>&1 &
```

---

## Pipeline: local testing (DirectRunner)

Free, local, no GCP billing. Good for iterating on transform logic, but its
watermark heuristic against an unbounded Pub/Sub source is slow/unpredictable
(documented repeatedly in NOTES.md) — some windows may not close in a short
run, and merging (Session) windows in particular may not close at all within
a practical test duration. Use TestStream-based unit tests to prove logic
correctness instead of relying on live DirectRunner timing.

```bash
cd pipeline
python3.11 step10_avg_pick_time.py --runner=DirectRunner
```

Run in background, then kill after enough time has passed for windows to close:
```bash
python3.11 step10_avg_pick_time.py --runner=DirectRunner > /tmp/run.log 2>&1 &
# ... wait a few minutes ...
pkill -f step10_avg_pick_time.py
```

### Unit tests

```bash
cd pipeline
python3.11 -m unittest test_step7_all_topics test_step8_inventory_velocity \
  test_step9_active_deliveries test_step10_avg_pick_time -v
```

### Syntax/compile check only (no execution)

```bash
python3.11 -m py_compile step10_avg_pick_time.py
```

---

## Terraform (Pub/Sub topics, Dataflow staging bucket, Dataflow worker SA)

```bash
cd infrastructure/terraform
terraform fmt
terraform validate
terraform init -input=false
terraform plan -input=false -out=/tmp/tfplan
terraform apply -input=false /tmp/tfplan
```

Key outputs used elsewhere:
```bash
terraform output dataflow_staging_bucket           # peakcart-dataflow-staging-2026
terraform output dataflow_worker_service_account   # peakcart-dataflow-worker@harsha-data-platform.iam.gserviceaccount.com
```

---

## Pipeline: real Dataflow deployment

Billable, real Compute Engine workers, runs continuously against the
unbounded Pub/Sub source until explicitly stopped. Always verify briefly
then cancel rather than leaving a job running.

```bash
cd pipeline
python3.11 step10_avg_pick_time.py \
  --runner=DataflowRunner \
  --project=harsha-data-platform \
  --region=us-central1 \
  --temp_location=gs://peakcart-dataflow-staging-2026/temp \
  --staging_location=gs://peakcart-dataflow-staging-2026/staging \
  --service_account_email=peakcart-dataflow-worker@harsha-data-platform.iam.gserviceaccount.com \
  --job_name=peakcart-step10-verify-N
```

If worker startup fails with `ZONE_RESOURCE_POOL_EXHAUSTED` (a transient
capacity issue in one zone, not a config problem), pin a different zone:
```bash
  --worker_zone=us-central1-a
```

### Monitoring and teardown

```bash
gcloud dataflow jobs list --project=harsha-data-platform --region=us-central1 --status=active
gcloud dataflow jobs describe JOB_ID --project=harsha-data-platform --region=us-central1 --format='value(currentState)'
gcloud dataflow jobs cancel JOB_ID --project=harsha-data-platform --region=us-central1
```

---

## BigQuery verification queries

Check recent output from a pipeline run (works for both DirectRunner and Dataflow):
```bash
bq query --nouse_legacy_sql --format=pretty "
  SELECT * FROM \`harsha-data-platform.peakcart_streaming.orders_per_minute\`
  WHERE pipeline_processed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
  ORDER BY pipeline_processed_at DESC LIMIT 10"
```

Same pattern for `active_deliveries`, `inventory_velocity`, `avg_pick_time`.
`malformed_events` uses `processing_time` instead of `pipeline_processed_at`
as its timestamp column — different schema, easy to mix up.

**Important**: streaming tables can have multiple rows per window (late-data
refires under `ACCUMULATING` mode each write the window's current cumulative
value, not a delta). Never `SUM()`/`AVG()` raw rows for a rollup — dedupe to
the latest row per window first:
```sql
SELECT warehouse_zone, SUM(order_count) AS total_orders
FROM (
  SELECT warehouse_zone, order_count,
    ROW_NUMBER() OVER (PARTITION BY window_start, warehouse_zone ORDER BY pipeline_processed_at DESC) AS rn
  FROM `harsha-data-platform.peakcart_streaming.orders_per_minute`
  WHERE DATE(window_start) = '2026-07-28'
)
WHERE rn = 1
GROUP BY warehouse_zone
```
(A naive un-deduped sum gave 753/250/1041 per zone for one test day; the
correct deduped numbers were 74/48/88 — roughly 10x off. See NOTES.md
2026-07-27 entry and `dags/project04_streaming_rollup_dag.py`.)

---

## Cloud Composer (daily rollup DAG)

### One-time project setup (already done, keeping for reference)

```bash
# Dedicated SA for a Composer environment (Composer 3 requires an explicit SA)
gcloud iam service-accounts create peakcart-composer-verify \
  --project=harsha-data-platform \
  --display-name="Temporary Composer verification environment SA"

for ROLE in roles/composer.worker roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding harsha-data-platform \
    --member="serviceAccount:peakcart-composer-verify@harsha-data-platform.iam.gserviceaccount.com" \
    --role="${ROLE}" --condition=None
done

# One-time per-project grant so Composer's own service agent can manage
# environments at all (needed the first time a Composer 2 env is created
# in a project) -- the service agent email's project number is stable
# per-project, get it via: gcloud projects describe harsha-data-platform --format='value(projectNumber)'
gcloud projects add-iam-policy-binding harsha-data-platform \
  --member="serviceAccount:service-435348575003@cloudcomposer-accounts.iam.gserviceaccount.com" \
  --role="roles/composer.ServiceAgentV2Ext" --condition=None
```

### Create / verify / delete an environment

```bash
gcloud composer environments create peakcart-composer-verify \
  --project=harsha-data-platform \
  --location=us-central1 \
  --environment-size=small \
  --image-version=composer-2-airflow-2 \
  --service-account=peakcart-composer-verify@harsha-data-platform.iam.gserviceaccount.com \
  --async
# Takes ~20-25 minutes. Check status:
gcloud composer environments describe peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 --format='value(state)'

# Find the DAGs GCS folder and upload a DAG:
gcloud composer environments describe peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 --format='value(config.dagGcsPrefix)'
gsutil cp dags/project04_streaming_rollup_dag.py gs://<dag-bucket>/dags/

# Tear down when done verifying -- Composer is one of the most expensive
# resources in this repo; don't leave an environment running unattended.
gcloud composer environments delete peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1
```

**Gotcha**: `gcloud composer environments list --locations us-central1 --project=harsha-data-platform`
can return 0 environments even when a Composer *bucket* still exists in GCS
(`gs://us-central1-peakcart-compos-*-bucket/`) — the environment itself can
be deleted while its bucket lingers. A bucket existing is NOT proof an
environment is live; always check `environments list`/`describe` directly.
