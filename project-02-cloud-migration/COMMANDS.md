# Project 2 Command Reference

A running log of the actual commands used to build, test, and deploy this
project. Not a tutorial -- see NOTES.md for the reasoning behind decisions.
This file is just "what do I type" for each workflow, kept up to date as new
commands get used for the first time (same convention as project-04's
COMMANDS.md).

---

## Terraform (Cloud SQL, Dataproc, BigQuery bronze dataset, Secret Manager)

```bash
cd project-02-cloud-migration/infrastructure/terraform
terraform init
terraform plan -out=/tmp/p2.tfplan
terraform apply /tmp/p2.tfplan
```

Outputs the Cloud SQL instance connection name and the (sensitive) generated
DB password:
```bash
terraform output -raw instance_connection_name
terraform output -raw db_password
```

---

## Cloud SQL Auth Proxy (local access to the legacy Postgres instance)

Used for local data loading/verification -- connects via IAM auth (the
proxy authenticates as your own gcloud ADC identity) rather than
whitelisting a raw IP, so it keeps working regardless of your current
network/IP.

```bash
brew install cloud-sql-proxy   # one-time

cloud-sql-proxy harsha-data-platform:us-central1:peakcart-legacy-postgres \
  --port 5432
```

Leave this running in a separate terminal (or background it) while you run
anything below that connects to `127.0.0.1:5432`.

---

## Loading the legacy data into Cloud SQL

```bash
python3.11 -m venv ~/.venv/migration-env
source ~/.venv/migration-env/bin/activate
pip install -r project-02-cloud-migration/scripts/requirements.txt

export PEAKCART_LEGACY_DB_PASSWORD=$(cd project-02-cloud-migration/infrastructure/terraform && terraform output -raw db_password)

python3 project-02-cloud-migration/scripts/load_legacy_data.py
```

Loads all 7 tables (suppliers, customers, products, orders, order_items,
inventory_snapshots, product_price_history) from the shared CSVs, via a
stage-then-merge pattern that dedupes `orders` (the generator intentionally
seeds ~1% duplicate order rows, by design -- see NOTES.md).

---

## Dataproc cluster (single-node, bulk migration)

The cluster (`google_dataproc_cluster.migration` in `main.tf`) has
`internal_ip_only = true` -- this project's org policy blocks external IPs
on VMs, discovered when the Cloud SQL Auth Proxy running on the node (via
`scripts/install-cloud-sql-proxy.sh`, still present as an init action but
unused by the final approach) couldn't dial Cloud SQL's public IP at all.
The working path instead connects the Spark JDBC read directly to Cloud
SQL's **private IP** over the shared VPC (see "Private IP" below) -- no
proxy needed for VPC-internal traffic. See NOTES.md for the full story.

### Private IP (Cloud SQL <-> Dataproc, same VPC)

Cloud SQL's `private_network` setting (in `main.tf`) reuses whatever
Private Service Access peering already exists on the `default` network --
check for one before assuming you need to create it:
```bash
gcloud services vpc-peerings list --network=default --project=harsha-data-platform
```

Get the assigned private IP after `terraform apply`:
```bash
cd project-02-cloud-migration/infrastructure/terraform
terraform refresh
terraform output -raw private_ip_address
```

### PostgreSQL JDBC driver

Spark's JDBC reader needs the driver jar available in GCS to pass via
`--jars` (not bundled with Dataproc images by default, unlike the
spark-bigquery-connector, which is preinstalled):
```bash
curl -sL -o /tmp/postgresql-42.7.4.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

gsutil cp /tmp/postgresql-42.7.4.jar \
  gs://peakcart-dataproc-staging-2026/jars/postgresql-42.7.4.jar
```

### Submit the bulk migration job

```bash
gcloud dataproc jobs submit pyspark \
  project-02-cloud-migration/scripts/bulk_migrate.py \
  --cluster=peakcart-migration-cluster \
  --region=us-central1 \
  --project=harsha-data-platform \
  --jars=gs://peakcart-dataproc-staging-2026/jars/postgresql-42.7.4.jar \
  -- --project=harsha-data-platform --staging-bucket=peakcart-dataproc-staging-2026 \
     --db-host=<cloud-sql-private-ip>   # from `terraform output -raw private_ip_address`
```

Reads all 7 tables via JDBC from Cloud SQL's private IP directly and writes
each to `peakcart_migration_bronze.<table>` in BigQuery via the Spark
BigQuery connector's indirect-write path (needs `--staging-bucket` as
scratch space for that write).

The job fetches the DB password at runtime via `gcloud secrets versions
access` (Secret Manager) rather than taking it as a plaintext argument, so
it never shows up in job history/logs.

---

## Verification

```bash
bq query --use_legacy_sql=false "
SELECT 'suppliers' as t, COUNT(*) as n FROM \`harsha-data-platform.peakcart_migration_bronze.suppliers\`
UNION ALL SELECT 'customers', COUNT(*) FROM \`harsha-data-platform.peakcart_migration_bronze.customers\`
UNION ALL SELECT 'products', COUNT(*) FROM \`harsha-data-platform.peakcart_migration_bronze.products\`
UNION ALL SELECT 'orders', COUNT(*) FROM \`harsha-data-platform.peakcart_migration_bronze.orders\`
UNION ALL SELECT 'order_items', COUNT(*) FROM \`harsha-data-platform.peakcart_migration_bronze.order_items\`
UNION ALL SELECT 'inventory_snapshots', COUNT(*) FROM \`harsha-data-platform.peakcart_migration_bronze.inventory_snapshots\`
UNION ALL SELECT 'product_price_history', COUNT(*) FROM \`harsha-data-platform.peakcart_migration_bronze.product_price_history\`
ORDER BY t
"
```

Row counts confirmed matching source exactly: suppliers 20, customers 1000,
products 200, orders 5000 (post-dedup), order_items 15000,
inventory_snapshots 3000, product_price_history 359. Spot-checked actual
row values too (not just counts) -- e.g. `customers` row 1 matches the
source CSV byte-for-byte.

---

## Datastream (incremental CDC sync)

### One-time setup: logical replication + publication/slot

Applying the `cloudsql.logical_decoding=on` flag (in `main.tf`) restarts
the instance. After that, connect as the `postgres` admin user (via the
Auth Proxy) and run once:

```bash
source ~/.venv/migration-env/bin/activate
cd project-02-cloud-migration/infrastructure/terraform
PGPASSWORD=$(terraform output -raw postgres_admin_password) python3 -c "
import psycopg2, os
conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='peakcart_legacy', user='postgres', password=os.environ['PGPASSWORD'])
conn.autocommit = True
cur = conn.cursor()
cur.execute('ALTER ROLE postgres WITH REPLICATION;')
cur.execute('CREATE PUBLICATION datastream_publication FOR ALL TABLES;')
cur.execute(\"SELECT pg_create_logical_replication_slot('datastream_slot', 'pgoutput');\")
"
```

Also grant `postgres` SELECT on the tables (created by `peakcart_app`,
Cloud SQL's admin role doesn't bypass table ACLs):
```bash
PGPASSWORD=$(terraform output -raw db_password) python3 -c "
import psycopg2, os
conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='peakcart_legacy', user='peakcart_app', password=os.environ['PGPASSWORD'])
conn.autocommit = True
conn.cursor().execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;')
"
```

### Connectivity: IP allowlist, not private connectivity

Private connectivity (VPC peering) hit an unresolved transitive-routing
timeout -- see NOTES.md. Used IP-allowlist connectivity instead. Fetch
Datastream's static IPs for the region:
```bash
curl -s "https://datastream.googleapis.com/v1/projects/harsha-data-platform/locations/us-central1:fetchStaticIps" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)"
```
These are added as Cloud SQL authorized networks in `main.tf`.

### Verify the stream and simulate changes

```bash
gcloud datastream streams describe peakcart-migration-cdc-stream \
  --location=us-central1 --project=harsha-data-platform --format="yaml(state,errors)"

source ~/.venv/migration-env/bin/activate
export PEAKCART_LEGACY_DB_PASSWORD=$(cd project-02-cloud-migration/infrastructure/terraform && terraform output -raw db_password)
python3 project-02-cloud-migration/scripts/simulate_incremental_writes.py

# Wait a few minutes (data_freshness target), then check (note the
# Postgres-schema-prefixed table name Datastream auto-creates):
bq query --use_legacy_sql=false \
  "SELECT order_id, status, total_amount FROM \`harsha-data-platform.peakcart_migration_cdc.public_orders\` ORDER BY order_id"
```

---

## Cloud Composer (ephemeral-cluster migration DAG)

### One-time setup (if the verification SA/environment don't already exist)

```bash
gcloud iam service-accounts create peakcart-composer-verify \
  --project=harsha-data-platform \
  --display-name="Temporary Composer verification environment SA"

for ROLE in roles/composer.worker roles/bigquery.dataEditor roles/bigquery.jobUser roles/dataproc.editor; do
  gcloud projects add-iam-policy-binding harsha-data-platform \
    --member="serviceAccount:peakcart-composer-verify@harsha-data-platform.iam.gserviceaccount.com" \
    --role="${ROLE}" --condition=None
done

# Composer's SA needs permission to launch a cluster running as a
# *different* SA (the Dataproc cluster's own peakcart-dataproc-migration SA)
gcloud iam service-accounts add-iam-policy-binding \
  peakcart-dataproc-migration@harsha-data-platform.iam.gserviceaccount.com \
  --member="serviceAccount:peakcart-composer-verify@harsha-data-platform.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser" --project=harsha-data-platform

# One-time per-project grant (if not already present)
gcloud projects add-iam-policy-binding harsha-data-platform \
  --member="serviceAccount:service-435348575003@cloudcomposer-accounts.iam.gserviceaccount.com" \
  --role="roles/composer.ServiceAgentV2Ext" --condition=None
```

### Create, upload the DAG, and trigger a run

```bash
gcloud composer environments create peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 \
  --environment-size=small --image-version=composer-2-airflow-2 \
  --service-account=peakcart-composer-verify@harsha-data-platform.iam.gserviceaccount.com \
  --async
# Takes ~20-25 minutes.

DAG_BUCKET=$(gcloud composer environments describe peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 --format='value(config.dagGcsPrefix)')
gsutil cp project-02-cloud-migration/dags/project02_migration_dag.py "${DAG_BUCKET}/"

# Also stage the PySpark script + JDBC jar the DAG's job references:
gsutil cp project-02-cloud-migration/scripts/bulk_migrate.py \
  gs://peakcart-dataproc-staging-2026/pyspark/bulk_migrate.py

gcloud composer environments run peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 \
  dags trigger -- project02_migration_bulk_load
```

### Check status / logs

```bash
gcloud composer environments run peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 \
  tasks states-for-dag-run -- project02_migration_bulk_load <execution_date>

# Cross-check against the real Dataproc job state directly -- Airflow's
# own status polling can lag several minutes behind reality:
gcloud dataproc jobs list --region=us-central1 --project=harsha-data-platform --limit=5

# Re-run just one failed task after a fix, without redoing already-
# successful work:
gcloud composer environments run peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1 \
  tasks clear -- project02_migration_bulk_load -t <task_id> \
  -s <execution_date> -e <execution_date> --yes
```

### Tear down

```bash
gcloud composer environments delete peakcart-composer-verify \
  --project=harsha-data-platform --location=us-central1
```

---

## Full teardown

```bash
cd project-02-cloud-migration/infrastructure/terraform

terraform destroy \
  -target=google_datastream_stream.migration \
  -target=google_datastream_connection_profile.postgres_source \
  -target=google_datastream_connection_profile.bigquery_dest \
  -target=google_dataproc_cluster.migration

# Cloud SQL: Terraform's destroy graph tries to drop the SQL user before
# the instance, which fails on real Postgres ownership semantics (the
# user still owns tables). Delete the instance directly instead -- this
# wipes everything atomically -- then reconcile Terraform state:
gcloud sql instances delete peakcart-legacy-postgres --project=harsha-data-platform

terraform state rm google_sql_database_instance.legacy_postgres \
  google_sql_database.peakcart_legacy google_sql_user.peakcart_app \
  google_sql_user.postgres_admin random_password.postgres_admin_password

# Kill the local Auth Proxy if still running
ps aux | grep cloud-sql-proxy
```

BigQuery datasets, service accounts, the Secret Manager secret, and the
staging bucket are left in place -- no ongoing cost, and they're the
actual reusable deliverable (same pattern as every other project here).
