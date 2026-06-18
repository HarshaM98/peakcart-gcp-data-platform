![dbt CI](https://github.com/HarshaM98/peakcart-gcp-data-platform/actions/workflows/dbt-ci.yml/badge.svg)

# Project 1: PeakCart Data Warehouse

A production-grade analytical data warehouse built on Google Cloud Platform
using the Medallion Architecture (Bronze, Silver, Gold). This project
demonstrates end-to-end data engineering from raw CSV ingestion through to
a star schema optimised for business intelligence.

---

## Architecture

GCS (raw CSVs)
│ load_bronze.sh (bq load + stage-and-replace)
▼
BigQuery Bronze raw data, no transformations, all columns NULLABLE
│ dbt staging models (views)
▼
BigQuery Silver cleaned, typed, deduplicated, quality-flagged
│ dbt mart models (tables, incremental)
▼
BigQuery Gold star schema, facts and dimensions, BI-ready

### Star Schema

| Table                | Grain                                     | Rows   | Type         |
| -------------------- | ----------------------------------------- | ------ | ------------ |
| fact_orders          | one row per order item                    | 14,800 | incremental  |
| fact_daily_inventory | one row per product per warehouse per day | 3,000  | incremental  |
| dim_customers        | one row per customer version              | 1,000  | SCD Type 2   |
| dim_products         | one row per product price period          | 359    | SCD Type 2   |
| dim_dates            | one row per calendar date                 | 1,461  | static table |
| dim_suppliers        | one row per supplier                      | 20     | SCD Type 1   |

---

## Technologies

| Tool                 | Purpose                                                     |
| -------------------- | ----------------------------------------------------------- |
| Google Cloud Storage | Data lake, raw CSV storage with lifecycle policies          |
| BigQuery             | Data warehouse, all three Medallion layers                  |
| dbt Core 1.11        | All transformations, testing, documentation                 |
| dbt_utils 1.3.0      | Surrogate key generation                                    |
| Python 3.11          | Sample data generation                                      |
| Google Cloud SDK     | GCS upload, BigQuery load jobs                              |
| Terraform 1.15       | Infrastructure as code for GCS bucket and BigQuery datasets |
| GitHub Actions       | CI/CD pipeline running dbt build on every push              |

---

## Key Design Decisions

### SCD Type 2 on dim_products via price history table

Products have a `product_price_history` source table logging every price
change with effective dates. The staging model converts NULL end dates to
a `9999-12-31` sentinel value enabling clean BETWEEN joins. fact_orders
joins on both `product_id` and `order_date BETWEEN valid_from AND valid_to`,
linking every transaction to the price active at purchase time.

A `price_variance` column (current price minus purchase price) is computed
on the fact table, enabling pricing drift analysis across categories and time.

Without SCD Type 2, this analysis is impossible.

### SCD Type 2 on dim_customers via dbt snapshots

Customer segment, city, and state changes are tracked using dbt snapshots.
On every pipeline run, dbt compares current source data to the snapshot table
and automatically versions any changed rows with `dbt_valid_from` and
`dbt_valid_to` timestamps.

This ensures that revenue attributed to a customer segment reflects the
segment the customer was in at the time of purchase, not their current segment.

### Incremental fact tables with 3-day lookback

Both fact tables use incremental materialization. On the first run, dbt
builds the full table. On subsequent runs, only rows within a 3-day lookback
window are processed, catching late-arriving data without reprocessing the
entire table.

A full rebuild is triggered with `dbt run --full-refresh` when schema
changes occur or data corrections are needed.

### Stage-and-replace pattern for metadata columns

`bq load` does not support computed columns. The Bronze load script uses a
two-step pattern: load the CSV into a staging table, then CREATE OR REPLACE
TABLE using SELECT \* plus `CURRENT_TIMESTAMP()` as `_loaded_at` and the GCS
path as `_source_file`. The staging table is dropped after.

In production (Project 2), a Dataflow pipeline adds metadata before the data
reaches BigQuery, eliminating this workaround.

### Schema JSON files contain source columns only

Metadata columns (`_loaded_at`, `_source_file`) are not in the schema JSON
files. They are added by `load_bronze.sh` via the stage-and-replace pattern.
This keeps schema files as a faithful representation of the source system.

---

## Data Quality

92 dbt tests across all models:

| Layer              | Tests | Coverage                                               |
| ------------------ | ----- | ------------------------------------------------------ |
| Silver (7 models)  | 46    | primary keys, not_null, accepted_values, relationships |
| Gold (6 models)    | 44    | surrogate keys, foreign keys, accepted_values          |
| Singular (2 tests) | 2     | positive line totals (warn), valid price periods       |

Known data quality issues from the source system (intentionally seeded):

- 2% of customers have NULL emails (flagged by `is_valid_email`)
- 2% of products have NULL supplier_id (flagged by `has_product_id`)
- 0.5% of order items have negative quantities representing returns
  (flagged by `is_positive_quantity`, test configured as warning)

---

## How to Run

### Prerequisites

```bash
# Activate dbt virtualenv
dbt-activate

# Verify connection
cd project-01-data-warehouse/dbt/peakcart_dbt
dbt debug
```

### Setting up profiles.yml

```bash
# Copy the example file and fill in your GCP project ID
cp profiles.yml.example ~/.dbt/profiles.yml

# Edit with your project ID
code ~/.dbt/profiles.yml
```

### Full pipeline run

```bash
# 1. Generate sample data (from project root)
python3.11 shared/data-generators/generate_peakcart_data.py

# 2. Upload to GCS and load Bronze layer
bash project-01-data-warehouse/infrastructure/load_bronze.sh

# 3. Run dbt snapshot (SCD Type 2 customer history)
dbt snapshot

# 4. Run all dbt models
dbt run

# 5. Run all tests
dbt test

# 6. Generate and view documentation
dbt docs generate && dbt docs serve
```

### Incremental run (daily)

```bash
dbt snapshot
dbt run
dbt test
```

### Full rebuild (after schema changes)

```bash
dbt run --full-refresh
dbt test
```

---

## Infrastructure

All GCP infrastructure is managed as Terraform code.

```bash
cd project-01-data-warehouse/infrastructure/terraform

# First time only: create remote state bucket
./bootstrap.sh

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply changes
terraform apply
```

Resources managed:

- GCS bucket `peakcart-data-lake-2026` with lifecycle policies
- BigQuery datasets: `peakcart_bronze`, `peakcart_silver`, `peakcart_gold`, `peakcart_snapshots`
- IAM bindings for each dataset
- Remote state stored in `gs://peakcart-terraform-state-2026`

---

## Repository Structure

project-01-data-warehouse/
infrastructure/
load_bronze.sh Bronze layer load script
schemas/ BigQuery table schema JSON files
lifecycle-policy.json GCS lifecycle rules
dbt/
peakcart_dbt/
models/
staging/ Silver layer (7 views)
marts/
dimensions/ Gold dimensions (4 tables)
facts/ Gold facts (2 incremental tables)
snapshots/ SCD Type 2 customer history
tests/ Singular business rule tests
macros/ generate_schema_name override
advanced-sql/ 5 interview-ready analytical queries
01_rfm_segmentation.sql
02_rolling_revenue.sql
03_cohort_analysis.sql
04_inventory_anomaly.sql
05_merge_late_arriving.sql
shared/
data-generators/
generate_peakcart_data.py generates all sample CSVs
diagrams/
project-01-lineage-full-dag.png
project-01-lineage-fact-orders.png

---

## Advanced SQL Queries

Five production-grade analytical queries built against the Gold layer:

| Query             | Technique                  | Business Use                   |
| ----------------- | -------------------------- | ------------------------------ |
| RFM Segmentation  | NTILE window functions     | Customer value scoring         |
| Rolling Revenue   | ROWS BETWEEN window frames | Revenue trend analysis         |
| Cohort Analysis   | COUNT DISTINCT CASE WHEN   | Customer retention measurement |
| Inventory Anomaly | LAG + Z-score              | Supply chain anomaly detection |
| MERGE Upsert      | BigQuery MERGE statement   | Late-arriving data handling    |

---

## Access Control Recommendations

For production deployment:

- `data_engineer` role: read/write Bronze and Silver, read Gold
- `analyst` role: read Gold only
- `marketing` role: read Gold customer aggregates, PII columns masked
- `ml_engineer` role: read feature tables, no PII access

Column-level security on PII fields (email, address) implemented in
Project 6 (Governance and AI).

---

## Production Considerations

Things deliberately simplified in this portfolio that a production deployment would handle differently:

**Infrastructure**

- Terraform workspaces separating dev, staging, and prod environments
- Separate GCP projects per environment to isolate billing and access
- Terraform runs via CI/CD pipeline rather than manually from a laptop

**Ingestion**

- Dataflow pipeline replacing the `load_bronze.sh` script, adding metadata before data reaches BigQuery rather than via the stage-and-replace workaround
- CDC via Datastream for near-real-time ingestion from operational databases

**Transformation**

- dbt Cloud or Cloud Composer for scheduled pipeline runs rather than manual execution
- Slim CI using `state:modified+` already implemented via GitHub Actions

**Security**

- Column-level security on PII fields via BigQuery policy tags (covered in Project 6)
- Secret Manager for all credentials rather than environment variables
- Separate service accounts per pipeline with least-privilege permissions

**Snapshots**

- dbt snapshots initialized before any historical data exists, so `dbt_valid_from` reflects true history rather than the snapshot run date
- In this portfolio, `valid_from` was backdated via a direct BigQuery UPDATE to align with order history start date

_Part of the PeakCart GCP Data Platform portfolio._
_Built by Harsha Manjunatha | June 2026 | harsha-data-platform (GCP)_
