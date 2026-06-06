#!/bin/bash
# =============================================================================
# PeakCart Bronze Layer Loader
# Loads raw CSV files from GCS into BigQuery Bronze tables
# Bronze tables contain raw data exactly as it arrived, no transformations
# Metadata columns (_loaded_at, _source_file) are added in Silver layer by dbt
# Usage: bash project-01-data-warehouse/infrastructure/load_bronze.sh
# =============================================================================

set -e

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT="harsha-data-platform"
DATASET="peakcart_bronze"
GCS_PATH="gs://peakcart-data-lake-2026/raw/2026/06/04"
SCHEMA_DIR="project-01-data-warehouse/infrastructure/schemas"

# ── Helper function ────────────────────────────────────────────────────────────
load_table() {
  local TABLE=$1
  local FILE=$2

  echo ""
  echo "Loading ${TABLE}..."

  # Step 1: Load CSV into a temporary staging table
  bq load \
    --project_id="${PROJECT}" \
    --source_format=CSV \
    --schema="${SCHEMA_DIR}/${TABLE}.json" \
    --skip_leading_rows=1 \
    --replace \
    "${DATASET}.${TABLE}_stage" \
    "${GCS_PATH}/${FILE}"

  # Step 2: Create final table with metadata columns from staging
  bq query \
    --project_id="${PROJECT}" \
    --use_legacy_sql=false \
    "CREATE OR REPLACE TABLE \`${PROJECT}.${DATASET}.${TABLE}\` AS
     SELECT
       *,
       CURRENT_TIMESTAMP()        AS _loaded_at,
       '${GCS_PATH}/${FILE}'      AS _source_file
     FROM \`${PROJECT}.${DATASET}.${TABLE}_stage\`"

  # Step 3: Drop the staging table
  bq rm -f \
    --project_id="${PROJECT}" \
    "${DATASET}.${TABLE}_stage"

  echo "${TABLE} done."
}

# ── Load all tables ────────────────────────────────────────────────────────────
echo "Starting PeakCart Bronze layer load..."
echo "Project:   ${PROJECT}"
echo "Dataset:   ${DATASET}"
echo "GCS path:  ${GCS_PATH}"
echo ""

load_table "bronze_suppliers"            "suppliers.csv"
load_table "bronze_products"             "products.csv"
load_table "bronze_customers"            "customers.csv"
load_table "bronze_orders"               "orders.csv"
load_table "bronze_order_items"          "order_items.csv"
load_table "bronze_inventory_snapshots"  "inventory_snapshots.csv"
load_table "bronze_product_price_history" "product_price_history.csv"

echo ""
echo "All Bronze tables loaded successfully."
echo "Next step: run dbt Silver models to add metadata and clean data."