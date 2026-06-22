#!/bin/bash
# =============================================================================
# PeakCart Project 03 - Bronze Layer Load Script
# Loads four source CSVs from GCS into BigQuery Bronze
# Uses stage-and-replace pattern (same as Project 1)
#
# Usage: bash project-03-customer-360/infrastructure/load_bronze.sh
# =============================================================================

set -e  # Exit immediately if any command fails

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT="harsha-data-platform"
DATASET="peakcart_bronze"
GCS_PATH="gs://peakcart-data-lake-2026/project-03/raw/2025"
SCHEMA_DIR="project-03-customer-360/infrastructure/schemas"

echo "============================================================"
echo "PeakCart Project 03 - Bronze Load"
echo "Project:  $PROJECT"
echo "Dataset:  $DATASET"
echo "GCS Path: $GCS_PATH"
echo "============================================================"

# ── Helper function ───────────────────────────────────────────────────────────
# Runs the stage-and-replace pattern for one table.
# Arguments:
#   $1 = table name (e.g. bronze_customer_profiles)
#   $2 = CSV filename in GCS (e.g. customer_profiles.csv)
#   $3 = schema JSON filename (e.g. bronze_customer_profiles.json)

load_table() {
  local TABLE=$1
  local CSV_FILE=$2
  local SCHEMA_FILE=$3
  local STAGE_TABLE="${TABLE}_stage"

  echo ""
  echo "Loading: $TABLE"
  echo "  Source:  $GCS_PATH/$CSV_FILE"
  echo "  Schema:  $SCHEMA_DIR/$SCHEMA_FILE"

  # Step 1: Load CSV into stage table (source columns only)
  echo "  Step 1: Loading into stage table..."
  bq load \
    --project_id="$PROJECT" \
    --source_format=CSV \
    --schema="$SCHEMA_DIR/$SCHEMA_FILE" \
    --skip_leading_rows=1 \
    --replace \
    "$DATASET.$STAGE_TABLE" \
    "$GCS_PATH/$CSV_FILE"

  # Step 2: Create final table with metadata columns added
  echo "  Step 2: Creating final table with metadata columns..."
  bq query \
    --project_id="$PROJECT" \
    --use_legacy_sql=false \
    "
    CREATE OR REPLACE TABLE \`$PROJECT.$DATASET.$TABLE\` AS
    SELECT
      *,
      CURRENT_TIMESTAMP()  AS _loaded_at,
      '$GCS_PATH/$CSV_FILE' AS _source_file
    FROM \`$PROJECT.$DATASET.$STAGE_TABLE\`
    "

  # Step 3: Drop stage table (cleanup)
  echo "  Step 3: Dropping stage table..."
  bq rm -f "$PROJECT:$DATASET.$STAGE_TABLE"

  # Step 4: Verify row count
  echo "  Step 4: Verifying row count..."
  bq query \
    --project_id="$PROJECT" \
    --use_legacy_sql=false \
    --format=prettyjson \
    "SELECT COUNT(*) AS row_count FROM \`$PROJECT.$DATASET.$TABLE\`"

  echo "  Done: $TABLE"
}

# ── Load all four tables ──────────────────────────────────────────────────────

load_table \
  "bronze_customer_profiles" \
  "customer_profiles.csv" \
  "bronze_customer_profiles.json"

load_table \
  "bronze_order_history" \
  "order_history.csv" \
  "bronze_order_history.json"

load_table \
  "bronze_clickstream_events" \
  "clickstream_events.csv" \
  "bronze_clickstream_events.json"

load_table \
  "bronze_delivery_feedback" \
  "delivery_feedback.csv" \
  "bronze_delivery_feedback.json"

# ── Final verification ────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "All tables loaded. Final row counts:"
echo "============================================================"

bq query \
  --project_id="$PROJECT" \
  --use_legacy_sql=false \
  "
  SELECT 'bronze_customer_profiles'  AS table_name,
         COUNT(*) AS row_count
  FROM \`$PROJECT.$DATASET.bronze_customer_profiles\`
  UNION ALL
  SELECT 'bronze_order_history',      COUNT(*)
  FROM \`$PROJECT.$DATASET.bronze_order_history\`
  UNION ALL
  SELECT 'bronze_clickstream_events', COUNT(*)
  FROM \`$PROJECT.$DATASET.bronze_clickstream_events\`
  UNION ALL
  SELECT 'bronze_delivery_feedback',  COUNT(*)
  FROM \`$PROJECT.$DATASET.bronze_delivery_feedback\`
  ORDER BY table_name
  "

echo ""
echo "Bronze load complete. Next step: dbt staging models."
