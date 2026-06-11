#!/bin/bash
set -e

PROJECT_ID="harsha-data-platform"
STATE_BUCKET="peakcart-terraform-state-2026"
REGION="US-CENTRAL1"

echo "Creating Terraform state bucket..."

gcloud storage buckets create gs://${STATE_BUCKET} \
  --project=${PROJECT_ID} \
  --location=${REGION} \
  --uniform-bucket-level-access

gcloud storage buckets update gs://${STATE_BUCKET} \
  --versioning

echo "State bucket created: gs://${STATE_BUCKET}"
echo "Versioning enabled."
echo ""
echo "Next step: run terraform init"
