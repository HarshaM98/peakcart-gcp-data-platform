terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.45.2"
    }
  }

  backend "gcs" {
    bucket = "peakcart-terraform-state-2026"
    prefix = "project-05/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Needed for Vertex AI Model Registry integration (BQML's
# model_registry='vertex_ai' option) and for deploying endpoints.
resource "google_project_service" "aiplatform" {
  project            = var.project_id
  service            = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

# Bronze layer for project-05's supply-chain data (product_demand_daily,
# inventory_daily) -- raw CSVs loaded as-is, same medallion pattern as
# project-01. Reuses project-01's bigquery_dataset module rather than
# duplicating it.
module "bronze" {
  source      = "../../../project-01-data-warehouse/infrastructure/terraform/modules/bigquery_dataset"
  dataset_id  = "peakcart_supply_chain_bronze"
  description = "Raw supply-chain simulation data (demand + inventory), loaded as-is from CSV."
  location    = var.region
  labels = {
    project = "project05"
    domain  = "supply_chain_ml"
    layer   = "bronze"
  }
  writers = var.dataset_writers
}
