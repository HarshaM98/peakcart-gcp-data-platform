terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "peakcart-terraform-state-2026"
    prefix = "project-01/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "bronze" {
  source      = "./modules/bigquery_dataset"
  dataset_id  = "peakcart_bronze"
  description = "Raw data layer. Data loaded exactly as received from source systems."
  location    = var.region
  labels = {
    layer = "bronze"
    env   = "dev"
  }
  writers = ["user:harsha.manjunatha98@gmail.com"]
}

module "silver" {
  source      = "./modules/bigquery_dataset"
  dataset_id  = "peakcart_silver"
  description = "Cleaned and typed data. dbt staging models as views."
  location    = var.region
  labels = {
    layer = "silver"
    env   = "dev"
  }
  writers = ["user:harsha.manjunatha98@gmail.com"]
}

module "gold" {
  source      = "./modules/bigquery_dataset"
  dataset_id  = "peakcart_gold"
  description = "Business-ready star schema. dbt mart models as tables."
  location    = var.region
  labels = {
    layer = "gold"
    env   = "dev"
  }
  writers = ["user:harsha.manjunatha98@gmail.com"]
}

module "snapshots" {
  source      = "./modules/bigquery_dataset"
  dataset_id  = "peakcart_snapshots"
  description = "SCD Type 2 snapshot history tables managed by dbt snapshot."
  location    = var.region
  labels = {
    layer = "snapshots"
    env   = "dev"
  }
  writers = ["user:harsha.manjunatha98@gmail.com"]
}

resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["raw/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["raw/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age            = 365
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }
}
