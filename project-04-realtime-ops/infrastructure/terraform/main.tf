terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.45.2"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.45.2"
    }
  }

  backend "gcs" {
    bucket = "peakcart-terraform-state-2026"
    prefix = "project-04/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Fetches the project's numeric ID immediately (known at plan time),
# not deferred like a resource output would be.
data "google_project" "current" {
  project_id = var.project_id
}

locals {
  # The Pub/Sub service agent email follows a fixed, documented Google
  # pattern. Computing it here avoids depending on google_project_service_identity's
  # output, which is unknown until apply and was forcing our IAM bindings
  # to be destroyed and recreated unnecessarily.
  pubsub_service_agent_email = "service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Ensures the service identity formally exists. Idempotent: safe to "create"
# even though it already exists, and cannot be imported (see note in chat history).
resource "google_project_service_identity" "pubsub" {
  provider = google-beta
  project  = var.project_id
  service  = "pubsub.googleapis.com"
}

module "order_events" {
  source                       = "./modules/pubsub_topic_with_dlq"
  project_id                   = var.project_id
  topic_name                   = "peakcart-order-events"
  max_delivery_attempts        = 5
  enable_exactly_once_delivery = true
  labels                       = var.common_labels
  pubsub_service_agent_email   = local.pubsub_service_agent_email
}

module "delivery_events" {
  source                       = "./modules/pubsub_topic_with_dlq"
  project_id                   = var.project_id
  topic_name                   = "peakcart-delivery-events"
  max_delivery_attempts        = 5
  enable_exactly_once_delivery = true
  labels                       = var.common_labels
  pubsub_service_agent_email   = local.pubsub_service_agent_email
}

module "inventory_events" {
  source                       = "./modules/pubsub_topic_with_dlq"
  project_id                   = var.project_id
  topic_name                   = "peakcart-inventory-events"
  max_delivery_attempts        = 5
  enable_exactly_once_delivery = true
  labels                       = var.common_labels
  pubsub_service_agent_email   = local.pubsub_service_agent_email
}

# --- Dataflow deployment: staging bucket, worker identity, and the roles it needs ---

resource "google_project_service" "dataflow" {
  project            = var.project_id
  service            = "dataflow.googleapis.com"
  disable_on_destroy = false
}

# Dataflow needs somewhere to stage the pipeline's serialized graph and
# temp shuffle/side-input data -- distinct from peakcart-data-lake-2026,
# which holds actual dataset content, not job-run scratch space.
resource "google_storage_bucket" "dataflow_staging" {
  name                        = "peakcart-dataflow-staging-2026"
  location                    = var.region
  force_destroy               = true # scratch space only, safe to force-delete
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  labels = var.common_labels
}

# Dedicated worker identity rather than the default Compute Engine service
# account, so this pipeline's GCP permissions are scoped to exactly what it
# needs (pull these 3 subscriptions, write these BigQuery tables, use this
# staging bucket) instead of inheriting the project's default broad grants.
resource "google_service_account" "dataflow_worker" {
  account_id   = "peakcart-dataflow-worker"
  display_name = "Dataflow worker for project-04 streaming pipeline"
}

resource "google_project_iam_member" "dataflow_worker_role" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "dataflow_worker_pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "dataflow_worker_pubsub_viewer" {
  project = var.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "dataflow_worker_bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "dataflow_worker_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_worker_staging_access" {
  bucket = google_storage_bucket.dataflow_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}
