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
