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

# Resolves (or creates, if missing) the auto-generated Pub/Sub service agent
# for this project. Used by every module call below for dead-letter IAM
# bindings, instead of hardcoding the service account email.
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
  pubsub_service_agent_email   = google_project_service_identity.pubsub.email
}

module "delivery_events" {
  source                       = "./modules/pubsub_topic_with_dlq"
  project_id                   = var.project_id
  topic_name                   = "peakcart-delivery-events"
  max_delivery_attempts        = 5
  enable_exactly_once_delivery = true
  labels                       = var.common_labels
  pubsub_service_agent_email   = google_project_service_identity.pubsub.email
}

module "inventory_events" {
  source                       = "./modules/pubsub_topic_with_dlq"
  project_id                   = var.project_id
  topic_name                   = "peakcart-inventory-events"
  max_delivery_attempts        = 5
  enable_exactly_once_delivery = true
  labels                       = var.common_labels
  pubsub_service_agent_email   = google_project_service_identity.pubsub.email
}
