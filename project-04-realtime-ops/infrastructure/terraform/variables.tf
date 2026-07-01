variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "harsha-data-platform"
}

variable "region" {
  description = "GCP region for provider default"
  type        = string
  default     = "us-central1"
}

variable "common_labels" {
  description = "Labels applied to all Pub/Sub resources in this project"
  type        = map(string)
  default = {
    project = "project04"
    domain  = "fulfillment"
  }
}
