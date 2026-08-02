variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "harsha-data-platform"
}

variable "region" {
  description = "GCP region for provider default and Dataplex/BigQuery resource location"
  type        = string
  default     = "us-central1"
}
