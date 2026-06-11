variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "US-CENTRAL1"
}

variable "bucket_name" {
  description = "GCS data lake bucket name"
  type        = string
  default     = "peakcart-data-lake-2026"
}
