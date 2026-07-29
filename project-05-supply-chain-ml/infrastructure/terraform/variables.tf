variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "harsha-data-platform"
}

variable "region" {
  description = "GCP region for provider default and BigQuery dataset location"
  type        = string
  default     = "us-central1"
}

variable "dataset_writers" {
  description = "List of IAM members who can write to the bronze dataset"
  type        = list(string)
  default     = ["user:harsha.manjunatha98@gmail.com"]
}
