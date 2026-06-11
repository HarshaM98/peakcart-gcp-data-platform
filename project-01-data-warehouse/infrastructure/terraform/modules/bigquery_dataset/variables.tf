variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "description" {
  description = "Human readable description of this dataset"
  type        = string
}

variable "location" {
  description = "GCP region for this dataset"
  type        = string
}

variable "labels" {
  description = "Labels to apply to this dataset"
  type        = map(string)
  default     = {}
}

variable "readers" {
  description = "List of IAM members who can read this dataset"
  type        = list(string)
  default     = []
}

variable "writers" {
  description = "List of IAM members who can write to this dataset"
  type        = list(string)
  default     = []
}
