output "data_lake_bucket" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "bronze_dataset_id" {
  description = "BigQuery bronze dataset ID"
  value       = module.bronze.dataset_id
}

output "silver_dataset_id" {
  description = "BigQuery silver dataset ID"
  value       = module.silver.dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery gold dataset ID"
  value       = module.gold.dataset_id
}

output "snapshots_dataset_id" {
  description = "BigQuery snapshots dataset ID"
  value       = module.snapshots.dataset_id
}
