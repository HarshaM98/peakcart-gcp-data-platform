output "dataset_id" {
  description = "The BigQuery dataset ID"
  value       = google_bigquery_dataset.this.dataset_id
}

output "self_link" {
  description = "The URI of the dataset"
  value       = google_bigquery_dataset.this.self_link
}
