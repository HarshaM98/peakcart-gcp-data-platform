output "governance_function_service_account" {
  description = "Service account email for the anomaly-explainer Cloud Function"
  value       = google_service_account.governance_function.email
}

output "datascan_ids" {
  description = "Data quality DataScan IDs created for the gold layer"
  value = [
    google_dataplex_datascan.fact_orders_quality.data_scan_id,
    google_dataplex_datascan.dim_customers_quality.data_scan_id,
    google_dataplex_datascan.dim_products_quality.data_scan_id,
    google_dataplex_datascan.fact_daily_inventory_quality.data_scan_id,
  ]
}
