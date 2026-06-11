resource "google_bigquery_dataset" "this" {
  dataset_id  = var.dataset_id
  location    = var.location
  description = var.description
  labels      = var.labels

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset_iam_member" "readers" {
  for_each   = toset(var.readers)
  dataset_id = google_bigquery_dataset.this.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

resource "google_bigquery_dataset_iam_member" "writers" {
  for_each   = toset(var.writers)
  dataset_id = google_bigquery_dataset.this.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = each.value
}
