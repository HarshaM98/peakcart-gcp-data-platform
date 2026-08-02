terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.45.2"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  backend "gcs" {
    bucket = "peakcart-terraform-state-2026"
    prefix = "project-06/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "dataplex" {
  project            = var.project_id
  service            = "dataplex.googleapis.com"
  disable_on_destroy = false
}

# Gemini API access (via Vertex AI's generative models) for the anomaly
# explainer Cloud Function.
resource "google_project_service" "aiplatform" {
  project            = var.project_id
  service            = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudfunctions" {
  project            = var.project_id
  service            = "cloudfunctions.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "run" {
  project            = var.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "eventarc" {
  project            = var.project_id
  service            = "eventarc.googleapis.com"
  disable_on_destroy = false
}

# A Dataplex Lake is the top-level governance container; a Zone within it
# groups data by curation level. Both are required before a BigQuery
# dataset can be registered as a governed Asset.
resource "google_dataplex_lake" "supply_chain" {
  location = var.region
  name     = "peakcart-governance-lake"
  project  = var.project_id

  labels = {
    project = "project06"
    domain  = "governance_ai"
  }

  depends_on = [google_project_service.dataplex]
}

resource "google_dataplex_zone" "gold_curated" {
  location = var.region
  lake     = google_dataplex_lake.supply_chain.name
  name     = "gold-curated-zone"
  project  = var.project_id

  type = "CURATED"

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  discovery_spec {
    enabled = false
  }
}

# Registers the existing peakcart_gold BigQuery dataset (built in
# project-01) as a governed asset -- reused as-is, not duplicated.
resource "google_dataplex_asset" "gold_dataset" {
  location      = var.region
  lake          = google_dataplex_lake.supply_chain.name
  dataplex_zone = google_dataplex_zone.gold_curated.name
  name          = "peakcart-gold-dataset"
  project       = var.project_id

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/peakcart_gold"
    type = "BIGQUERY_DATASET"
  }
}

# Service account for the anomaly-explainer Cloud Function: needs to read
# Dataplex DataScan job results and call the Gemini API via Vertex AI.
resource "google_service_account" "governance_function" {
  project      = var.project_id
  account_id   = "peakcart-governance-function"
  display_name = "peakcart-governance-function (project-06 Cloud Function)"
}

resource "google_project_iam_member" "governance_function_dataplex_viewer" {
  project = var.project_id
  role    = "roles/dataplex.viewer"
  member  = "serviceAccount:${google_service_account.governance_function.email}"
}

resource "google_project_iam_member" "governance_function_aiplatform_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.governance_function.email}"
}

resource "google_project_iam_member" "governance_function_bq_data_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.governance_function.email}"
}

resource "google_project_iam_member" "governance_function_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.governance_function.email}"
}

# dataplex.viewer alone can't call DataScan:run -- the function needs to
# trigger the scan itself, not just read prior results.
resource "google_project_iam_member" "governance_function_dataplex_datascan_admin" {
  project = var.project_id
  role    = "roles/dataplex.dataScanAdmin"
  member  = "serviceAccount:${google_service_account.governance_function.email}"
}

resource "google_project_service" "cloudbuild" {
  project            = var.project_id
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  project            = var.project_id
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudscheduler" {
  project            = var.project_id
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

# Function source bucket. Short lifecycle since only the latest deployed
# source archive matters -- same reasoning as the Dataflow/Vertex Pipelines
# staging buckets elsewhere in this portfolio.
resource "google_storage_bucket" "function_source" {
  name                        = "peakcart-governance-function-source-2026"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 14
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project = "project06"
    domain  = "governance_ai"
    purpose = "function_source"
  }
}

data "archive_file" "anomaly_explainer_source" {
  type        = "zip"
  source_dir  = "${path.module}/../../functions/anomaly_explainer"
  output_path = "${path.module}/.terraform-tmp/anomaly_explainer.zip"
}

resource "google_storage_bucket_object" "anomaly_explainer_source" {
  name   = "anomaly_explainer-${data.archive_file.anomaly_explainer_source.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.anomaly_explainer_source.output_path
}

resource "google_cloudfunctions2_function" "anomaly_explainer" {
  name     = "peakcart-governance-anomaly-explainer"
  location = var.region
  project  = var.project_id

  build_config {
    runtime     = "python311"
    entry_point = "run_governance_report"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.anomaly_explainer_source.name
      }
    }
  }

  service_config {
    available_memory      = "512Mi"
    available_cpu         = "1"
    timeout_seconds       = 480
    max_instance_count    = 3
    service_account_email = google_service_account.governance_function.email
    ingress_settings      = "ALLOW_ALL"
  }

  depends_on = [
    google_project_service.cloudfunctions,
    google_project_service.run,
    google_project_service.cloudbuild,
    google_project_service.artifactregistry,
  ]
}

# Requires authentication -- only Cloud Scheduler's own service account
# (and, for manual verification, the operator) can invoke it.
resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  location = var.region
  project  = var.project_id
  name     = google_cloudfunctions2_function.anomaly_explainer.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.governance_scheduler.email}"
}

resource "google_cloud_run_v2_service_iam_member" "operator_invoker" {
  location = var.region
  project  = var.project_id
  name     = google_cloudfunctions2_function.anomaly_explainer.name
  role     = "roles/run.invoker"
  member   = "user:harsha.manjunatha98@gmail.com"
}

resource "google_service_account" "governance_scheduler" {
  project      = var.project_id
  account_id   = "peakcart-governance-scheduler"
  display_name = "peakcart-governance-scheduler (Cloud Scheduler caller)"
}

# Created but left PAUSED by default -- verify with one manual/bounded
# run, same cost discipline as every scheduled resource in this portfolio
# (see project-05's bounded PipelineJobSchedule).
resource "google_cloud_scheduler_job" "governance_report_daily" {
  name      = "peakcart-governance-report-daily"
  project   = var.project_id
  region    = var.region
  schedule  = "0 6 * * *"
  time_zone = "Etc/UTC"
  paused    = true

  http_target {
    uri         = google_cloudfunctions2_function.anomaly_explainer.url
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.governance_scheduler.email
    }
  }

  depends_on = [google_project_service.cloudscheduler]
}

# ---------------------------------------------------------------------------
# Data quality DataScans -- one per gold table, each targeting a real gap
# found by reading the actual dbt SQL rather than assuming the gold layer
# is clean (see README "Key Technical Decisions" for the full reasoning).
# Run on-demand (triggered manually / by the Cloud Function's caller), not
# on a schedule, to keep this cost-bounded like every other project here.
# ---------------------------------------------------------------------------

resource "google_dataplex_datascan" "fact_orders_quality" {
  location     = var.region
  data_scan_id = "fact-orders-quality"
  project      = var.project_id

  data {
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/peakcart_gold/tables/fact_orders"
  }

  execution_spec {
    trigger {
      on_demand {}
    }
  }

  data_quality_spec {
    # stg_order_items computes is_positive_quantity/is_positive_price/
    # is_valid_discount flags, but fact_orders never carries them forward
    # or filters on them -- these rules catch what the pipeline silently
    # drops before the mart.
    rules {
      column    = "quantity"
      dimension = "VALIDITY"
      threshold = 1.0
      row_condition_expectation {
        sql_expression = "quantity > 0"
      }
    }
    rules {
      column    = "unit_price"
      dimension = "VALIDITY"
      threshold = 1.0
      row_condition_expectation {
        sql_expression = "unit_price > 0"
      }
    }
    rules {
      column    = "discount"
      dimension = "VALIDITY"
      threshold = 1.0
      row_condition_expectation {
        sql_expression = "discount >= 0 AND discount <= 1"
      }
    }
    rules {
      column    = "customer_surrogate_key"
      dimension = "COMPLETENESS"
      threshold = 1.0
      non_null_expectation {}
    }
    rules {
      column    = "product_surrogate_key"
      dimension = "COMPLETENESS"
      threshold = 1.0
      non_null_expectation {}
    }
  }

  depends_on = [google_dataplex_asset.gold_dataset]
}

resource "google_dataplex_datascan" "dim_customers_quality" {
  location     = var.region
  data_scan_id = "dim-customers-quality"
  project      = var.project_id

  data {
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/peakcart_gold/tables/dim_customers"
  }

  execution_spec {
    trigger {
      on_demand {}
    }
  }

  data_quality_spec {
    # is_valid_email/is_valid_signup_date are carried forward as real gold
    # columns (unlike fact_orders' dropped flags) -- this rule surfaces
    # the known ~2% seeded invalid-email rate as a named, tracked metric
    # instead of an invisible column nobody queries.
    rules {
      column    = "is_valid_email"
      dimension = "VALIDITY"
      threshold = 1.0
      row_condition_expectation {
        sql_expression = "is_valid_email = true"
      }
    }
    rules {
      column    = "customer_surrogate_key"
      dimension = "UNIQUENESS"
      threshold = 1.0
      uniqueness_expectation {}
    }
  }

  depends_on = [google_dataplex_asset.gold_dataset]
}

resource "google_dataplex_datascan" "dim_products_quality" {
  location     = var.region
  data_scan_id = "dim-products-quality"
  project      = var.project_id

  data {
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/peakcart_gold/tables/dim_products"
  }

  execution_spec {
    trigger {
      on_demand {}
    }
  }

  data_quality_spec {
    # stg_products carries supplier_id through with no nullness flag and
    # no filter -- the seeded NULL-supplier issue is uncaught at every
    # layer of the existing pipeline today.
    rules {
      column    = "supplier_id"
      dimension = "COMPLETENESS"
      threshold = 1.0
      non_null_expectation {}
    }
    rules {
      column    = "product_surrogate_key"
      dimension = "UNIQUENESS"
      threshold = 1.0
      uniqueness_expectation {}
    }
  }

  depends_on = [google_dataplex_asset.gold_dataset]
}

resource "google_dataplex_datascan" "fact_daily_inventory_quality" {
  location     = var.region
  data_scan_id = "fact-daily-inventory-quality"
  project      = var.project_id

  data {
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/peakcart_gold/tables/fact_daily_inventory"
  }

  execution_spec {
    trigger {
      on_demand {}
    }
  }

  data_quality_spec {
    # qty_available is derived (qty_on_hand - qty_reserved), not raw
    # input, but nothing upstream actually enforces this bound -- worth
    # checking a derived business-logic invariant, not just raw columns.
    rules {
      column    = "qty_available"
      dimension = "VALIDITY"
      threshold = 1.0
      row_condition_expectation {
        sql_expression = "qty_available >= 0"
      }
    }
  }

  depends_on = [google_dataplex_asset.gold_dataset]
}
