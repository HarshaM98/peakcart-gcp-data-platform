terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.45.2"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "gcs" {
    bucket = "peakcart-terraform-state-2026"
    prefix = "project-02/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "sqladmin" {
  project            = var.project_id
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

resource "random_password" "legacy_db_password" {
  length  = 24
  special = false
}

resource "google_project_service" "secretmanager" {
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

# Stores the legacy DB password so the Dataproc job fetches it at runtime
# (via `gcloud secrets versions access`) rather than passing it as a
# plaintext job argument, which would otherwise show up in job history/logs.
resource "google_secret_manager_secret" "legacy_db_password" {
  project   = var.project_id
  secret_id = "peakcart-legacy-db-password"

  replication {
    auto {}
  }

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "legacy_db_password" {
  secret      = google_secret_manager_secret.legacy_db_password.id
  secret_data = random_password.legacy_db_password.result
}

resource "google_project_service" "servicenetworking" {
  project            = var.project_id
  service            = "servicenetworking.googleapis.com"
  disable_on_destroy = false
}

data "google_compute_network" "default" {
  project = var.project_id
  name    = "default"
}

# Private Service Access (VPC peering) so Cloud SQL can get a private IP on
# the same VPC as the Dataproc cluster. Discovered as necessary the hard
# way: the Dataproc node has no general internet egress (an org policy
# blocks external IPs on VMs here), so even the Cloud SQL Auth Proxy running
# on the node couldn't dial Cloud SQL's public IP -- see NOTES.md. A direct
# private-IP connection over the VPC sidesteps that entirely, and is the
# more idiomatic pattern for VPC-internal service-to-service traffic anyway
# (the Auth Proxy is for external/local-dev access, not this).
#
# The default network already has a servicenetworking peering connection
# (google-managed-services-default, from something else in this project) --
# reused as-is rather than creating a second, conflicting one. Cloud SQL's
# private_network setting below is all that's needed; it uses whatever
# peering range is already available on the network, with no explicit
# Terraform-managed connection resource required here.

# Represents PeakCart's legacy on-prem OLTP source, pre-dating the GCP
# warehouse -- smallest available tier (db-f1-micro) since this instance
# only needs to exist for migration verification, not steady-state serving.
# No authorized networks: access is via the Cloud SQL Auth Proxy (IAM +
# client certs) rather than whitelisting a raw IP, both more secure and
# avoids re-whitelisting every time a local/dynamic IP changes.
resource "google_sql_database_instance" "legacy_postgres" {
  name                = "peakcart-legacy-postgres"
  project             = var.project_id
  region              = var.region
  database_version    = "POSTGRES_15"
  deletion_protection = false

  settings {
    tier              = "db-f1-micro"
    availability_type = "ZONAL"
    disk_size         = 10
    disk_type         = "PD_SSD"

    ip_configuration {
      ipv4_enabled    = true
      private_network = data.google_compute_network.default.id

      # Datastream's static IPs for us-central1 (fetched via
      # `datastream.projects.locations.fetchStaticIps`) -- switched to this
      # public-IP + allowlist connectivity after private connectivity via
      # VPC peering hit an unresolved transitive-routing timeout (see
      # NOTES.md). Datastream itself isn't a VM in our project, so it's not
      # subject to the org policy blocking external IPs that affected the
      # Dataproc cluster.
      dynamic "authorized_networks" {
        for_each = toset([
          "34.71.242.81",
          "34.72.28.29",
          "34.67.6.157",
          "34.67.234.134",
          "34.72.239.218",
        ])
        content {
          name  = "datastream-${replace(authorized_networks.value, ".", "-")}"
          value = "${authorized_networks.value}/32"
        }
      }
    }

    backup_configuration {
      enabled = false
    }

    # Required for Datastream's Postgres CDC (Phase 3) -- enables logical
    # replication (WAL-based change capture). Applying this flag restarts
    # the instance.
    database_flags {
      name  = "cloudsql.logical_decoding"
      value = "on"
    }
  }

  depends_on = [
    google_project_service.sqladmin,
    google_project_service.servicenetworking,
  ]
}

resource "google_sql_database" "peakcart_legacy" {
  name     = "peakcart_legacy"
  project  = var.project_id
  instance = google_sql_database_instance.legacy_postgres.name
}

resource "google_sql_user" "peakcart_app" {
  name     = "peakcart_app"
  project  = var.project_id
  instance = google_sql_database_instance.legacy_postgres.name
  password = random_password.legacy_db_password.result
}

resource "random_password" "postgres_admin_password" {
  length  = 24
  special = false
}

# Cloud SQL's built-in default superuser -- Terraform manages its password
# rather than creating a new role, since it already has the REPLICATION
# privilege Datastream's connection needs, with no separate GRANT required.
resource "google_sql_user" "postgres_admin" {
  name     = "postgres"
  project  = var.project_id
  instance = google_sql_database_instance.legacy_postgres.name
  password = random_password.postgres_admin_password.result
}

resource "google_project_service" "dataproc" {
  project            = var.project_id
  service            = "dataproc.googleapis.com"
  disable_on_destroy = false
}

# Bronze layer for the migrated legacy data, written by the Dataproc/Spark
# bulk migration job. Reuses project-01's bigquery_dataset module rather
# than duplicating it.
module "migration_bronze" {
  source      = "../../../project-01-data-warehouse/infrastructure/terraform/modules/bigquery_dataset"
  dataset_id  = "peakcart_migration_bronze"
  description = "Bulk-migrated legacy OLTP data (from Cloud SQL PostgreSQL), written by the Dataproc/Spark migration job."
  location    = var.region
  labels = {
    project = "project02"
    domain  = "cloud_migration"
    layer   = "bronze"
  }
  writers = ["user:harsha.manjunatha98@gmail.com"]
}

# Dataproc staging/temp bucket -- required for both cluster job staging and
# the Spark BigQuery connector's indirect write path. Short lifecycle since
# this is scratch space, not durable data (same reasoning as the Dataflow
# staging bucket in project-04).
resource "google_storage_bucket" "dataproc_staging" {
  name                        = "peakcart-dataproc-staging-2026"
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
    project = "project02"
    domain  = "cloud_migration"
    purpose = "dataproc_staging"
  }
}

# Service account for the Dataproc cluster's VMs: needs to reach Cloud SQL
# (via the Auth Proxy running as a cluster init action), write to the
# staging bucket, and write the migrated data to BigQuery.
resource "google_service_account" "dataproc_migration" {
  project      = var.project_id
  account_id   = "peakcart-dataproc-migration"
  display_name = "peakcart-dataproc-migration (project-02 Dataproc cluster)"
}

resource "google_project_iam_member" "dataproc_migration_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.dataproc_migration.email}"
}

resource "google_project_iam_member" "dataproc_migration_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_migration.email}"
}

resource "google_project_iam_member" "dataproc_migration_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc_migration.email}"
}

resource "google_project_iam_member" "dataproc_migration_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_migration.email}"
}

resource "google_project_iam_member" "dataproc_migration_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_migration.email}"
}

resource "google_project_iam_member" "dataproc_migration_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.dataproc_migration.email}"
}

resource "google_storage_bucket_object" "init_action" {
  name         = "init-actions/install-cloud-sql-proxy.sh"
  bucket       = google_storage_bucket.dataproc_staging.name
  source       = "${path.module}/../../scripts/install-cloud-sql-proxy.sh"
  content_type = "text/x-shellscript"
}

# Single-node cluster (dataproc:dataproc.allow.zero.workers) -- this is a
# short-lived job for a one-off bulk migration, not a standing cluster, so
# the smallest viable footprint keeps cost down.
#
# internal_ip_only is explicitly true (matching what GCP actually enforces
# here via an org policy blocking external IPs on VMs, discovered when the
# Auth Proxy running on the node couldn't dial Cloud SQL's public IP --
# see NOTES.md). Because of that, the migration job connects to Cloud SQL's
# private IP directly (see private_network on the SQL instance above)
# rather than through the Auth Proxy init action, which is left in place
# but unused for this path.
resource "google_dataproc_cluster" "migration" {
  name    = "peakcart-migration-cluster"
  project = var.project_id
  region  = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging.name

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-2"
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    software_config {
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      service_account  = google_service_account.dataproc_migration.email
      internal_ip_only = true
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
      ]
      metadata = {
        instance-connection-name = google_sql_database_instance.legacy_postgres.connection_name
      }
    }

    initialization_action {
      script      = "gs://${google_storage_bucket.dataproc_staging.name}/${google_storage_bucket_object.init_action.name}"
      timeout_sec = 300
    }
  }

  depends_on = [
    google_project_service.dataproc,
    google_storage_bucket_object.init_action,
  ]
}

# ---------------------------------------------------------------------------
# Phase 3: Datastream incremental sync (Postgres CDC -> BigQuery)
#
# Dataproc (above) already did the historical bulk migration, so this stream
# is configured with backfill_none -- it only needs to pick up changes going
# forward, which is the realistic reason both mechanisms coexist in a real
# migration (one-time cutover vs. ongoing sync).
# ---------------------------------------------------------------------------

resource "google_project_service" "datastream" {
  project            = var.project_id
  service            = "datastream.googleapis.com"
  disable_on_destroy = false
}

# Originally used private connectivity (VPC peering) to reach Cloud SQL's
# private IP, but that hit an unresolved transitive-routing timeout even
# after enabling custom route exchange on both peerings (see NOTES.md).
# Switched to IP-allowlist connectivity instead -- Cloud SQL's public IP,
# with Datastream's published static IPs (below) added as authorized
# networks. Datastream itself isn't a VM in our project, so it's not
# subject to the org policy blocking external IPs that affected Dataproc.
resource "google_datastream_connection_profile" "postgres_source" {
  project               = var.project_id
  location              = var.region
  connection_profile_id = "peakcart-legacy-postgres-source"
  display_name          = "peakcart-legacy-postgres-source"

  postgresql_profile {
    hostname = google_sql_database_instance.legacy_postgres.public_ip_address
    port     = 5432
    username = google_sql_user.postgres_admin.name
    password = random_password.postgres_admin_password.result
    database = google_sql_database.peakcart_legacy.name
  }
}

# Separate dataset from peakcart_migration_bronze (the Dataproc bulk-load
# output) -- Datastream manages its own replica-table schema/merge logic,
# which would conflict with the plain overwrite-written bronze tables.
module "migration_cdc" {
  source      = "../../../project-01-data-warehouse/infrastructure/terraform/modules/bigquery_dataset"
  dataset_id  = "peakcart_migration_cdc"
  description = "Ongoing CDC replica tables from Datastream (Postgres logical replication), kept separate from the Dataproc bulk-load bronze dataset."
  location    = var.region
  labels = {
    project = "project02"
    domain  = "cloud_migration"
    layer   = "cdc"
  }
  writers = ["user:harsha.manjunatha98@gmail.com"]
}

resource "google_datastream_connection_profile" "bigquery_dest" {
  project               = var.project_id
  location              = var.region
  connection_profile_id = "peakcart-migration-bigquery-dest"
  display_name          = "peakcart-migration-bigquery-dest"

  bigquery_profile {}

  depends_on = [google_project_service.datastream]
}

resource "google_datastream_stream" "migration" {
  project       = var.project_id
  location      = var.region
  stream_id     = "peakcart-migration-cdc-stream"
  display_name  = "peakcart-migration-cdc-stream"
  desired_state = "RUNNING"

  source_config {
    source_connection_profile = google_datastream_connection_profile.postgres_source.id
    postgresql_source_config {
      replication_slot = "datastream_slot"
      publication      = "datastream_publication"
    }
  }

  destination_config {
    destination_connection_profile = google_datastream_connection_profile.bigquery_dest.id
    bigquery_destination_config {
      data_freshness = "300s"
      single_target_dataset {
        dataset_id = "projects/${var.project_id}/datasets/${module.migration_cdc.dataset_id}"
      }
    }
  }

  backfill_none {}
}
