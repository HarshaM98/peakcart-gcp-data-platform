output "instance_connection_name" {
  description = "Cloud SQL instance connection name, for use with the Cloud SQL Auth Proxy"
  value       = google_sql_database_instance.legacy_postgres.connection_name
}

output "db_password" {
  description = "Password for the peakcart_app database user"
  value       = random_password.legacy_db_password.result
  sensitive   = true
}

output "private_ip_address" {
  description = "Cloud SQL private IP, reachable directly from the Dataproc cluster's VPC"
  value       = google_sql_database_instance.legacy_postgres.private_ip_address
}

output "postgres_admin_password" {
  description = "Password for the built-in postgres superuser (used by Datastream and for logical replication setup)"
  value       = random_password.postgres_admin_password.result
  sensitive   = true
}
