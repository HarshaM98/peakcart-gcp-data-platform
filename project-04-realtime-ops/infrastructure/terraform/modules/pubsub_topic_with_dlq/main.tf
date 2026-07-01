resource "google_pubsub_topic" "main" {
  project = var.project_id
  name    = var.topic_name
  labels  = var.labels
}

resource "google_pubsub_topic" "dlq" {
  project = var.project_id
  name    = "${var.topic_name}-dlq"
  labels  = var.labels
}

resource "google_pubsub_subscription" "main" {
  project = var.project_id
  name    = "${var.topic_name}-sub"
  topic   = google_pubsub_topic.main.id

  labels                       = var.labels
  enable_exactly_once_delivery = var.enable_exactly_once_delivery

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = var.max_delivery_attempts
  }
}

resource "google_pubsub_subscription" "dlq" {
  project = var.project_id
  name    = "${var.topic_name}-dlq-sub"
  topic   = google_pubsub_topic.dlq.id
  labels  = var.labels
}

resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.pubsub_service_agent_email}"
}

resource "google_pubsub_subscription_iam_member" "main_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.main.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${var.pubsub_service_agent_email}"
}
