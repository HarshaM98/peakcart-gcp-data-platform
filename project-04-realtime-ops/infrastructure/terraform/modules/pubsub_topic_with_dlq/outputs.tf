output "topic_id" {
  description = "Fully qualified ID of the main topic"
  value       = google_pubsub_topic.main.id
}

output "topic_name" {
  description = "Name of the main topic"
  value       = google_pubsub_topic.main.name
}

output "dlq_topic_id" {
  description = "Fully qualified ID of the dead-letter topic"
  value       = google_pubsub_topic.dlq.id
}

output "subscription_id" {
  description = "Fully qualified ID of the main subscription"
  value       = google_pubsub_subscription.main.id
}

output "subscription_name" {
  description = "Name of the main subscription, used by the Dataflow pipeline in Phase 3"
  value       = google_pubsub_subscription.main.name
}

output "dlq_subscription_name" {
  description = "Name of the dead-letter inspection subscription"
  value       = google_pubsub_subscription.dlq.name
}
