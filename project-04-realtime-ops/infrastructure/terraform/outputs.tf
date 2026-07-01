output "order_events_subscription" {
  description = "Subscription name Dataflow will read order events from"
  value       = module.order_events.subscription_name
}

output "delivery_events_subscription" {
  description = "Subscription name Dataflow will read delivery events from"
  value       = module.delivery_events.subscription_name
}

output "inventory_events_subscription" {
  description = "Subscription name Dataflow will read inventory events from"
  value       = module.inventory_events.subscription_name
}

output "order_events_dlq_subscription" {
  description = "Subscription name for inspecting dead-lettered order events"
  value       = module.order_events.dlq_subscription_name
}

output "delivery_events_dlq_subscription" {
  description = "Subscription name for inspecting dead-lettered delivery events"
  value       = module.delivery_events.dlq_subscription_name
}

output "inventory_events_dlq_subscription" {
  description = "Subscription name for inspecting dead-lettered inventory events"
  value       = module.inventory_events.dlq_subscription_name
}

output "pubsub_service_agent_email" {
  description = "Resolved Pub/Sub service agent email used for DLQ IAM bindings"
  value       = google_project_service_identity.pubsub.email
}
