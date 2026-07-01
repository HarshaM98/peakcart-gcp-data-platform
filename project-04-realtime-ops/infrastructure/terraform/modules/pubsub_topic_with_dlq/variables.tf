variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "topic_name" {
  description = "Name of the main topic, e.g. peakcart-order-events"
  type        = string
}

variable "max_delivery_attempts" {
  description = "Number of delivery attempts before a message is routed to the dead-letter topic"
  type        = number
  default     = 5
}

variable "enable_exactly_once_delivery" {
  description = "Whether the main subscription enforces exactly-once delivery"
  type        = bool
  default     = true
}

variable "labels" {
  description = "Labels applied to all resources created by this module"
  type        = map(string)
  default     = {}
}

variable "pubsub_service_agent_email" {
  description = "Email of the Pub/Sub service agent, resolved once at the root and passed into every module call"
  type        = string
}
