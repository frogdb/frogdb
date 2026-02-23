# Monitoring Module Variables

variable "namespace" {
  description = "Kubernetes namespace for monitoring stack"
  type        = string
  default     = "monitoring"
}

variable "create_namespace" {
  description = "Create the namespace if it doesn't exist"
  type        = bool
  default     = true
}

variable "prometheus_chart_version" {
  description = "kube-prometheus-stack chart version"
  type        = string
  default     = "55.0.0"
}

variable "storage_class" {
  description = "Storage class for persistent volumes"
  type        = string
  default     = ""
}

variable "prometheus_retention" {
  description = "Prometheus data retention period"
  type        = string
  default     = "15d"
}

variable "prometheus_storage_size" {
  description = "Prometheus storage size"
  type        = string
  default     = "50Gi"
}

variable "grafana_enabled" {
  description = "Enable Grafana"
  type        = bool
  default     = true
}

variable "grafana_admin_password" {
  description = "Grafana admin password"
  type        = string
  sensitive   = true
  default     = "admin"
}

variable "grafana_storage_size" {
  description = "Grafana storage size"
  type        = string
  default     = "10Gi"
}

variable "alertmanager_enabled" {
  description = "Enable Alertmanager"
  type        = bool
  default     = true
}

variable "alertmanager_storage_size" {
  description = "Alertmanager storage size"
  type        = string
  default     = "10Gi"
}
