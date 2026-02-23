# FrogDB Helm Module Variables

variable "release_name" {
  description = "Helm release name"
  type        = string
  default     = "frogdb"
}

variable "namespace" {
  description = "Kubernetes namespace"
  type        = string
  default     = "frogdb"
}

variable "create_namespace" {
  description = "Create the namespace if it doesn't exist"
  type        = bool
  default     = true
}

variable "helm_repository" {
  description = "Helm chart repository URL"
  type        = string
  default     = ""
}

variable "helm_chart" {
  description = "Helm chart name or path"
  type        = string
  default     = "../../helm/frogdb"
}

variable "helm_chart_version" {
  description = "Helm chart version"
  type        = string
  default     = null
}

variable "replica_count" {
  description = "Number of FrogDB replicas"
  type        = number
  default     = 1
}

variable "cluster_enabled" {
  description = "Enable Raft cluster mode"
  type        = bool
  default     = false
}

variable "image_repository" {
  description = "Container image repository"
  type        = string
  default     = "ghcr.io/nathanjordan/frogdb"
}

variable "image_tag" {
  description = "Container image tag"
  type        = string
  default     = "latest"
}

variable "storage_class" {
  description = "Storage class for PVCs"
  type        = string
  default     = ""
}

variable "storage_size" {
  description = "Storage size for each replica"
  type        = string
  default     = "10Gi"
}

variable "cpu_request" {
  description = "CPU request per replica"
  type        = string
  default     = "500m"
}

variable "cpu_limit" {
  description = "CPU limit per replica"
  type        = string
  default     = "2"
}

variable "memory_request" {
  description = "Memory request per replica"
  type        = string
  default     = "1Gi"
}

variable "memory_limit" {
  description = "Memory limit per replica"
  type        = string
  default     = "4Gi"
}

variable "service_monitor_enabled" {
  description = "Enable Prometheus ServiceMonitor"
  type        = bool
  default     = false
}

variable "wait" {
  description = "Wait for deployment to complete"
  type        = bool
  default     = true
}

variable "timeout" {
  description = "Helm install/upgrade timeout in seconds"
  type        = number
  default     = 600
}

variable "labels" {
  description = "Additional labels for resources"
  type        = map(string)
  default     = {}
}

variable "additional_set_values" {
  description = "Additional Helm values to set"
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}
