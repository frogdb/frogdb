# GCP GKE Module Variables

variable "cluster_name" {
  description = "GKE cluster name"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "regional" {
  description = "Create a regional cluster (vs zonal)"
  type        = bool
  default     = true
}

variable "release_channel" {
  description = "GKE release channel (RAPID, REGULAR, STABLE)"
  type        = string
  default     = "REGULAR"
}

variable "subnet_cidr" {
  description = "Subnet CIDR"
  type        = string
  default     = "10.0.0.0/20"
}

variable "pods_cidr" {
  description = "Pods secondary IP range CIDR"
  type        = string
  default     = "10.1.0.0/16"
}

variable "services_cidr" {
  description = "Services secondary IP range CIDR"
  type        = string
  default     = "10.2.0.0/20"
}

variable "master_cidr" {
  description = "Master CIDR block for private cluster"
  type        = string
  default     = "172.16.0.0/28"
}

variable "node_machine_type" {
  description = "Node machine type"
  type        = string
  default     = "e2-standard-4"
}

variable "node_count" {
  description = "Number of nodes (for zonal clusters)"
  type        = number
  default     = 3
}

variable "enable_autoscaling" {
  description = "Enable cluster autoscaling"
  type        = bool
  default     = true
}

variable "node_min_count" {
  description = "Minimum nodes for autoscaling"
  type        = number
  default     = 1
}

variable "node_max_count" {
  description = "Maximum nodes for autoscaling"
  type        = number
  default     = 10
}

variable "node_disk_size" {
  description = "Node disk size in GB"
  type        = number
  default     = 100
}

variable "labels" {
  description = "Resource labels"
  type        = map(string)
  default     = {}
}
