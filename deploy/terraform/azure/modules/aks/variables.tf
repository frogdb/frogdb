# Azure AKS Module Variables

variable "cluster_name" {
  description = "AKS cluster name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "vnet_cidr" {
  description = "VNet CIDR"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "Subnet CIDR"
  type        = string
  default     = "10.0.0.0/20"
}

variable "node_vm_size" {
  description = "Node VM size"
  type        = string
  default     = "Standard_D4s_v3"
}

variable "node_count" {
  description = "Initial node count"
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
  description = "Node OS disk size in GB"
  type        = number
  default     = 100
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
