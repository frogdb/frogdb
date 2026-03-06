# Azure Dev Environment Variables

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "image_tag" {
  description = "FrogDB image tag"
  type        = string
  default     = "latest"
}
