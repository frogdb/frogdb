# AWS Dev Environment Variables

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "image_tag" {
  description = "FrogDB image tag"
  type        = string
  default     = "latest"
}
