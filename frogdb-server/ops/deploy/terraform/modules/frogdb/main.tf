# FrogDB Helm Deployment Module
#
# This module deploys FrogDB to a Kubernetes cluster using Helm.
# It is cloud-agnostic and works with any Kubernetes cluster.

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0"
    }
  }
}

resource "kubernetes_namespace" "frogdb" {
  count = var.create_namespace ? 1 : 0

  metadata {
    name = var.namespace
    labels = merge(var.labels, {
      "app.kubernetes.io/managed-by" = "terraform"
    })
  }
}

resource "helm_release" "frogdb" {
  name       = var.release_name
  repository = var.helm_repository
  chart      = var.helm_chart
  version    = var.helm_chart_version
  namespace  = var.namespace

  create_namespace = false
  wait             = var.wait
  timeout          = var.timeout

  values = [
    templatefile("${path.module}/values.yaml.tpl", {
      replica_count           = var.replica_count
      cluster_enabled         = var.cluster_enabled
      image_repository        = var.image_repository
      image_tag               = var.image_tag
      storage_class           = var.storage_class
      storage_size            = var.storage_size
      cpu_request             = var.cpu_request
      cpu_limit               = var.cpu_limit
      memory_request          = var.memory_request
      memory_limit            = var.memory_limit
      service_monitor_enabled = var.service_monitor_enabled
    })
  ]

  dynamic "set" {
    for_each = var.additional_set_values
    content {
      name  = set.value.name
      value = set.value.value
    }
  }

  depends_on = [kubernetes_namespace.frogdb]
}

# Output connection information
output "service_name" {
  description = "FrogDB service name"
  value       = "${var.release_name}-frogdb"
}

output "service_port" {
  description = "FrogDB service port"
  value       = 6379
}

output "namespace" {
  description = "Kubernetes namespace"
  value       = var.namespace
}

output "cluster_dns" {
  description = "FrogDB cluster DNS name"
  value       = "${var.release_name}-frogdb.${var.namespace}.svc.cluster.local"
}
