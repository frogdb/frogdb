# Monitoring Stack Module (Prometheus + Grafana)

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

resource "kubernetes_namespace" "monitoring" {
  count = var.create_namespace ? 1 : 0

  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
}

# Prometheus Operator (kube-prometheus-stack)
resource "helm_release" "prometheus" {
  name       = "prometheus"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = var.prometheus_chart_version
  namespace  = var.namespace

  create_namespace = false
  wait             = true
  timeout          = 600

  values = [
    <<-EOT
    prometheus:
      prometheusSpec:
        retention: ${var.prometheus_retention}
        storageSpec:
          volumeClaimTemplate:
            spec:
              storageClassName: ${var.storage_class}
              resources:
                requests:
                  storage: ${var.prometheus_storage_size}
        serviceMonitorSelectorNilUsesHelmValues: false
        podMonitorSelectorNilUsesHelmValues: false

    grafana:
      enabled: ${var.grafana_enabled}
      adminPassword: ${var.grafana_admin_password}
      persistence:
        enabled: true
        storageClassName: ${var.storage_class}
        size: ${var.grafana_storage_size}
      dashboardProviders:
        dashboardproviders.yaml:
          apiVersion: 1
          providers:
            - name: 'frogdb'
              orgId: 1
              folder: 'FrogDB'
              type: file
              disableDeletion: false
              editable: true
              options:
                path: /var/lib/grafana/dashboards/frogdb

    alertmanager:
      enabled: ${var.alertmanager_enabled}
      alertmanagerSpec:
        storage:
          volumeClaimTemplate:
            spec:
              storageClassName: ${var.storage_class}
              resources:
                requests:
                  storage: ${var.alertmanager_storage_size}
    EOT
  ]

  depends_on = [kubernetes_namespace.monitoring]
}

# FrogDB Grafana Dashboard
resource "kubernetes_config_map" "frogdb_dashboard" {
  count = var.grafana_enabled ? 1 : 0

  metadata {
    name      = "frogdb-dashboard"
    namespace = var.namespace
    labels = {
      grafana_dashboard = "1"
    }
  }

  data = {
    "frogdb-overview.json" = file("${path.module}/../../dashboards/frogdb-overview.json")
  }

  depends_on = [helm_release.prometheus]
}

# Outputs
output "prometheus_service" {
  description = "Prometheus service URL"
  value       = "prometheus-kube-prometheus-prometheus.${var.namespace}.svc.cluster.local:9090"
}

output "grafana_service" {
  description = "Grafana service URL"
  value       = var.grafana_enabled ? "prometheus-grafana.${var.namespace}.svc.cluster.local:80" : null
}

output "alertmanager_service" {
  description = "Alertmanager service URL"
  value       = var.alertmanager_enabled ? "prometheus-kube-prometheus-alertmanager.${var.namespace}.svc.cluster.local:9093" : null
}
