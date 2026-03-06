# GCP GKE Cluster Module for FrogDB

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0"
    }
  }
}

# Data sources
data "google_project" "current" {}

data "google_compute_zones" "available" {
  region = var.region
}

# VPC Network
resource "google_compute_network" "vpc" {
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.cluster_name}-subnet"
  network       = google_compute_network.vpc.id
  region        = var.region
  ip_cidr_range = var.subnet_cidr

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }

  private_ip_google_access = true
}

# Cloud Router for NAT
resource "google_compute_router" "router" {
  name    = "${var.cluster_name}-router"
  network = google_compute_network.vpc.id
  region  = var.region
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.cluster_name}-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# GKE Cluster
resource "google_container_cluster" "cluster" {
  name     = var.cluster_name
  location = var.regional ? var.region : data.google_compute_zones.available.names[0]

  # Remove default node pool
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # Workload Identity
  workload_identity_config {
    workload_pool = "${data.google_project.current.project_id}.svc.id.goog"
  }

  # Release channel
  release_channel {
    channel = var.release_channel
  }

  # Addons
  addons_config {
    http_load_balancing {
      disabled = false
    }
    horizontal_pod_autoscaling {
      disabled = false
    }
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # Private cluster
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = var.master_cidr
  }

  # Master authorized networks
  master_authorized_networks_config {
    cidr_blocks {
      cidr_block   = "0.0.0.0/0"
      display_name = "All"
    }
  }

  resource_labels = var.labels

  lifecycle {
    ignore_changes = [initial_node_count]
  }
}

# Node Pool
resource "google_container_node_pool" "frogdb" {
  name       = "${var.cluster_name}-frogdb"
  location   = google_container_cluster.cluster.location
  cluster    = google_container_cluster.cluster.name
  node_count = var.regional ? null : var.node_count

  dynamic "autoscaling" {
    for_each = var.enable_autoscaling ? [1] : []
    content {
      min_node_count = var.node_min_count
      max_node_count = var.node_max_count
    }
  }

  node_config {
    machine_type = var.node_machine_type
    disk_size_gb = var.node_disk_size
    disk_type    = "pd-ssd"

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    labels = merge(var.labels, {
      workload = "frogdb"
    })

    tags = ["frogdb"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Storage class for FrogDB
resource "kubernetes_storage_class" "ssd" {
  metadata {
    name = "ssd"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner    = "pd.csi.storage.gke.io"
  reclaim_policy         = "Delete"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  parameters = {
    type = "pd-ssd"
  }

  depends_on = [google_container_node_pool.frogdb]
}

# Outputs
output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.cluster.name
}

output "cluster_endpoint" {
  description = "GKE cluster endpoint"
  value       = google_container_cluster.cluster.endpoint
}

output "cluster_ca_certificate" {
  description = "GKE cluster CA certificate"
  value       = google_container_cluster.cluster.master_auth[0].cluster_ca_certificate
}

output "cluster_location" {
  description = "GKE cluster location"
  value       = google_container_cluster.cluster.location
}

output "vpc_name" {
  description = "VPC name"
  value       = google_compute_network.vpc.name
}

output "kubeconfig_command" {
  description = "Command to update kubeconfig"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.cluster.name} --region ${var.region}"
}
