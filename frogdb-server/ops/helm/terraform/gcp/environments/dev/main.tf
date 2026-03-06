# GCP Dev Environment

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
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.0"
    }
  }

  backend "gcs" {
    # Configure via backend config file or CLI flags
    # bucket = "frogdb-terraform-state"
    # prefix = "gcp/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GKE Cluster
module "gke" {
  source = "../../modules/gke"

  cluster_name      = "frogdb-dev"
  region            = var.region
  regional          = false
  release_channel   = "REGULAR"
  node_machine_type = "e2-standard-4"
  enable_autoscaling = true
  node_min_count    = 1
  node_max_count    = 5

  labels = {
    environment = "dev"
    project     = "frogdb"
    managed-by  = "terraform"
  }
}

# Configure Kubernetes provider
data "google_client_config" "default" {}

provider "kubernetes" {
  host                   = "https://${module.gke.cluster_endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
}

# Configure Helm provider
provider "helm" {
  kubernetes {
    host                   = "https://${module.gke.cluster_endpoint}"
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
  }
}

# FrogDB Deployment
module "frogdb" {
  source = "../../../modules/frogdb"

  release_name    = "frogdb"
  namespace       = "frogdb"
  replica_count   = 1
  cluster_enabled = false

  image_repository = "ghcr.io/nathanjordan/frogdb"
  image_tag        = var.image_tag

  storage_class = "ssd"
  storage_size  = "10Gi"

  cpu_request    = "500m"
  cpu_limit      = "2"
  memory_request = "1Gi"
  memory_limit   = "4Gi"

  service_monitor_enabled = false

  depends_on = [module.gke]
}

# Outputs
output "cluster_name" {
  value = module.gke.cluster_name
}

output "kubeconfig_command" {
  value = module.gke.kubeconfig_command
}

output "frogdb_service" {
  value = module.frogdb.cluster_dns
}
