# Azure Dev Environment

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.0"
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

  backend "azurerm" {
    # Configure via backend config file or CLI flags
    # resource_group_name  = "frogdb-terraform-state"
    # storage_account_name = "frogdbtfstate"
    # container_name       = "tfstate"
    # key                  = "azure/dev/terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# AKS Cluster
module "aks" {
  source = "../../modules/aks"

  cluster_name       = "frogdb-dev"
  location           = var.location
  kubernetes_version = "1.28"
  node_vm_size       = "Standard_D4s_v3"
  enable_autoscaling = true
  node_min_count     = 1
  node_max_count     = 5

  tags = {
    environment = "dev"
    project     = "frogdb"
    managed-by  = "terraform"
  }
}

# Configure Kubernetes provider
provider "kubernetes" {
  host                   = module.aks.cluster_host
  cluster_ca_certificate = base64decode(module.aks.cluster_ca_certificate)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "kubelogin"
    args        = ["get-token", "--login", "azurecli", "--server-id", "6dae42f8-4368-4678-94ff-3960e28e3630"]
  }
}

# Configure Helm provider
provider "helm" {
  kubernetes {
    host                   = module.aks.cluster_host
    cluster_ca_certificate = base64decode(module.aks.cluster_ca_certificate)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "kubelogin"
      args        = ["get-token", "--login", "azurecli", "--server-id", "6dae42f8-4368-4678-94ff-3960e28e3630"]
    }
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

  storage_class = "premium-ssd"
  storage_size  = "10Gi"

  cpu_request    = "500m"
  cpu_limit      = "2"
  memory_request = "1Gi"
  memory_limit   = "4Gi"

  service_monitor_enabled = false

  depends_on = [module.aks]
}

# Outputs
output "cluster_name" {
  value = module.aks.cluster_name
}

output "kubeconfig_command" {
  value = module.aks.kubeconfig_command
}

output "frogdb_service" {
  value = module.frogdb.cluster_dns
}
