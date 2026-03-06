# AWS Dev Environment

terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
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

  backend "s3" {
    # Configure via backend config file or CLI flags
    # bucket = "frogdb-terraform-state"
    # key    = "aws/dev/terraform.tfstate"
    # region = "us-west-2"
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Environment = "dev"
      Project     = "frogdb"
      ManagedBy   = "terraform"
    }
  }
}

# EKS Cluster
module "eks" {
  source = "../../modules/eks"

  cluster_name       = "frogdb-dev"
  region             = var.region
  environment        = "dev"
  kubernetes_version = "1.28"

  node_instance_types = ["m5.large"]
  node_min_size       = 1
  node_max_size       = 5
  node_desired_size   = 2
  use_spot_instances  = true

  tags = {
    Environment = "dev"
    Project     = "frogdb"
  }
}

# Configure Kubernetes provider
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

# Configure Helm provider
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
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

  storage_class = "gp3"
  storage_size  = "10Gi"

  cpu_request    = "500m"
  cpu_limit      = "2"
  memory_request = "1Gi"
  memory_limit   = "4Gi"

  service_monitor_enabled = false

  depends_on = [module.eks]
}

# Outputs
output "cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  value = module.eks.kubeconfig_command
}

output "frogdb_service" {
  value = module.frogdb.cluster_dns
}
