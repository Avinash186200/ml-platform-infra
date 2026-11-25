# Dev environment: one VPC, one EKS cluster, one artifact bucket, one Redis,
# one image repository, and an IAM role per workload. Nothing here has been
# applied to a real AWS account; CI only runs fmt, validate, tflint and checkov.

locals {
  name      = "${var.project}-${var.environment}"
  namespace = "ml-platform"
  tags = {
    project     = var.project
    environment = var.environment
    managed-by  = "terraform"
  }
}

module "network" {
  source           = "../../modules/network"
  name             = local.name
  region           = var.region
  cluster_name     = local.name
  cidr_block       = var.vpc_cidr
  az_count         = 2
  logs_kms_key_arn = aws_kms_key.logs.arn
  tags             = local.tags
}

module "eks" {
  source               = "../../modules/eks"
  name                 = local.name
  kubernetes_version   = var.kubernetes_version
  private_subnet_ids   = module.network.private_subnet_ids
  public_access_cidrs  = var.api_allowed_cidrs
  admin_principal_arns = var.cluster_admin_role_arns
  secrets_kms_key_arn  = aws_kms_key.data.arn
  logs_kms_key_arn     = aws_kms_key.logs.arn
  node_instance_types  = var.node_instance_types
  node_capacity_type   = var.node_capacity_type
  node_min             = 1
  node_desired         = 2
  node_max             = 4
  tags                 = local.tags
}

module "storage" {
  source        = "../../modules/storage"
  bucket_name   = "${local.name}-artifacts-${data.aws_caller_identity.current.account_id}"
  kms_key_arn   = aws_kms_key.data.arn
  force_destroy = false
  tags          = local.tags
}

module "registry" {
  source      = "../../modules/registry"
  name        = "${var.project}/platform"
  kms_key_arn = aws_kms_key.data.arn
  tags        = local.tags
}

module "redis" {
  source                    = "../../modules/redis"
  name                      = local.name
  vpc_id                    = module.network.vpc_id
  subnet_ids                = module.network.private_subnet_ids
  allowed_security_group_id = module.eks.cluster_security_group_id
  kms_key_arn               = aws_kms_key.data.arn
  node_type                 = var.redis_node_type
  replicas                  = var.redis_replicas
  tags                      = local.tags
}

module "iam" {
  source            = "../../modules/iam"
  name              = local.name
  region            = var.region
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer       = module.eks.oidc_issuer
  bucket_arn        = module.storage.bucket_arn
  kms_key_arn       = aws_kms_key.data.arn
  tags              = local.tags

  workloads = {
    feature-api = {
      namespace       = local.namespace
      service_account = "feature-api"
      read_prefixes   = ["features/"]
      write_prefixes  = []
    }
    model-api = {
      namespace       = local.namespace
      service_account = "model-api"
      read_prefixes   = ["models/"]
      write_prefixes  = []
    }
    jobs-worker = {
      namespace       = local.namespace
      service_account = "jobs-worker"
      read_prefixes   = ["features/"]
      write_prefixes  = ["jobs/"]
    }
    dagster = {
      namespace       = local.namespace
      service_account = "dagster"
      read_prefixes   = ["raw/", "features/", "training/", "models/"]
      write_prefixes  = ["raw/", "features/", "training/", "models/"]
    }
  }
}
