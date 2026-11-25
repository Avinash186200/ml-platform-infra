variable "project" {
  description = "Project name, used in resource names and tags."
  type        = string
  default     = "mlplat"
}

variable "environment" {
  description = "Environment name."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "VPC CIDR."
  type        = string
  default     = "10.40.0.0/16"
}

variable "kubernetes_version" {
  description = "EKS version (keep in step with KUBERNETES_VERSION in the CI workflow)."
  type        = string
  default     = "1.31"
}

variable "api_allowed_cidrs" {
  description = "CIDRs allowed to reach the public Kubernetes API. Empty = private endpoint only (use a VPN or bastion)."
  type        = list(string)
  default     = []
}

variable "cluster_admin_role_arns" {
  description = "IAM roles given cluster-admin through EKS access entries."
  type        = list(string)
  default     = []
}

variable "node_instance_types" {
  description = "Node instance types."
  type        = list(string)
  default     = ["t3.large"]
}

variable "node_capacity_type" {
  description = "ON_DEMAND or SPOT."
  type        = string
  default     = "ON_DEMAND"
}

variable "redis_node_type" {
  description = "ElastiCache node type."
  type        = string
  default     = "cache.t4g.small"
}

variable "redis_replicas" {
  description = "Redis read replicas (0 = single node, cheapest; 1 = Multi-AZ failover)."
  type        = number
  default     = 0
}
