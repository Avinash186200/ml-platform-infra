variable "name" {
  description = "Cluster name; also the prefix for its IAM roles."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version. CI validates the manifests against the same minor version."
  type        = string
  default     = "1.31"
}

variable "private_subnet_ids" {
  description = "Private subnets for the control-plane ENIs and the nodes."
  type        = list(string)
}

variable "public_access_cidrs" {
  description = "CIDRs allowed to reach the public API endpoint. Empty = private endpoint only."
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.public_access_cidrs, "0.0.0.0/0")
    error_message = "Do not open the Kubernetes API to the whole internet."
  }
}

variable "admin_principal_arns" {
  description = "IAM role ARNs granted cluster-admin through EKS access entries."
  type        = list(string)
  default     = []
}

variable "vpc_cni_version" {
  description = "VPC CNI add-on version (null = the default for the cluster version). Network policy needs v1.14 or later."
  type        = string
  default     = null
}

variable "secrets_kms_key_arn" {
  description = "KMS key for envelope encryption of Kubernetes Secrets."
  type        = string
}

variable "logs_kms_key_arn" {
  description = "KMS key for the control-plane log group."
  type        = string
}

variable "log_retention_days" {
  description = "Retention for control-plane logs."
  type        = number
  default     = 30
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["t3.large"]
}

variable "node_capacity_type" {
  description = "ON_DEMAND or SPOT."
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "SPOT"], var.node_capacity_type)
    error_message = "node_capacity_type must be ON_DEMAND or SPOT."
  }
}

variable "node_min" {
  description = "Minimum nodes."
  type        = number
  default     = 1
}

variable "node_desired" {
  description = "Initial node count."
  type        = number
  default     = 2
}

variable "node_max" {
  description = "Maximum nodes."
  type        = number
  default     = 4
}

variable "node_disk_gb" {
  description = "Root volume size per node."
  type        = number
  default     = 50
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
