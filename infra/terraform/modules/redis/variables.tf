variable "name" {
  description = "Prefix for resource names."
  type        = string
}

variable "vpc_id" {
  description = "VPC for the Redis security group."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnets for the cache nodes."
  type        = list(string)
}

variable "allowed_security_group_id" {
  description = "Only this security group (the EKS cluster SG) may connect."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for at-rest encryption and the AUTH secret."
  type        = string
}

variable "engine_version" {
  description = "Redis engine version."
  type        = string
  default     = "7.1"
}

variable "node_type" {
  description = "Cache node type."
  type        = string
  default     = "cache.t4g.small"
}

variable "replicas" {
  description = "Read replicas. 0 = single node (dev); 1+ enables Multi-AZ failover."
  type        = number
  default     = 1

  validation {
    condition     = var.replicas >= 0 && var.replicas <= 5
    error_message = "replicas must be between 0 and 5."
  }
}

variable "snapshot_retention_days" {
  description = "Daily snapshots kept. The job queue lives here too, so keep at least 1."
  type        = number
  default     = 1
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
