variable "name" {
  description = "Prefix for resource names."
  type        = string
}

variable "region" {
  description = "AWS region, used for the S3 gateway endpoint service name."
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name, used in subnet discovery tags."
  type        = string
}

variable "cidr_block" {
  description = "VPC CIDR. Public subnets take the first /20s, private subnets start at the ninth."
  type        = string
  default     = "10.40.0.0/16"

  validation {
    condition     = can(cidrhost(var.cidr_block, 0)) && tonumber(split("/", var.cidr_block)[1]) <= 16
    error_message = "cidr_block must be a valid CIDR of /16 or larger."
  }
}

variable "az_count" {
  description = "Number of availability zones (2 or 3)."
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be 2 or 3."
  }
}

variable "log_retention_days" {
  description = "Retention for VPC flow logs."
  type        = number
  default     = 30
}

variable "logs_kms_key_arn" {
  description = "KMS key for the flow-log group."
  type        = string
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
