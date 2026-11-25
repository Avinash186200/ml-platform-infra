variable "name" {
  description = "Prefix for role names."
  type        = string
}

variable "region" {
  description = "AWS region (for the kms:ViaService condition)."
  type        = string
}

variable "oidc_provider_arn" {
  description = "EKS OIDC provider ARN."
  type        = string
}

variable "oidc_issuer" {
  description = "OIDC issuer host and path, without https://."
  type        = string
}

variable "bucket_arn" {
  description = "Artifact bucket ARN."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key that encrypts the bucket."
  type        = string
}

variable "workloads" {
  description = "Workload name -> service account and the S3 prefixes it may read and write."
  type = map(object({
    namespace       = string
    service_account = string
    read_prefixes   = list(string)
    write_prefixes  = list(string)
  }))

  validation {
    condition = alltrue([
      for w in values(var.workloads) : alltrue([
        for p in concat(w.read_prefixes, w.write_prefixes) : length(p) > 0 && endswith(p, "/")
      ])
    ])
    error_message = "Prefixes must be non-empty and end with '/', so no role can be granted the whole bucket."
  }
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
