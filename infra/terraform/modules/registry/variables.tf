variable "name" {
  description = "Repository name."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for image encryption."
  type        = string
}

variable "keep_images" {
  description = "How many images to keep (older ones expire)."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
