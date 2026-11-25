output "cluster_name" {
  description = "EKS cluster name (aws eks update-kubeconfig --name ...)."
  value       = module.eks.cluster_name
}

output "artifact_bucket" {
  description = "Artifact bucket; MLPI_ARTIFACTS_URL is s3://<this>/."
  value       = module.storage.bucket_name
}

output "image_repository" {
  description = "Push the platform image here; the dev overlay's images entry points at it."
  value       = module.registry.repository_url
}

output "redis_endpoint" {
  description = "Redis primary endpoint; MLPI_REDIS_URL is rediss://<this>:6379/0."
  value       = module.redis.primary_endpoint
}

output "redis_auth_secret_arn" {
  description = "Secrets Manager ARN of the Redis AUTH token (value not output)."
  value       = module.redis.auth_secret_arn
}

output "workload_role_arns" {
  description = "IRSA role per workload, for the service-account annotations in k8s/overlays/dev."
  value       = module.iam.role_arns
}
