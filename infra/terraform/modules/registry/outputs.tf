output "repository_url" {
  description = "Image repository URL (push target; Kubernetes image prefix)."
  value       = aws_ecr_repository.this.repository_url
}

output "repository_arn" {
  description = "Repository ARN."
  value       = aws_ecr_repository.this.arn
}
