output "role_arns" {
  description = "Workload name -> IAM role ARN (goes into the service account annotation)."
  value       = { for k, r in aws_iam_role.workload : k => r.arn }
}
