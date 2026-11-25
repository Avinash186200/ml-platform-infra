output "primary_endpoint" {
  description = "Primary endpoint address (use rediss:// — TLS is on)."
  value       = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "auth_secret_arn" {
  description = "Secrets Manager ARN holding the AUTH token (the value is never output)."
  value       = aws_secretsmanager_secret.auth.arn
}

output "security_group_id" {
  description = "Redis security group."
  value       = aws_security_group.redis.id
}
