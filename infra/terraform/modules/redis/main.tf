# Managed Redis (ElastiCache) for the feature cache and the job queue.
# TLS in transit, encrypted at rest, an AUTH token kept in Secrets Manager,
# reachable only from the EKS cluster security group.
#
# The generated token is stored in Terraform state (random_password and the
# secret version both keep it). The state bucket must therefore be encrypted
# and readable only by the deploy role; see docs/cost-and-security.md.

resource "random_password" "auth" {
  length  = 48
  special = false # ElastiCache AUTH tokens reject several special characters
}

resource "aws_secretsmanager_secret" "auth" {
  #checkov:skip=CKV2_AWS_57:Rotation is a runbook step (ElastiCache needs a two-phase AUTH token change); see docs/runbook.md
  name                    = "${var.name}/redis-auth"
  description             = "AUTH token for the ${var.name} Redis replication group."
  kms_key_id              = var.kms_key_arn
  recovery_window_in_days = 7
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "auth" {
  secret_id     = aws_secretsmanager_secret.auth.id
  secret_string = random_password.auth.result
}

# volatile-lru evicts only keys that have a TTL. Cache entries always have
# one, and so do finished job records (7 days); queued and running jobs and
# the queue lists have none, so they are never evicted. Memory pressure can
# drop cached features (re-read on the next miss) or an old finished job
# record (its key could then run again), never pending work.
resource "aws_elasticache_parameter_group" "this" {
  name   = "${var.name}-redis7"
  family = "redis7"

  parameter {
    name  = "maxmemory-policy"
    value = "volatile-lru"
  }

  tags = var.tags
}

resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis"
  subnet_ids = var.subnet_ids
  tags       = var.tags
}

resource "aws_security_group" "redis" {
  name        = "${var.name}-redis"
  description = "Redis: TLS port from the EKS cluster only"
  vpc_id      = var.vpc_id
  tags        = var.tags
}

resource "aws_vpc_security_group_ingress_rule" "from_cluster" {
  security_group_id            = aws_security_group.redis.id
  description                  = "Redis TLS from EKS pods and nodes"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = var.allowed_security_group_id
}

resource "aws_elasticache_replication_group" "this" {
  #checkov:skip=CKV2_AWS_50:Dev defaults to one node (redis_replicas = 0) to save cost; replicas >= 1 turns on Multi-AZ failover
  replication_group_id = "${var.name}-redis"
  description          = "Feature cache and job queue"
  engine               = "redis"
  engine_version       = var.engine_version
  node_type            = var.node_type
  port                 = 6379
  parameter_group_name = aws_elasticache_parameter_group.this.name

  num_cache_clusters         = var.replicas + 1
  automatic_failover_enabled = var.replicas > 0
  multi_az_enabled           = var.replicas > 0

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.redis.id]

  at_rest_encryption_enabled = true
  kms_key_id                 = var.kms_key_arn
  transit_encryption_enabled = true
  auth_token                 = random_password.auth.result

  snapshot_retention_limit   = var.snapshot_retention_days
  auto_minor_version_upgrade = true
  apply_immediately          = false

  tags = var.tags
}
