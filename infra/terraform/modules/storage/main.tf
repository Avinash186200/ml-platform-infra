# The artifact bucket: raw data, feature snapshots, training sets, models.
# Private, versioned (so a deleted or overwritten pointer can be recovered),
# encrypted with a customer-managed key, TLS-only.

resource "aws_s3_bucket" "artifacts" {
  #checkov:skip=CKV_AWS_18:Server access logs not enabled in dev; CloudTrail data events are the prod plan (docs/cost-and-security.md)
  #checkov:skip=CKV2_AWS_62:No consumer for bucket events; pipelines are scheduled, not event-driven
  #checkov:skip=CKV_AWS_144:Single-region dev; everything in the bucket can be rebuilt from the pipeline
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
  tags          = var.tags
}

resource "aws_s3_bucket_ownership_controls" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # No age-based expiry on features/v*: if the nightly build stops, the
  # snapshot CURRENT points at keeps getting older, and an expiry rule would
  # delete the file every pod serves. Old snapshots are pruned by the deploy
  # role (docs/runbook.md), never the one CURRENT or STAGED names.

  depends_on = [aws_s3_bucket_versioning.artifacts]
}

data "aws_iam_policy_document" "tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "artifacts" {
  bucket     = aws_s3_bucket.artifacts.id
  policy     = data.aws_iam_policy_document.tls_only.json
  depends_on = [aws_s3_bucket_public_access_block.artifacts]
}
