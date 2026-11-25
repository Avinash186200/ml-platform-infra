# One IAM role per workload, assumable only by its own Kubernetes service
# account through the cluster's OIDC provider (IRSA). Each role gets exactly
# the S3 prefixes it needs:
#
#   feature-api  read  features/*
#   model-api    read  models/*
#   jobs-worker  read  features/*            write jobs/*
#   dagster      read  raw/* features/* training/* models/*
#                write raw/* features/* training/* models/*
#
# Every role may list key names in this bucket (needed for 404 vs 403 on
# missing keys). No role can delete objects, touch other buckets, or read the Redis secret
# (that is synced into a Kubernetes Secret by the deploy step, see the runbook).

data "aws_iam_policy_document" "assume" {
  for_each = var.workloads

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${each.value.namespace}:${each.value.service_account}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "workload" {
  for_each             = var.workloads
  name                 = "${var.name}-${each.key}"
  assume_role_policy   = data.aws_iam_policy_document.assume[each.key].json
  max_session_duration = 3600
  tags                 = var.tags
}

data "aws_iam_policy_document" "access" {
  for_each = var.workloads

  # Plain ListBucket on this single-purpose bucket. Without it S3 answers a
  # read of a missing key (an absent pointer) with 403 instead of 404, and the
  # code cannot tell "not there yet" from "not allowed". It exposes key names
  # only, never content; object reads stay limited to the prefixes below.
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
  }

  statement {
    sid       = "ReadObjects"
    actions   = ["s3:GetObject"]
    resources = [for p in distinct(concat(each.value.read_prefixes, each.value.write_prefixes)) : "${var.bucket_arn}/${p}*"]
  }

  dynamic "statement" {
    for_each = length(each.value.write_prefixes) > 0 ? [1] : []
    content {
      sid       = "WriteObjects"
      actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
      resources = [for p in each.value.write_prefixes : "${var.bucket_arn}/${p}*"]
    }
  }

  statement {
    sid       = "UseBucketKey"
    actions   = length(each.value.write_prefixes) > 0 ? ["kms:Decrypt", "kms:GenerateDataKey"] : ["kms:Decrypt"]
    resources = [var.kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "access" {
  for_each = var.workloads
  name     = "artifact-access"
  role     = aws_iam_role.workload[each.key].id
  policy   = data.aws_iam_policy_document.access[each.key].json
}
