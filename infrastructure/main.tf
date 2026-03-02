provider "aws" {
  region = var.aws_region
}

# Pre-existing S3 bucket that acts as the Bronze layer
data "aws_s3_bucket" "bronze_bucket" {
  bucket = var.s3_bucket_name
}

# Assume Role Policy for Databricks to assume this IAM Role
data "aws_iam_policy_document" "databricks_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      # IMPORTANT: Replace this ARN with the specific AWS Account ID used by your Databricks deployment
      identifiers = ["arn:aws:iam::414360369270:role/databricks-cross-account-role"] 
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_workspace_id]
    }
  }
}

# Creates the IAM Role for Databricks S3 access
resource "aws_iam_role" "databricks_s3_access_role" {
  name               = "databricks-bronze-s3-access-role"
  assume_role_policy = data.aws_iam_policy_document.databricks_assume_role.json
}

# Least Privilege Policy: Access only to the specific S3 Bronze bucket
data "aws_iam_policy_document" "databricks_s3_policy_doc" {
  # List Bucket Permissions
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      data.aws_s3_bucket.bronze_bucket.arn
    ]
  }

  # Object level permissions (Read/Write)
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:PutObjectAcl"
    ]
    resources = [
      "${data.aws_s3_bucket.bronze_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "databricks_s3_policy" {
  name        = "databricks-s3-least-privilege"
  description = "Allows Databricks workspace restricted access to the specific Bronze S3 bucket"
  policy      = data.aws_iam_policy_document.databricks_s3_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "databricks_policy_attach" {
  role       = aws_iam_role.databricks_s3_access_role.name
  policy_arn = aws_iam_policy.databricks_s3_policy.arn
}

resource "aws_iam_instance_profile" "databricks_instance_profile" {
  name = "databricks-bronze-instance-profile"
  role = aws_iam_role.databricks_s3_access_role.name
}

output "instance_profile_arn" {
  description = "Use this Instance Profile ARN inside your Databricks Cluster configuration"
  value       = aws_iam_instance_profile.databricks_instance_profile.arn
}
output "iam_role_arn" {
  description = "IAM Role ARN for Databricks to assume"
  value       = aws_iam_role.databricks_s3_access_role.arn
}
