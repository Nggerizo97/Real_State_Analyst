variable "aws_region" {
  description = "AWS region deployed to"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "The name of the existing S3 bucket intended as the Bronze layer"
  type        = string
  # Example: "real-state-data-bronze"
}

variable "databricks_workspace_id" {
  description = "The Workspace ID of your Databricks account"
  type        = string
}
