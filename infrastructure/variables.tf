variable "aws_region" {
  description = "AWS region deployed to"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "The name of the existing S3 bucket intended as the Bronze layer"
  type        = string
  # Example: "bronce-scrap-date"
}

variable "databricks_workspace_id" {
  description = "The Workspace ID of your Databricks account"
  type        = string
}

# ── ECS / Networking ─────────────────────────────────────────────────────────

variable "vpc_id" {
  description = "ID de la VPC donde desplegar los servicios ECS (ej. vpc-0abc123)"
  type        = string
}

variable "public_subnet_ids" {
  description = "Lista de IDs de subnets públicas para las tareas Fargate (al menos 1, idealmente 2 para HA)"
  type        = list(string)
  # Example: ["subnet-0abc123", "subnet-0def456"]
}

variable "streamlit_secrets_arn" {
  description = <<-EOT
    ARN del secreto en AWS Secrets Manager que contiene las credenciales para
    st.secrets de Streamlit. Formato del secreto JSON:
      {"aws":{"aws_access_key_id":"...","aws_secret_access_key":"...","aws_region":"us-east-1"}}
    Dejar en "" si aún no está configurado (el contenedor arrancará sin inyección de secrets).
  EOT
  type        = string
  default     = ""
  sensitive   = true
}

