# infrastructure/ecr.tf
# ─────────────────────────────────────────────────────────────────────────────
# Amazon ECR — Repositorios de imágenes Docker
# ─────────────────────────────────────────────────────────────────────────────
# Los repos ya existen en ECR (creados por el workflow build-push-ecr.yml).
# Los bloques import los adoptan en el estado de Terraform sin recrearlos.
# ───────────────────────────────────────────────────────────────────────────

import {
  to = aws_ecr_repository.rea_api
  id = "rea-api"
}

import {
  to = aws_ecr_repository.rea_streamlit
  id = "rea-streamlit"
}

# ── Repositorios ECR ─────────────────────────────────────────────────────────

resource "aws_ecr_repository" "rea_api" {
  name                 = "rea-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project = "RealEstateAnalyst"
    Service = "api"
  }
}

resource "aws_ecr_repository" "rea_streamlit" {
  name                 = "rea-streamlit"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project = "RealEstateAnalyst"
    Service = "streamlit"
  }
}

# Política de ciclo de vida: mantener sólo las últimas 10 imágenes por repo
resource "aws_ecr_lifecycle_policy" "rea_api_lifecycle" {
  repository = aws_ecr_repository.rea_api.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecr_lifecycle_policy" "rea_streamlit_lifecycle" {
  repository = aws_ecr_repository.rea_streamlit.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# ── Outputs ──────────────────────────────────────────────────────────────────
# NOTA: los permisos del bot (github-scraper-bot) se gestionan manualmente en
# AWS IAM Console, no por Terraform. Esto evita el bootstrapping circular donde
# el bot necesita permisos para crear la política que le otorga permisos.
# Políticas requeridas (adjuntar en IAM → Users → github-scraper-bot):
#   AmazonECR_FullAccess, AmazonECS_FullAccess, IAMFullAccess,
#   AmazonEC2FullAccess, CloudWatchLogsFullAccess,
#   AWSCloudMapFullAccess, AutoScalingFullAccess

output "ecr_api_url" {
  description = "ECR repository URL for the API image"
  value       = aws_ecr_repository.rea_api.repository_url
}

output "ecr_streamlit_url" {
  description = "ECR repository URL for the Streamlit image"
  value       = aws_ecr_repository.rea_streamlit.repository_url
}
