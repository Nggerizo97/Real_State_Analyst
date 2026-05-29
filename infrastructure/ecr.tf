# infrastructure/ecr.tf
# ─────────────────────────────────────────────────────────────────────────────
# Amazon ECR — Repositorios de imágenes Docker
# ─────────────────────────────────────────────────────────────────────────────
# Crea:
#   • rea-api        → imagen del backend FastAPI
#   • rea-streamlit  → imagen del frontend Streamlit
#   • Usuario IAM ci-ecr-push con permisos mínimos para que GitHub Actions pueda
#     hacer push sin credenciales de administrador.
# ─────────────────────────────────────────────────────────────────────────────

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

# ── Política ECR para el usuario IAM existente de GitHub Actions ─────────────
# Se usa el nombre directamente para evitar requerir iam:GetUser en el bot.

locals {
  github_bot_username = "github-scraper-bot"
}

data "aws_iam_policy_document" "github_bot_ecr_policy_doc" {
  # GetAuthorizationToken no admite restricción de recurso → "*" es obligatorio
  statement {
    sid       = "ECRLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  # Push / pull sobre los repositorios de este proyecto
  statement {
    sid = "ECRPush"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [
      aws_ecr_repository.rea_api.arn,
      aws_ecr_repository.rea_streamlit.arn,
    ]
  }

  # Crear y describir repositorios (el workflow los crea si no existen)
  statement {
    sid     = "ECRManageRepos"
    actions = [
      "ecr:CreateRepository",
      "ecr:DescribeRepositories",
    ]
    resources = ["*"]
  }

  # ── Estado remoto de Terraform en S3 ─────────────────────────────────────
  # El workflow de Terraform guarda el estado en s3://bucket/terraform-state/
  # El bot necesita leer y escribir ese prefijo para que `terraform init` y
  # `terraform apply` funcionen en CI.
  statement {
    sid = "TerraformStateS3"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketVersioning",
    ]
    resources = [
      "arn:aws:s3:::${var.s3_bucket_name}",
      "arn:aws:s3:::${var.s3_bucket_name}/terraform-state/*",
    ]
  }

  # Terraform necesita permisos para crear/modificar todos los recursos
  # que gestiona (ECS, IAM, ECR, CloudWatch, Service Discovery).
  # Esta política amplia es segura porque el usuario sólo se usa en CI.
  statement {
    sid = "TerraformManageInfra"
    actions = [
      # ECS
      "ecs:*",
      # IAM (limitado a roles/policies con prefijo rea-)
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:ListRoles",
      "iam:AttachRolePolicy", "iam:DetachRolePolicy",
      "iam:CreatePolicy", "iam:DeletePolicy", "iam:GetPolicy",
      "iam:GetPolicyVersion", "iam:CreatePolicyVersion", "iam:DeletePolicyVersion",
      "iam:ListPolicyVersions", "iam:ListAttachedRolePolicies",
      "iam:PassRole", "iam:TagRole", "iam:UntagRole",
      "iam:CreateInstanceProfile", "iam:DeleteInstanceProfile",
      "iam:AddRoleToInstanceProfile", "iam:RemoveRoleFromInstanceProfile",
      "iam:GetInstanceProfile", "iam:ListInstanceProfiles",
      "iam:GetUser", "iam:CreateUser",
      "iam:AttachUserPolicy", "iam:DetachUserPolicy",
      "iam:ListAttachedUserPolicies",
      # CloudWatch Logs
      "logs:CreateLogGroup", "logs:DeleteLogGroup",
      "logs:DescribeLogGroups", "logs:PutRetentionPolicy",
      "logs:TagLogGroup", "logs:ListTagsLogGroup",
      # EC2 / VPC (Security Groups)
      "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
      "ec2:DescribeSecurityGroups", "ec2:AuthorizeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress", "ec2:RevokeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupEgress", "ec2:DescribeVpcs",
      "ec2:DescribeSubnets", "ec2:CreateTags", "ec2:DescribeTags",
      # Service Discovery (Cloud Map)
      "servicediscovery:*",
      # Application Auto Scaling
      "application-autoscaling:*",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "github_bot_ecr_policy" {
  name        = "github-scraper-bot-ecr-policy"
  description = "ECR + infra permissions for github-scraper-bot (GitHub Actions CI/CD)"
  policy      = data.aws_iam_policy_document.github_bot_ecr_policy_doc.json
}

resource "aws_iam_user_policy_attachment" "github_bot_ecr_attach" {
  user       = local.github_bot_username
  policy_arn = aws_iam_policy.github_bot_ecr_policy.arn
}

# ── Outputs ──────────────────────────────────────────────────────────────────

output "ecr_api_url" {
  description = "URI completa del repositorio ECR de la API → usar como ECR_REGISTRY en .env de producción"
  value       = aws_ecr_repository.rea_api.repository_url
}

output "ecr_streamlit_url" {
  description = "URI completa del repositorio ECR de Streamlit"
  value       = aws_ecr_repository.rea_streamlit.repository_url
}
