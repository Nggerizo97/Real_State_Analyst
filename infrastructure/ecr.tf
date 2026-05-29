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
# Referencia al usuario existente (github-scraper-bot) sin modificar su creación.

data "aws_iam_user" "github_bot" {
  user_name = "github-scraper-bot"
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
}

resource "aws_iam_policy" "github_bot_ecr_policy" {
  name        = "github-scraper-bot-ecr-policy"
  description = "Permisos ECR para github-scraper-bot → GitHub Actions (proyecto RealEstateAnalyst)"
  policy      = data.aws_iam_policy_document.github_bot_ecr_policy_doc.json
}

resource "aws_iam_user_policy_attachment" "github_bot_ecr_attach" {
  user       = data.aws_iam_user.github_bot.user_name
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
