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

# ── Usuario IAM para GitHub Actions (permisos mínimos) ───────────────────────

resource "aws_iam_user" "ci_ecr_push" {
  name = "ci-ecr-push"
  path = "/ci/"

  tags = {
    Project = "RealEstateAnalyst"
    Purpose = "GitHub Actions ECR push"
  }
}

# Access key (guarda el output en Secrets de GitHub)
resource "aws_iam_access_key" "ci_ecr_push" {
  user = aws_iam_user.ci_ecr_push.name
}

# Política con permisos mínimos: sólo GetAuthorizationToken + push a los repos de este proyecto
data "aws_iam_policy_document" "ci_ecr_push_policy_doc" {
  # Necesario para `docker login` a ECR (no admite restricción de recurso)
  statement {
    sid     = "ECRLogin"
    actions = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  # Operaciones de push sobre los repositorios del proyecto
  statement {
    sid = "ECRPush"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      # Necesario para `docker pull` de capas base ya existentes (acelera builds)
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [
      aws_ecr_repository.rea_api.arn,
      aws_ecr_repository.rea_streamlit.arn,
    ]
  }

  # Permitir crear repositorios (el workflow los crea si no existen)
  statement {
    sid     = "ECRCreateRepo"
    actions = ["ecr:CreateRepository", "ecr:DescribeRepositories"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "ci_ecr_push_policy" {
  name        = "ci-ecr-push-policy"
  description = "Permisos mínimos para GitHub Actions → ECR push (proyecto RealEstateAnalyst)"
  policy      = data.aws_iam_policy_document.ci_ecr_push_policy_doc.json
}

resource "aws_iam_user_policy_attachment" "ci_ecr_push_attach" {
  user       = aws_iam_user.ci_ecr_push.name
  policy_arn = aws_iam_policy.ci_ecr_push_policy.arn
}

# ── Outputs ──────────────────────────────────────────────────────────────────

output "ecr_api_url" {
  description = "URI del repositorio ECR de la API (usar como ECR_REGISTRY en .env de producción)"
  value       = aws_ecr_repository.rea_api.repository_url
}

output "ecr_streamlit_url" {
  description = "URI del repositorio ECR de Streamlit"
  value       = aws_ecr_repository.rea_streamlit.repository_url
}

output "ci_access_key_id" {
  description = "Access Key ID del usuario CI → copiar en GitHub Secret AWS_ACCESS_KEY_ID"
  value       = aws_iam_access_key.ci_ecr_push.id
  sensitive   = false
}

output "ci_secret_access_key" {
  description = "Secret Key del usuario CI → copiar en GitHub Secret AWS_SECRET_ACCESS_KEY"
  value       = aws_iam_access_key.ci_ecr_push.secret
  sensitive   = true   # sólo visible con: terraform output -raw ci_secret_access_key
}
