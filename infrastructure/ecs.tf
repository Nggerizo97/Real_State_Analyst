# infrastructure/ecs.tf
# ─────────────────────────────────────────────────────────────────────────────
# Amazon ECS (Fargate) — API FastAPI + Frontend Streamlit
#
# Sizing razonado (0.25 vCPU / 1 GiB cada servicio):
#   • La API carga el bundle XGBoost (~80-150 MB) + DuckDB en memoria.
#     DuckDB no descarga datos: hace predicate-pushdown directo a S3 via httpfs.
#     1 GiB es el mínimo seguro; con 512 MB el JVM de XGBoost puede OOM al init.
#   • Streamlit ocupa ~260 MB en boot según profiling. 1 GiB deja 750 MB de
#     margen para pandas, plotly y el model bundle si REA_API_URL no está activo.
#
# Costo estimado 24/7 en us-east-1 (mayo 2026):
#   Fargate vCPU: $0.04048/h   Fargate GB: $0.004445/h
#   Por tarea: (0.25 × $0.04048 + 1 × $0.004445) × 720 h/mes ≈ $10.49/mes
#   Dos tareas: ≈ $21/mes total
#
# Para bajar aún más: usa un EventBridge Scheduler que escale a 0 fuera del
# horario de uso (ej. 22:00-07:00) → hasta -40% del costo mensual.
# ─────────────────────────────────────────────────────────────────────────────

# ── Data sources de red ───────────────────────────────────────────────────────

data "aws_vpc" "main" {
  id = var.vpc_id
}

# ── CloudWatch Log Groups (retención 7 días = mínimo costo) ──────────────────

resource "aws_cloudwatch_log_group" "rea_api" {
  name              = "/ecs/rea-api"
  retention_in_days = 7

  tags = { Project = "RealEstateAnalyst", Service = "api" }
}

resource "aws_cloudwatch_log_group" "rea_streamlit" {
  name              = "/ecs/rea-streamlit"
  retention_in_days = 7

  tags = { Project = "RealEstateAnalyst", Service = "streamlit" }
}

# ── ECS Cluster ───────────────────────────────────────────────────────────────

resource "aws_ecs_cluster" "rea" {
  name = "rea-cluster"

  # Container Insights desactivado → ahorra ~$2-5/mes en CloudWatch
  # Actívalo sólo si necesitas métricas por contenedor: value = "enabled"
  setting {
    name  = "containerInsights"
    value = "disabled"
  }

  tags = { Project = "RealEstateAnalyst" }
}

# Capacidad Fargate como proveedor por defecto
resource "aws_ecs_cluster_capacity_providers" "rea" {
  cluster_name       = aws_ecs_cluster.rea.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }
}

# ── IAM: Task Execution Role ──────────────────────────────────────────────────
# Necesario para que ECS pueda:
#   • Descargar la imagen desde ECR
#   • Escribir logs en CloudWatch
#   • Leer secrets de AWS Secrets Manager (si se usan)

resource "aws_iam_role" "ecs_exec_role" {
  name = "rea-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })

  tags = { Project = "RealEstateAnalyst" }
}

resource "aws_iam_role_policy_attachment" "ecs_exec_managed" {
  role       = aws_iam_role.ecs_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ── IAM: Task Role ────────────────────────────────────────────────────────────
# Permisos que usa la APP en runtime. Principio de mínimo privilegio:
#   • S3: sólo lectura sobre el bucket bronce-scrap-date (Gold Parquet + modelo)
#   • No se pasan AWS_ACCESS_KEY_ID/SECRET como env vars; boto3 y DuckDB httpfs
#     los obtienen automáticamente desde el metadata endpoint de ECS (más seguro).

resource "aws_iam_role" "ecs_task_role" {
  name = "rea-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })

  tags = { Project = "RealEstateAnalyst" }
}

data "aws_iam_policy_document" "ecs_task_s3_doc" {
  # Listar el bucket (requerido por DuckDB para descubrir particiones Parquet)
  statement {
    sid       = "S3List"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [data.aws_s3_bucket.bronze_bucket.arn]
  }

  # Leer objetos (Gold Parquet + bundle del modelo)
  statement {
    sid       = "S3Read"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.bronze_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "ecs_task_s3_policy" {
  name        = "rea-ecs-task-s3-read"
  description = "Acceso de solo lectura al bucket S3 para contenedores ECS"
  policy      = data.aws_iam_policy_document.ecs_task_s3_doc.json
}

resource "aws_iam_role_policy_attachment" "ecs_task_s3_attach" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.ecs_task_s3_policy.arn
}

# ── Security Groups ───────────────────────────────────────────────────────────
# Usamos recursos separados para las reglas de ingress y egress para evitar
# la dependencia circular (API SG referencia al SG de Streamlit y viceversa).

resource "aws_security_group" "rea_api" {
  name        = "rea-api-sg"
  description = "FastAPI backend: inbound 8000 solo desde Streamlit, outbound HTTPS"
  vpc_id      = var.vpc_id

  # Sin regla de ingress aquí → se añade abajo con aws_security_group_rule
  # para romper la dependencia circular con rea_streamlit.

  egress {
    description = "HTTPS saliente (S3, ECR, CloudWatch)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "rea-api-sg", Project = "RealEstateAnalyst" }
}

resource "aws_security_group" "rea_streamlit" {
  name        = "rea-streamlit-sg"
  description = "Streamlit frontend: inbound 8501 desde internet, outbound libre"
  vpc_id      = var.vpc_id

  ingress {
    description = "UI publica"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Todo el trafico saliente (API interna + S3 + ECR)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "rea-streamlit-sg", Project = "RealEstateAnalyst" }
}

# Regla de ingress del API que referencia el SG de Streamlit (separada para
# evitar ciclos de dependencia entre los dos security_group resources).
resource "aws_security_group_rule" "api_from_streamlit" {
  type                     = "ingress"
  description              = "FastAPI desde Streamlit (8000)"
  from_port                = 8000
  to_port                  = 8000
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rea_api.id
  source_security_group_id = aws_security_group.rea_streamlit.id
}

# ── Cloud Map (Service Discovery) ────────────────────────────────────────────
# Permite que Streamlit acceda a la API con un nombre DNS fijo:
#   http://api.rea.local:8000
# Costo: prácticamente 0 para uso bajo (primeras 1000 consultas/mes gratis).

resource "aws_service_discovery_private_dns_namespace" "rea" {
  name        = "rea.local"
  description = "DNS interno ECS para RealEstateAnalyst"
  vpc         = var.vpc_id

  tags = { Project = "RealEstateAnalyst" }
}

resource "aws_service_discovery_service" "api" {
  name = "api"

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.rea.id
    routing_policy = "MULTIVALUE"

    dns_records {
      ttl  = 10   # TTL bajo para reaccionar rápido a reinicios de tareas
      type = "A"
    }
  }

  health_check_custom_config {
    failure_threshold = 1
  }

  tags = { Project = "RealEstateAnalyst" }
}

# ── Task Definitions ─────────────────────────────────────────────────────────

# ── API (FastAPI + DuckDB) ────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "rea_api" {
  family                   = "rea-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]

  # ┌─────────────────────────────────────────────────────────────────────┐
  # │ TAMAÑO ÓPTIMO                                                       │
  # │  • 256 CPU (0.25 vCPU): suficiente para FastAPI + DuckDB query SQL  │
  # │    DuckDB hace el filtrado en C++ antes de bajar datos de S3.        │
  # │  • 1024 MB (1 GiB): mínimo para cargar bundle XGBoost + DuckDB     │
  # │    Si en el futuro el bundle supera 300 MB, sube a 2048 MB.         │
  # └─────────────────────────────────────────────────────────────────────┘
  cpu    = "256"
  memory = "1024"

  execution_role_arn = aws_iam_role.ecs_exec_role.arn
  task_role_arn      = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([{
    name      = "rea-api"
    image     = "${aws_ecr_repository.rea_api.repository_url}:latest"
    essential = true

    portMappings = [{
      containerPort = 8000
      protocol      = "tcp"
      name          = "api-port"  # requerido para Service Connect (opcional)
    }]

    environment = [
      { name = "PYTHONPATH",  value = "/app/src" },
      { name = "AWS_REGION",  value = var.aws_region },
      { name = "S3_BUCKET",   value = var.s3_bucket_name },
      # Nota: NO se pasan AWS_ACCESS_KEY_ID ni AWS_SECRET_ACCESS_KEY.
      # boto3 y DuckDB httpfs usan automáticamente el IAM Task Role a través
      # del metadata endpoint de ECS (más seguro que env vars en texto plano).
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.rea_api.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -sf http://localhost:8000/health || exit 1"]
      interval    = 30
      timeout     = 10
      retries     = 3
      startPeriod = 90  # tiempo para cargar el bundle XGBoost desde S3
    }
  }])

  tags = { Project = "RealEstateAnalyst", Service = "api" }
}

# ── Streamlit (Frontend) ──────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "rea_streamlit" {
  family                   = "rea-streamlit"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]

  # ┌─────────────────────────────────────────────────────────────────────┐
  # │  256 CPU / 1024 MB: Streamlit ocupa ~260 MB en boot. Con REA_API_URL│
  # │  activo no carga el modelo localmente → 1 GiB es ampliamente seguro. │
  # └─────────────────────────────────────────────────────────────────────┘
  cpu    = "256"
  memory = "1024"

  execution_role_arn = aws_iam_role.ecs_exec_role.arn
  task_role_arn      = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([{
    name      = "rea-streamlit"
    image     = "${aws_ecr_repository.rea_streamlit.repository_url}:latest"
    essential = true

    portMappings = [{
      containerPort = 8501
      protocol      = "tcp"
    }]

    environment = [
      # Apunta al API via Cloud Map DNS (sin hardcodear IPs)
      { name = "REA_API_URL",      value = "http://api.rea.local:8000" },
      { name = "AWS_REGION",       value = var.aws_region },
      { name = "S3_BUCKET_NAME",   value = var.s3_bucket_name },
      # NOTA: Streamlit usa st.secrets para AWS creds. En ECS se inyectan
      # vía AWS Secrets Manager (ver variable streamlit_secrets_arn) o
      # mediante un script de entrypoint que crea .streamlit/secrets.toml.
      # Reemplaza el valor por el ARN de tu secreto en Secrets Manager:
      #   aws secretsmanager create-secret --name rea/streamlit \
      #     --secret-string '{"aws":{"aws_access_key_id":"...","aws_secret_access_key":"...","aws_region":"us-east-1"}}'
    ]

    secrets = var.streamlit_secrets_arn != "" ? [
      {
        name      = "STREAMLIT_SECRETS_JSON"
        valueFrom = var.streamlit_secrets_arn
      }
    ] : []

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.rea_streamlit.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -sf http://localhost:8501/_stcore/health || exit 1"]
      interval    = 30
      timeout     = 10
      retries     = 3
      startPeriod = 60
    }
  }])

  tags = { Project = "RealEstateAnalyst", Service = "streamlit" }
}

# ── ECS Services ──────────────────────────────────────────────────────────────

resource "aws_ecs_service" "rea_api" {
  name            = "rea-api"
  cluster         = aws_ecs_cluster.rea.id
  task_definition = aws_ecs_task_definition.rea_api.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # Ambas tareas en subnets públicas con IP pública → sin NAT Gateway (ahorra ~$32/mes)
  network_configuration {
    subnets          = var.public_subnet_ids
    security_groups  = [aws_security_group.rea_api.id]
    assign_public_ip = true   # permite a Fargate descargar imágenes de ECR sin NAT
  }

  # Registro en Cloud Map para que Streamlit encuentre la API por DNS
  service_registries {
    registry_arn = aws_service_discovery_service.api.arn
  }

  # Con desired_count=1: 0% mín garantiza rollout rápido sin dejar una tarea
  # vieja bloqueando el inicio de la nueva en instancias pequeñas.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 200

  # Evita recrear el servicio cuando sólo cambia la revisión de la task def
  # (los deployments se gestionan via CI/CD, no Terraform)
  lifecycle {
    ignore_changes = [task_definition, desired_count]
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_exec_managed,
    aws_iam_role_policy_attachment.ecs_task_s3_attach,
  ]

  tags = { Project = "RealEstateAnalyst", Service = "api" }
}

resource "aws_ecs_service" "rea_streamlit" {
  name            = "rea-streamlit"
  cluster         = aws_ecs_cluster.rea.id
  task_definition = aws_ecs_task_definition.rea_streamlit.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.public_subnet_ids
    security_groups  = [aws_security_group.rea_streamlit.id]
    assign_public_ip = true
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 200

  lifecycle {
    ignore_changes = [task_definition, desired_count]
  }

  depends_on = [
    aws_ecs_service.rea_api,
  ]

  tags = { Project = "RealEstateAnalyst", Service = "streamlit" }
}

# ── Auto Scaling ──────────────────────────────────────────────────────────────
# ┌──────────────────────────────────────────────────────────────────────────┐
# │ LÍMITE MÁXIMO = 2 TAREAS (protección anti-gasto accidental)              │
# │                                                                          │
# │ Con max=20 (valor por defecto de la consola), un bug en el código o un   │
# │ pico de errores podría lanzar 20 × $10.49 = $209/mes en minutos.         │
# │ Con max=2 el peor caso es $21/mes y atiende tráfico bajo perfectamente.  │
# └──────────────────────────────────────────────────────────────────────────┘

# Registrar los servicios ECS como targets de auto scaling
resource "aws_appautoscaling_target" "api" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.rea.name}/${aws_ecs_service.rea_api.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = 1
  max_capacity       = 2   # ← máximo 2 tareas (nunca 20)
}

resource "aws_appautoscaling_target" "streamlit" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.rea.name}/${aws_ecs_service.rea_streamlit.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = 1
  max_capacity       = 2   # ← máximo 2 tareas
}

# Política de escala: sube una tarea si CPU > 70% durante 2 minutos
# y la baja si CPU < 30% durante 5 minutos (evita flapping).
resource "aws_appautoscaling_policy" "api_cpu" {
  name               = "rea-api-cpu-scaling"
  service_namespace  = "ecs"
  resource_id        = aws_appautoscaling_target.api.resource_id
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension
  policy_type        = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    target_value       = 70.0   # escala si CPU promedio supera 70%
    scale_in_cooldown  = 300    # espera 5 min antes de reducir (evita oscilaciones)
    scale_out_cooldown = 60     # reacciona en 1 min al subir carga

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

resource "aws_appautoscaling_policy" "streamlit_cpu" {
  name               = "rea-streamlit-cpu-scaling"
  service_namespace  = "ecs"
  resource_id        = aws_appautoscaling_target.streamlit.resource_id
  scalable_dimension = aws_appautoscaling_target.streamlit.scalable_dimension
  policy_type        = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    target_value       = 70.0
    scale_in_cooldown  = 300
    scale_out_cooldown = 60

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "ecs_cluster_name" {
  value = aws_ecs_cluster.rea.name
}

output "api_service_discovery_dns" {
  description = "DNS interno para que Streamlit alcance la API"
  value       = "http://api.rea.local:8000"
}

output "streamlit_task_public_ip_hint" {
  description = "IP publica de Streamlit (obtener con: aws ecs describe-tasks)"
  value       = "aws ecs describe-tasks --cluster ${aws_ecs_cluster.rea.name} --tasks $(aws ecs list-tasks --cluster ${aws_ecs_cluster.rea.name} --service-name rea-streamlit --query 'taskArns[0]' --output text) --query 'tasks[0].attachments[0].details[?name==`publicIPv4Address`].value' --output text"
}

