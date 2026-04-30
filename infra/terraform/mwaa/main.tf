terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
  backend "s3" {
    bucket = "nextgen-databridge-terraform-state"
    key    = "nextgen-databridge/mwaa/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags { tags = local.common_tags }
}

locals {
  common_tags = {
    Project     = "NextGenDatabridge"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }

  # Deterministic bucket names — no data source needed
  mwaa_bucket             = "nextgen-databridge-mwaa-${var.environment}"
  duckdb_store_bucket     = "nextgen-databridge-duckdb-store-${var.environment}"
  pipeline_configs_bucket = "nextgen-databridge-pipeline-configs-${var.environment}"
  artifacts_bucket        = "nextgen-databridge-artifacts-${var.environment}"

  # Content hash of plugin source files — stable across zip regenerations.
  # archive_file produces non-deterministic zip metadata in CI (file timestamps
  # differ each fresh checkout), so etag = output_md5 would change every run.
  # source_hash bypasses the archive and hashes the actual source content instead.
  plugins_content_hash = sha256(join("", [
    for f in sort(fileset("${path.module}/../../../airflow/plugins", "**")) :
    filesha256("${path.module}/../../../airflow/plugins/${f}")
  ]))
}

# ─────────────────────────────────────────────────────────────────────────────
# Data sources — look up core resources by known name / tag
# ─────────────────────────────────────────────────────────────────────────────
data "aws_caller_identity" "current" {}

data "aws_vpc" "main" {
  tags = {
    Name      = "nextgen-databridge-vpc"
    ManagedBy = "Terraform"
  }
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.main.id]
  }
  filter {
    name   = "tag:kubernetes.io/role/internal-elb"
    values = ["1"]
  }
}

data "aws_eks_cluster" "main" {
  name = "nextgen-databridge"
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA S3 Bucket
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "mwaa" {
  bucket = local.mwaa_bucket
}
resource "aws_s3_bucket_versioning" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_public_access_block" "mwaa" {
  bucket                  = aws_s3_bucket.mwaa.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — MWAA execution role
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "mwaa_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa_execution" {
  name               = "nextgen-databridge-mwaa"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume.json
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "mwaa-execution"
  role = aws_iam_role.mwaa_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
        Resource = ["arn:aws:s3:::${local.mwaa_bucket}", "arn:aws:s3:::${local.mwaa_bucket}/*"]
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${local.duckdb_store_bucket}", "arn:aws:s3:::${local.duckdb_store_bucket}/*",
          "arn:aws:s3:::${local.pipeline_configs_bucket}", "arn:aws:s3:::${local.pipeline_configs_bucket}/*",
          "arn:aws:s3:::${local.artifacts_bucket}", "arn:aws:s3:::${local.artifacts_bucket}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:nextgen-databridge/*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
          "logs:GetLogEvents", "logs:GetLogRecord", "logs:GetLogGroupFields",
          "logs:GetQueryResults", "logs:DescribeLogGroups",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-nextgen-databridge-*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage",
        ]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      },
      {
        Effect    = "Allow"
        Action    = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"]
        Resource  = "*"
        Condition = { StringLike = { "kms:ViaService" = ["sqs.${var.aws_region}.amazonaws.com"] } }
      },
      {
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster"]
        Resource = data.aws_eks_cluster.main.arn
      },
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Security Group — MWAA
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_security_group" "mwaa" {
  name   = "nextgen-databridge-mwaa"
  vpc_id = data.aws_vpc.main.id

  ingress {
    description = "Self-referencing ingress for MWAA workers"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Grant the MWAA execution role permission to create/watch Jobs in the
# nextgen-databridge-jobs namespace. Without this the EKS API returns 401.
resource "aws_eks_access_entry" "mwaa" {
  cluster_name  = data.aws_eks_cluster.main.name
  principal_arn = aws_iam_role.mwaa_execution.arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "mwaa_edit" {
  cluster_name  = data.aws_eks_cluster.main.name
  principal_arn = aws_iam_role.mwaa_execution.arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"

  access_scope {
    type       = "namespace"
    namespaces = ["nextgen-databridge-jobs"]
  }

  depends_on = [aws_eks_access_entry.mwaa]
}

# Allow MWAA workers to call the EKS API server (port 443) so EKSJobOperator
# can submit Kubernetes Jobs without routing through load_incluster_config.
resource "aws_vpc_security_group_ingress_rule" "eks_from_mwaa" {
  security_group_id            = data.aws_eks_cluster.main.vpc_config[0].cluster_security_group_id
  referenced_security_group_id = aws_security_group.mwaa.id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"
  description                  = "MWAA workers to EKS API server"
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA S3 objects — plugins, requirements, startup script
# DAGs are uploaded by up-mwaa.sh (application code, not Terraform)
# ─────────────────────────────────────────────────────────────────────────────
data "archive_file" "mwaa_plugins" {
  type        = "zip"
  source_dir  = "${path.module}/../../../airflow/plugins"
  output_path = "${path.module}/plugins.zip"
}

resource "aws_s3_object" "mwaa_plugins" {
  bucket      = aws_s3_bucket.mwaa.id
  key         = "plugins.zip"
  source      = data.archive_file.mwaa_plugins.output_path
  source_hash = local.plugins_content_hash
}

resource "aws_s3_object" "mwaa_requirements" {
  bucket = aws_s3_bucket.mwaa.id
  key    = "requirements.txt"
  source = "${path.module}/../../../airflow/requirements.txt"
  etag   = filemd5("${path.module}/../../../airflow/requirements.txt")
}

# Startup script reads dynamic values (DB URL) from Secrets Manager at worker boot.
# Rendered from startup.sh.tpl which is committed with LF line endings (.gitattributes).
resource "aws_s3_object" "mwaa_startup" {
  bucket       = aws_s3_bucket.mwaa.id
  key          = "startup.sh"
  content_type = "text/x-sh"
  content = templatefile("${path.module}/startup.sh.tpl", {
    aws_region             = var.aws_region
    duckdb_store_bucket    = local.duckdb_store_bucket
    pipeline_configs_bucket = local.pipeline_configs_bucket
    environment            = var.environment
    ecr_registry           = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA Environment
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_mwaa_environment" "main" {
  name              = "nextgen-databridge"
  airflow_version   = "2.9.2"
  environment_class = var.environment == "production" ? "mw1.large" : "mw1.medium"
  min_workers       = var.environment == "production" ? 2 : 1
  max_workers       = var.environment == "production" ? 10 : 3

  execution_role_arn = aws_iam_role.mwaa_execution.arn
  source_bucket_arn  = aws_s3_bucket.mwaa.arn
  dag_s3_path        = "dags/"

  plugins_s3_path           = aws_s3_object.mwaa_plugins.key
  plugins_s3_object_version = aws_s3_object.mwaa_plugins.version_id

  requirements_s3_path           = aws_s3_object.mwaa_requirements.key
  requirements_s3_object_version = aws_s3_object.mwaa_requirements.version_id

  startup_script_s3_path           = aws_s3_object.mwaa_startup.key
  startup_script_s3_object_version = aws_s3_object.mwaa_startup.version_id

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = slice(tolist(data.aws_subnets.private.ids), 0, min(2, length(data.aws_subnets.private.ids)))
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  airflow_configuration_options = {
    "core.default_timezone" = "UTC"
    "core.parallelism"      = tostring(var.environment == "production" ? 32 : 16)
    "secrets.backend"       = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
    "secrets.backend_kwargs" = jsonencode({
      connections_prefix = "nextgen-databridge/connections"
      variables_prefix   = "nextgen-databridge/variables"
    })
  }

  webserver_access_mode = var.environment == "production" ? "PRIVATE_ONLY" : "PUBLIC_ONLY"

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────
output "mwaa_bucket"      { value = aws_s3_bucket.mwaa.id }
output "mwaa_webserver_url" { value = aws_mwaa_environment.main.webserver_url }
