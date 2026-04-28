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
  bucket = aws_s3_bucket.mwaa.id
  key    = "plugins.zip"
  source = data.archive_file.mwaa_plugins.output_path
  etag   = data.archive_file.mwaa_plugins.output_md5
}

resource "aws_s3_object" "mwaa_requirements" {
  bucket = aws_s3_bucket.mwaa.id
  key    = "requirements.txt"
  source = "${path.module}/../../../airflow/requirements.txt"
  etag   = filemd5("${path.module}/../../../airflow/requirements.txt")
}

# Startup script reads dynamic values (DB URL) from Secrets Manager at worker boot.
# Only static, code-level values (bucket names, env name) are embedded here.
resource "aws_s3_object" "mwaa_startup" {
  bucket       = aws_s3_bucket.mwaa.id
  key          = "startup.sh"
  content_type = "text/x-sh"
  content      = <<-SCRIPT
    #!/usr/bin/env bash
    _SM=$(aws secretsmanager get-secret-value \
      --region "${var.aws_region}" \
      --secret-id "nextgen-databridge/connections/audit_db" \
      --query SecretString --output text 2>/dev/null || echo '{}')
    export _SM
    export NEXTGEN_DATABRIDGE_AUDIT_DB_URL=$$(python3 -c \
      "import json,os; d=json.loads(os.environ['_SM']); print(d.get('url',''))")
    unset _SM
    export NEXTGEN_DATABRIDGE_DUCKDB_BUCKET="${local.duckdb_store_bucket}"
    export NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET="${local.pipeline_configs_bucket}"
    export NEXTGEN_DATABRIDGE_ENV="${var.environment}"
    export AWS_DEFAULT_REGION="${var.aws_region}"
    export EKS_CLUSTER_NAME="nextgen-databridge"
  SCRIPT
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
    subnet_ids         = slice(tolist(data.aws_subnets.private.ids), 0, 2)
  }

  logging_configuration {
    dag_processing_logs { enabled = true; log_level = "INFO" }
    scheduler_logs      { enabled = true; log_level = "INFO" }
    task_logs           { enabled = true; log_level = "INFO" }
    webserver_logs      { enabled = true; log_level = "INFO" }
    worker_logs         { enabled = true; log_level = "INFO" }
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
