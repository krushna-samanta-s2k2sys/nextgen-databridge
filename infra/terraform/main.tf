terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws        = { source = "hashicorp/aws",        version = "~> 5.0" }
    kubernetes = { source = "hashicorp/kubernetes",  version = "~> 2.25" }
    helm       = { source = "hashicorp/helm",        version = "~> 2.12" }
    archive    = { source = "hashicorp/archive",     version = "~> 2.4" }
  }
  backend "s3" {
    bucket = "nextgen-databridge-terraform-state"
    key    = "nextgen-databridge/terraform.tfstate"
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
  cluster_name = "nextgen-databridge-${var.environment}"
}

# ─────────────────────────────────────────────────────────────────────────────
# VPC
# ─────────────────────────────────────────────────────────────────────────────
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${local.cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = [for i, az in slice(data.aws_availability_zones.available.names, 0, 3) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, az in slice(data.aws_availability_zones.available.names, 0, 3) : cidrsubnet(var.vpc_cidr, 4, i + 10)]

  enable_nat_gateway     = true
  single_nat_gateway     = var.environment != "production"
  enable_dns_hostnames   = true
  enable_dns_support     = true

  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"                          = 1
    "kubernetes.io/cluster/${local.cluster_name}"              = "shared"
  }
}

data "aws_availability_zones" "available" {}
data "aws_caller_identity" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# EKS Cluster
# ─────────────────────────────────────────────────────────────────────────────
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = local.cluster_name
  cluster_version = "1.31"

  vpc_id                         = module.vpc.vpc_id
  subnet_ids                     = module.vpc.private_subnets
  cluster_endpoint_public_access = true

  enable_cluster_creator_admin_permissions = true

  cluster_addons = {
    coredns    = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni    = { most_recent = true }
  }

  eks_managed_node_groups = {
    system = {
      name           = "system"
      instance_types = ["m5.large"]
      min_size       = 2
      max_size       = 4
      desired_size   = 2
      labels         = { role = "system" }
    }
    jobs = {
      name           = "jobs"
      instance_types = ["r5.xlarge", "r5.2xlarge"]
      min_size       = 0
      max_size       = 20
      desired_size   = 1
      capacity_type  = "SPOT"
      labels         = { role = "nextgen-databridge-job" }
      taints         = [{ key = "nextgen-databridge-job", value = "true", effect = "NO_SCHEDULE" }]
    }
  }

  enable_irsa = true
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 Buckets
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "duckdb_store" {
  bucket = "nextgen-databridge-duckdb-store-${var.environment}-${data.aws_caller_identity.current.account_id}"
}
resource "aws_s3_bucket_versioning" "duckdb_store" {
  bucket = aws_s3_bucket.duckdb_store.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_lifecycle_configuration" "duckdb_store" {
  bucket = aws_s3_bucket.duckdb_store.id
  rule {
    id     = "expire-intermediate"
    status = "Enabled"
    filter { prefix = "pipelines/" }
    expiration { days = 30 }
  }
}

resource "aws_s3_bucket" "pipeline_configs" {
  bucket = "nextgen-databridge-pipeline-configs-${var.environment}-${data.aws_caller_identity.current.account_id}"
}
resource "aws_s3_bucket_versioning" "pipeline_configs" {
  bucket = aws_s3_bucket.pipeline_configs.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "nextgen-databridge-artifacts-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

# ─────────────────────────────────────────────────────────────────────────────
# RDS PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_db_subnet_group" "main" {
  name       = "${local.cluster_name}-db-subnet"
  subnet_ids = module.vpc.private_subnets
}

# Public subnet group — used when publicly_accessible = true so RDS gets a reachable public IP
resource "aws_db_subnet_group" "public" {
  name       = "${local.cluster_name}-db-subnet-public"
  subnet_ids = module.vpc.public_subnets
}

resource "aws_security_group" "rds" {
  name   = "${local.cluster_name}-rds-sg"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  dynamic "ingress" {
    for_each = length(var.developer_cidr_blocks) > 0 ? [1] : []
    content {
      description = "PostgreSQL from developer machines"
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      cidr_blocks = var.developer_cidr_blocks
    }
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "main" {
  identifier           = "${local.cluster_name}-postgres"
  engine               = "postgres"
  engine_version       = "15.10"
  instance_class       = var.environment == "production" ? "db.r6g.large" : "db.t3.medium"
  allocated_storage    = 100
  max_allocated_storage = 1000
  storage_encrypted    = true
  storage_type         = "gp3"
  db_name              = "airflow"
  username             = "airflow"
  password             = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.public.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true
  multi_az               = var.environment == "production"
  backup_retention_period = 7
  skip_final_snapshot    = var.environment != "production"
  performance_insights_enabled = true
  deletion_protection    = var.environment == "production"
}

# ─────────────────────────────────────────────────────────────────────────────
# ElastiCache Redis
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_elasticache_subnet_group" "main" {
  name       = "${local.cluster_name}-redis"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_replication_group" "main" {
  replication_group_id = "${local.cluster_name}-redis"
  description          = "NextGenDatabridge Celery broker"
  node_type            = "cache.t3.medium"
  num_cache_clusters   = var.environment == "production" ? 2 : 1
  engine_version       = "7.0"
  subnet_group_name    = aws_elasticache_subnet_group.main.name
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
}

# ─────────────────────────────────────────────────────────────────────────────
# ECR Repositories
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "api" {
  name                 = "nextgen-databridge/api"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  force_delete = true
}

resource "aws_ecr_repository" "ui" {
  name                 = "nextgen-databridge/ui"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  force_delete = true
}

resource "aws_ecr_repository" "transform" {
  name                 = "nextgen-databridge/transform"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  force_delete = true
}

# ─────────────────────────────────────────────────────────────────────────────
# RDS SQL Server — WideWorldImporters source database
# ─────────────────────────────────────────────────────────────────────────────

# IAM role so RDS can read the WideWorldImporters backup from S3
resource "aws_iam_role" "rds_s3_restore" {
  name = "nextgen-databridge-rds-s3-restore-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "rds.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "rds_s3_restore" {
  name = "s3-backup-restore"
  role = aws_iam_role.rds_s3_restore.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
      Resource = [
        aws_s3_bucket.artifacts.arn,
        "${aws_s3_bucket.artifacts.arn}/*",
      ]
    }]
  })
}

# Option group enables native SQL Server backup/restore from S3
resource "aws_db_option_group" "sqlserver" {
  name                     = "nextgen-databridge-sqlserver-${var.environment}"
  option_group_description = "SQL Server backup/restore from S3 for NextGenDatabridge"
  engine_name              = "sqlserver-se"
  major_engine_version     = "15.00"

  option {
    option_name = "SQLSERVER_BACKUP_RESTORE"
    option_settings {
      name  = "IAM_ROLE_ARN"
      value = aws_iam_role.rds_s3_restore.arn
    }
  }

  lifecycle { create_before_destroy = true }
}

resource "aws_security_group" "sqlserver" {
  name   = "${local.cluster_name}-sqlserver-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    description = "SQL Server from VPC"
    from_port   = 1433
    to_port     = 1433
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  dynamic "ingress" {
    for_each = length(var.developer_cidr_blocks) > 0 ? [1] : []
    content {
      description = "SQL Server from developer machines"
      from_port   = 1433
      to_port     = 1433
      protocol    = "tcp"
      cidr_blocks = var.developer_cidr_blocks
    }
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "sqlserver" {
  identifier            = "${local.cluster_name}-sqlserver"
  engine                = "sqlserver-se"
  engine_version        = var.sqlserver_version
  instance_class        = var.environment == "production" ? "db.r6g.xlarge" : "db.t3.xlarge"
  allocated_storage     = 100
  max_allocated_storage = 500
  storage_encrypted     = true
  storage_type          = "gp3"
  license_model         = "license-included"

  # SQL Server on RDS does not support setting db_name at creation time
  username = "sqladmin"
  password = var.mssql_password

  db_subnet_group_name   = aws_db_subnet_group.public.name
  vpc_security_group_ids = [aws_security_group.sqlserver.id]
  option_group_name      = aws_db_option_group.sqlserver.name

  multi_az                     = var.environment == "production"
  backup_retention_period      = 7
  skip_final_snapshot          = var.environment != "production"
  deletion_protection          = var.environment == "production"
  performance_insights_enabled = true
  publicly_accessible          = true

  tags = merge(local.common_tags, { Name = "${local.cluster_name}-sqlserver" })
}

# ─────────────────────────────────────────────────────────────────────────────
# Secrets Manager — SQL Server connection credentials
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_secretsmanager_secret" "sqlserver" {
  # Path matches the MWAA Secrets Manager backend connections_prefix
  name                    = "nextgen-databridge/connections/wwi_sqlserver"
  recovery_window_in_days = 0  # allow immediate deletion on destroy
}

resource "aws_secretsmanager_secret_version" "sqlserver" {
  secret_id = aws_secretsmanager_secret.sqlserver.id
  secret_string = jsonencode({
    conn_type = "mssql"
    host      = aws_db_instance.sqlserver.address
    port      = 1433
    login     = "sqladmin"
    password  = var.mssql_password
    schema    = "WideWorldImporters"
  })
}

resource "aws_secretsmanager_secret" "sqlserver_target" {
  name                    = "nextgen-databridge/connections/wwi_sqlserver_target"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "sqlserver_target" {
  secret_id = aws_secretsmanager_secret.sqlserver_target.id
  secret_string = jsonencode({
    conn_type = "mssql"
    host      = aws_db_instance.sqlserver.address
    port      = 1433
    login     = "sqladmin"
    password  = var.mssql_password
    schema    = "TargetDB"
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Environment config — written to S3 so config_loader.py can read it
# Terraform fills in all the resolved endpoint/bucket/registry values.
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_object" "env_config" {
  bucket       = aws_s3_bucket.pipeline_configs.id
  key          = "environments/${var.environment}.json"
  content_type = "application/json"
  content = jsonencode({
    environment = var.environment
    connections = {
      wwi_sqlserver = {
        airflow_conn_id = "wwi_sqlserver"
        type            = "mssql"
        host            = aws_db_instance.sqlserver.address
        port            = 1433
        database        = "WideWorldImporters"
        secret_arn      = aws_secretsmanager_secret.sqlserver.arn
      }
      wwi_sqlserver_target = {
        airflow_conn_id = "wwi_sqlserver_target"
        type            = "mssql"
        host            = aws_db_instance.sqlserver.address
        port            = 1433
        database        = "TargetDB"
        secret_arn      = aws_secretsmanager_secret.sqlserver_target.arn
      }
    }
    s3_buckets = {
      duckdb_store     = aws_s3_bucket.duckdb_store.id
      pipeline_configs = aws_s3_bucket.pipeline_configs.id
      artifacts        = aws_s3_bucket.artifacts.id
      mwaa             = aws_s3_bucket.mwaa.id
    }
    container_registries = {
      api       = aws_ecr_repository.api.repository_url
      transform = aws_ecr_repository.transform.repository_url
      ui        = aws_ecr_repository.ui.repository_url
    }
    secrets = {
      wwi_sqlserver        = aws_secretsmanager_secret.sqlserver.arn
      wwi_sqlserver_target = aws_secretsmanager_secret.sqlserver_target.arn
    }
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline configs — upload from configs/pipelines/ to S3 active/ prefix
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_object" "pipeline_wwi_sales" {
  bucket       = aws_s3_bucket.pipeline_configs.id
  key          = "active/wwi_sales_etl.json"
  source       = "${path.module}/../../configs/pipelines/wwi_sales_etl.json"
  content_type = "application/json"
  etag         = filemd5("${path.module}/../../configs/pipelines/wwi_sales_etl.json")
}

resource "aws_s3_object" "pipeline_wwi_inventory" {
  bucket       = aws_s3_bucket.pipeline_configs.id
  key          = "active/wwi_inventory_etl.json"
  source       = "${path.module}/../../configs/pipelines/wwi_inventory_etl.json"
  content_type = "application/json"
  etag         = filemd5("${path.module}/../../configs/pipelines/wwi_inventory_etl.json")
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM Role for EKS task runner (IRSA)
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "task_runner_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:nextgen-databridge:nextgen-databridge-task-runner",
                  "system:serviceaccount:nextgen-databridge-jobs:nextgen-databridge-task-runner"]
    }
  }
}

resource "aws_iam_role" "task_runner" {
  name               = "nextgen-databridge-task-runner-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.task_runner_assume.json
}

resource "aws_iam_role_policy" "task_runner_s3" {
  name = "s3-access"
  role = aws_iam_role.task_runner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.duckdb_store.arn,
          "${aws_s3_bucket.duckdb_store.arn}/*",
          aws_s3_bucket.pipeline_configs.arn,
          "${aws_s3_bucket.pipeline_configs.arn}/*",
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:nextgen-databridge/*"
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA — S3 bucket (versioning required by MWAA)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "mwaa" {
  bucket = "nextgen-databridge-mwaa-${var.environment}-${data.aws_caller_identity.current.account_id}"
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
# MWAA — IAM execution role
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
  name               = "nextgen-databridge-mwaa-execution-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume.json
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "mwaa-execution"
  role = aws_iam_role.mwaa_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # MWAA bucket — DAGs, plugins, requirements
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
        Resource = [aws_s3_bucket.mwaa.arn, "${aws_s3_bucket.mwaa.arn}/*"]
      },
      # DuckDB store & pipeline config buckets
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.duckdb_store.arn,
          "${aws_s3_bucket.duckdb_store.arn}/*",
          aws_s3_bucket.pipeline_configs.arn,
          "${aws_s3_bucket.pipeline_configs.arn}/*",
          aws_s3_bucket.artifacts.arn,
          "${aws_s3_bucket.artifacts.arn}/*",
        ]
      },
      # Secrets Manager — DB URLs, connection credentials
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:nextgen-databridge/*"
      },
      # CloudWatch Logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
          "logs:GetLogEvents", "logs:GetLogRecord", "logs:GetLogGroupFields",
          "logs:GetQueryResults", "logs:DescribeLogGroups",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-nextgen-databridge-*"
      },
      # CloudWatch metrics
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      # SQS — Celery broker used internally by MWAA
      {
        Effect   = "Allow"
        Action   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      },
      # KMS — MWAA environment encryption
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"]
        Resource = "*"
        Condition = {
          StringLike = { "kms:ViaService" = ["sqs.${var.aws_region}.amazonaws.com"] }
        }
      },
      # EKS — submit jobs from EKSJobOperator
      {
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster"]
        Resource = module.eks.cluster_arn
      },
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA — security group (outbound only; MWAA workers need internet/VPC access)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_security_group" "mwaa" {
  name   = "${local.cluster_name}-mwaa-sg"
  vpc_id = module.vpc.vpc_id

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
# MWAA — plugins.zip and requirements.txt (must exist in bucket before env create)
# ─────────────────────────────────────────────────────────────────────────────
data "archive_file" "mwaa_plugins" {
  type        = "zip"
  source_dir  = "${path.module}/../../airflow/plugins"
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
  source = "${path.module}/../../airflow/requirements.txt"
  etag   = filemd5("${path.module}/../../airflow/requirements.txt")
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA — startup script (sets OS env vars visible to all workers/schedulers)
# MWAA sources this script before starting Airflow components.
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_object" "mwaa_startup" {
  bucket       = aws_s3_bucket.mwaa.id
  key          = "startup.sh"
  content_type = "text/x-sh"
  content      = <<-SCRIPT
    export NEXTGEN_DATABRIDGE_AUDIT_DB_URL="postgresql://airflow:${var.db_password}@${aws_db_instance.main.address}:5432/airflow"
    export NEXTGEN_DATABRIDGE_DUCKDB_BUCKET="${aws_s3_bucket.duckdb_store.id}"
    export NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET="${aws_s3_bucket.pipeline_configs.id}"
    export NEXTGEN_DATABRIDGE_ENV="${var.environment}"
    export AWS_DEFAULT_REGION="${var.aws_region}"
    export EKS_CLUSTER_NAME="${module.eks.cluster_name}"
  SCRIPT
}

# ─────────────────────────────────────────────────────────────────────────────
# MWAA — environment
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_mwaa_environment" "main" {
  name               = "nextgen-databridge-${var.environment}"
  airflow_version    = "2.9.2"
  environment_class  = var.environment == "production" ? "mw1.large" : "mw1.medium"
  min_workers        = var.environment == "production" ? 2 : 1
  max_workers        = var.environment == "production" ? 10 : 3
  execution_role_arn = aws_iam_role.mwaa_execution.arn

  source_bucket_arn    = aws_s3_bucket.mwaa.arn
  dag_s3_path          = "dags/"
  plugins_s3_path         = aws_s3_object.mwaa_plugins.key
  plugins_s3_object_version    = aws_s3_object.mwaa_plugins.version_id
  requirements_s3_path    = aws_s3_object.mwaa_requirements.key
  requirements_s3_object_version = aws_s3_object.mwaa_requirements.version_id

  startup_script_s3_path             = aws_s3_object.mwaa_startup.key
  startup_script_s3_object_version   = aws_s3_object.mwaa_startup.version_id

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = slice(module.vpc.private_subnets, 0, 2)
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
output "cluster_name"             { value = module.eks.cluster_name }
output "cluster_endpoint"         { value = module.eks.cluster_endpoint }
output "audit_db_endpoint"        { value = aws_db_instance.main.address }
output "redis_endpoint"           { value = aws_elasticache_replication_group.main.primary_endpoint_address }
output "sqlserver_endpoint"       { value = aws_db_instance.sqlserver.address }
output "sqlserver_secret_arn"     { value = aws_secretsmanager_secret.sqlserver.arn }
output "duckdb_bucket"            { value = aws_s3_bucket.duckdb_store.id }
output "pipeline_configs_bucket"  { value = aws_s3_bucket.pipeline_configs.id }
output "artifacts_bucket"         { value = aws_s3_bucket.artifacts.id }
output "mwaa_bucket"              { value = aws_s3_bucket.mwaa.id }
output "mwaa_webserver_url"       { value = aws_mwaa_environment.main.webserver_url }
output "api_ecr_url"              { value = aws_ecr_repository.api.repository_url }
output "ui_ecr_url"               { value = aws_ecr_repository.ui.repository_url }
output "transform_ecr_url"        { value = aws_ecr_repository.transform.repository_url }
