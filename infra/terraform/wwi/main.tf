terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket = "nextgen-databridge-terraform-state"
    key    = "nextgen-databridge/wwi/terraform.tfstate"
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

  # Deterministic artifacts bucket name — created by the core module
  artifacts_bucket_arn = "arn:aws:s3:::nextgen-databridge-artifacts-${var.environment}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Data sources — resolve VPC created by core
# ─────────────────────────────────────────────────────────────────────────────
data "aws_caller_identity" "current" {}

data "aws_vpc" "main" {
  tags = {
    Name      = "nextgen-databridge-vpc"
    ManagedBy = "Terraform"
  }
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.main.id]
  }
  filter {
    name   = "tag:kubernetes.io/role/elb"
    values = ["1"]
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM Role — allows RDS to read the WideWorldImporters backup from S3
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_iam_role" "rds_s3_restore" {
  name = "nextgen-databridge-rds-s3-restore"
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
      Resource = [local.artifacts_bucket_arn, "${local.artifacts_bucket_arn}/*"]
    }]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# SQL Server option group — enables native backup/restore from S3
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_db_option_group" "sqlserver" {
  name                     = "nextgen-databridge-sqlserver"
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

# ─────────────────────────────────────────────────────────────────────────────
# Security Group — SQL Server
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_security_group" "sqlserver" {
  name   = "nextgen-databridge-sqlserver"
  vpc_id = data.aws_vpc.main.id

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

# ─────────────────────────────────────────────────────────────────────────────
# DB Subnet Group — public subnets (SQL Server is publicly accessible for dev)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_db_subnet_group" "sqlserver" {
  name       = "nextgen-databridge-sqlserver"
  subnet_ids = data.aws_subnets.public.ids
}

# ─────────────────────────────────────────────────────────────────────────────
# RDS SQL Server — WideWorldImporters source database
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_db_instance" "sqlserver" {
  identifier            = "nextgen-databridge-sqlserver"
  engine                = "sqlserver-se"
  engine_version        = var.sqlserver_version
  instance_class        = var.environment == "production" ? "db.r6g.xlarge" : "db.t3.xlarge"
  allocated_storage     = 100
  max_allocated_storage = 500
  storage_encrypted     = true
  storage_type          = "gp3"
  license_model         = "license-included"

  username = "sqladmin"
  password = var.mssql_password

  db_subnet_group_name   = aws_db_subnet_group.sqlserver.name
  vpc_security_group_ids = [aws_security_group.sqlserver.id]
  option_group_name      = aws_db_option_group.sqlserver.name

  multi_az                     = var.environment == "production"
  backup_retention_period      = 7
  skip_final_snapshot          = var.environment != "production"
  deletion_protection          = var.environment == "production"
  performance_insights_enabled = true
  publicly_accessible          = true
}

# ─────────────────────────────────────────────────────────────────────────────
# Secrets Manager — SQL Server connection credentials
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_secretsmanager_secret" "sqlserver" {
  name                    = "nextgen-databridge/connections/wwi_sqlserver"
  recovery_window_in_days = 0
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
# Outputs
# ─────────────────────────────────────────────────────────────────────────────
output "sqlserver_endpoint"     { value = aws_db_instance.sqlserver.address }
output "sqlserver_secret_name"  { value = aws_secretsmanager_secret.sqlserver.name }
output "artifacts_bucket"       { value = "nextgen-databridge-artifacts-${var.environment}" }
