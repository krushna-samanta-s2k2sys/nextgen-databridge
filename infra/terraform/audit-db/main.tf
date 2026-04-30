terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket = "nextgen-databridge-terraform-state"
    key    = "nextgen-databridge/audit-db/terraform.tfstate"
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
}

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
# Security Group — PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_security_group" "postgres" {
  name   = "nextgen-databridge-postgres"
  vpc_id = data.aws_vpc.main.id

  ingress {
    description = "PostgreSQL from VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.main.cidr_block]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# RDS PostgreSQL — Audit database
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_db_subnet_group" "postgres" {
  name       = "nextgen-databridge-postgres"
  subnet_ids = data.aws_subnets.public.ids
}

resource "aws_db_instance" "main" {
  identifier            = "nextgen-databridge-postgres"
  engine                = "postgres"
  engine_version        = "15.10"
  instance_class        = var.environment == "production" ? "db.r6g.large" : "db.t3.medium"
  allocated_storage     = 100
  max_allocated_storage = 1000
  storage_encrypted     = true
  storage_type          = "gp3"
  db_name               = "airflow"
  username              = "airflow"
  password              = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  publicly_accessible    = true
  multi_az               = var.environment == "production"

  backup_retention_period      = 7
  skip_final_snapshot          = var.environment != "production"
  performance_insights_enabled = true
  deletion_protection          = var.environment == "production"
}

# ─────────────────────────────────────────────────────────────────────────────
# Secrets Manager — Audit DB connection info
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_secretsmanager_secret" "audit_db" {
  name                    = "nextgen-databridge/connections/audit_db"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "audit_db" {
  secret_id = aws_secretsmanager_secret.audit_db.id
  secret_string = jsonencode({
    conn_type = "postgresql"
    host      = aws_db_instance.main.address
    port      = 5432
    login     = "airflow"
    password  = var.db_password
    schema    = "airflow"
    url       = "postgresql://airflow:${var.db_password}@${aws_db_instance.main.address}:5432/airflow"
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────
output "audit_db_endpoint"    { value = aws_db_instance.main.address }
output "audit_db_secret_name" { value = aws_secretsmanager_secret.audit_db.name }
