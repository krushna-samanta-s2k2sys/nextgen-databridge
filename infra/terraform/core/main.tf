terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.0" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.25" }
    helm       = { source = "hashicorp/helm", version = "~> 2.12" }
  }
  backend "s3" {
    bucket = "nextgen-databridge-terraform-state"
    key    = "nextgen-databridge/core/terraform.tfstate"
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
  cluster_name = "nextgen-databridge"
}

# ─────────────────────────────────────────────────────────────────────────────
# VPC
# ─────────────────────────────────────────────────────────────────────────────
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "nextgen-databridge-vpc"
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = [for i, _ in slice(data.aws_availability_zones.available.names, 0, 3) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, _ in slice(data.aws_availability_zones.available.names, 0, 3) : cidrsubnet(var.vpc_cidr, 4, i + 10)]

  enable_nat_gateway   = true
  single_nat_gateway   = var.environment != "production"
  enable_dns_hostnames = true
  enable_dns_support   = true

  manage_default_network_acl    = false
  manage_default_route_table    = false
  manage_default_security_group = false

  public_subnet_tags = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"          = 1
    "kubernetes.io/cluster/nextgen-databridge" = "shared"
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
# S3 Buckets  (globally unique — keep env suffix, drop account-id suffix)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "duckdb_store" {
  bucket = "nextgen-databridge-duckdb-store-${var.environment}"
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
  bucket = "nextgen-databridge-pipeline-configs-${var.environment}"
}
resource "aws_s3_bucket_versioning" "pipeline_configs" {
  bucket = aws_s3_bucket.pipeline_configs.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "nextgen-databridge-artifacts-${var.environment}"
}

# ─────────────────────────────────────────────────────────────────────────────
# RDS PostgreSQL  (audit / Airflow metadata database)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_db_subnet_group" "postgres" {
  name       = "nextgen-databridge-postgres"
  subnet_ids = module.vpc.public_subnets
}

resource "aws_security_group" "rds" {
  name   = "nextgen-databridge-rds"
  vpc_id = module.vpc.vpc_id

  ingress {
    description = "PostgreSQL open access"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
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
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true
  multi_az               = var.environment == "production"

  backup_retention_period      = 7
  skip_final_snapshot          = var.environment != "production"
  performance_insights_enabled = true
  deletion_protection          = var.environment == "production"
}

# ─────────────────────────────────────────────────────────────────────────────
# Secrets Manager — audit DB connection (dynamic values from Terraform)
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
# ElastiCache Redis
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_elasticache_subnet_group" "main" {
  name       = "nextgen-databridge-redis"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_replication_group" "main" {
  replication_group_id       = "nextgen-databridge-redis"
  description                = "NextGenDatabridge Celery broker"
  node_type                  = "cache.t3.medium"
  num_cache_clusters         = var.environment == "production" ? 2 : 1
  engine_version             = "7.0"
  subnet_group_name          = aws_elasticache_subnet_group.main.name
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
}

resource "aws_secretsmanager_secret" "redis" {
  name                    = "nextgen-databridge/connections/redis"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "redis" {
  secret_id = aws_secretsmanager_secret.redis.id
  secret_string = jsonencode({
    host = aws_elasticache_replication_group.main.primary_endpoint_address
    port = 6379
    url  = "rediss://${aws_elasticache_replication_group.main.primary_endpoint_address}:6379"
  })
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
      values = [
        "system:serviceaccount:nextgen-databridge:nextgen-databridge-task-runner",
        "system:serviceaccount:nextgen-databridge-jobs:nextgen-databridge-task-runner",
      ]
    }
  }
}

resource "aws_iam_role" "task_runner" {
  name               = "nextgen-databridge-task-runner"
  assume_role_policy = data.aws_iam_policy_document.task_runner_assume.json
}

resource "aws_iam_role_policy" "task_runner" {
  name = "s3-secrets-access"
  role = aws_iam_role.task_runner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.duckdb_store.arn, "${aws_s3_bucket.duckdb_store.arn}/*",
          aws_s3_bucket.pipeline_configs.arn, "${aws_s3_bucket.pipeline_configs.arn}/*",
          aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:nextgen-databridge/*"
      },
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────────────────────────────────────
output "cluster_name"            { value = module.eks.cluster_name }
output "cluster_endpoint"        { value = module.eks.cluster_endpoint }
output "oidc_provider_arn"       { value = module.eks.oidc_provider_arn }
output "vpc_id"                  { value = module.vpc.vpc_id }
output "audit_db_endpoint"       { value = aws_db_instance.main.address }
output "redis_endpoint"          { value = aws_elasticache_replication_group.main.primary_endpoint_address }
output "duckdb_bucket"           { value = aws_s3_bucket.duckdb_store.id }
output "pipeline_configs_bucket" { value = aws_s3_bucket.pipeline_configs.id }
output "artifacts_bucket"        { value = aws_s3_bucket.artifacts.id }
output "api_ecr_url"             { value = aws_ecr_repository.api.repository_url }
output "ui_ecr_url"              { value = aws_ecr_repository.ui.repository_url }
output "transform_ecr_url"       { value = aws_ecr_repository.transform.repository_url }
