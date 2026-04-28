variable "aws_region" {
  default = "us-east-1"
}

variable "environment" {
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Must be dev, staging, or production."
  }
}

variable "vpc_cidr" {
  default = "10.0.0.0/16"
}

variable "db_password" {
  sensitive   = true
  description = "Password for the PostgreSQL audit database (user: airflow)"
}

variable "developer_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "Additional CIDR blocks allowed direct RDS access (e.g. dev machine IPs)."
}
