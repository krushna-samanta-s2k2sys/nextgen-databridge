variable "aws_region" {
  default = "us-east-1"
}

variable "environment" {
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "production", "dr"], var.environment)
    error_message = "Must be dev, staging, production, or dr."
  }
}

variable "db_password" {
  sensitive   = true
  description = "Password for the PostgreSQL audit database (user: airflow). Min 8 chars."
}

variable "developer_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "Additional CIDR blocks allowed direct PostgreSQL access (e.g. dev machine IPs)."
}
