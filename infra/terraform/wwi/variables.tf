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

variable "mssql_password" {
  sensitive   = true
  description = "Password for the RDS SQL Server instance (user: sqladmin). Min 8 chars, must include uppercase, lowercase, digit and special char."
}

variable "sqlserver_version" {
  default     = "15.00.4345.5.v1"
  description = "RDS SQL Server engine version. 15.00.x = SQL Server 2019 SE."
}

variable "vpc_cidr" {
  default = "10.0.0.0/16"
}

variable "developer_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "Additional CIDR blocks allowed direct SQL Server access."
}
