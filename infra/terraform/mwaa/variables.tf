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
