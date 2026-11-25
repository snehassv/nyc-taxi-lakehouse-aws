variable "region" {
  type    = string
  default = "us-east-2"
}

variable "aws_profile" {
  description = "AWS CLI profile on your laptop (aws configure --profile nyc-taxi)"
  type        = string
  default     = "nyc-taxi"
}

variable "database" {
  type    = string
  default = "nyc_taxi"
}

variable "glue_workers" {
  description = "G.1X workers (1 DPU each). 2 is the minimum for a Spark job."
  type        = number
  default     = 2
}

variable "glue_execution_class" {
  description = "STANDARD, or FLEX (cheaper, may start slower)"
  type        = string
  default     = "STANDARD"
}

variable "athena_scan_limit_bytes" {
  description = "Athena cancels a query that scans more than this (10 GB)"
  type        = number
  default     = 10737418240
}
