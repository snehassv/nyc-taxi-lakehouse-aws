# Step 1 - apply this BEFORE anything else.
# A monthly AWS budget that emails you when spend (or forecast spend) crosses limits.
# The first two budgets in an account are free.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile
}

variable "region" {
  type    = string
  default = "us-east-2"
}

variable "aws_profile" {
  type    = string
  default = "nyc-taxi"
}

variable "alert_email" {
  description = "Email address that receives budget alerts"
  type        = string
}

variable "monthly_limit_usd" {
  type    = number
  default = 5
}

resource "aws_budgets_budget" "monthly" {
  name         = "nyc-taxi-lakehouse-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Email at $1 actual spend (20% of $5) - an early warning.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 20
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  # Email when actual spend passes the limit.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  # Email when AWS forecasts the month will pass the limit.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

output "budget_name" {
  value = aws_budgets_budget.monthly.name
}
