# Step 2 - the lakehouse: one S3 bucket, a Glue database, a Glue PySpark job,
# and an Athena workgroup with a per-query scan limit.
#
# `terraform destroy` removes everything, including all data in the bucket
# (force_destroy = true) and the Iceberg tables' catalog entries.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile
  default_tags {
    tags = { project = "nyc-taxi-lakehouse-aws" }
  }
}

data "aws_caller_identity" "me" {}

locals {
  bucket   = "nyc-taxi-lakehouse-${data.aws_caller_identity.me.account_id}"
  glue_dir = "${path.module}/../../glue_jobs"
}

# ------------------------------------------------------------------ S3
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
  rule {
    id     = "expire-glue-temp"
    status = "Enabled"
    filter { prefix = "glue-temp/" }
    expiration { days = 3 }
  }
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/load_trips.py"
  source = "${local.glue_dir}/load_trips.py"
  etag   = filemd5("${local.glue_dir}/load_trips.py")
}

resource "aws_s3_object" "glue_lib" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/schema_utils.py"
  source = "${local.glue_dir}/schema_utils.py"
  etag   = filemd5("${local.glue_dir}/schema_utils.py")
}

# ------------------------------------------------------------------ Glue catalog
resource "aws_glue_catalog_database" "taxi" {
  name = var.database
}

# ------------------------------------------------------------------ IAM role for Glue
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "nyc-taxi-glue-job"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_access" {
  statement {
    sid       = "LakeBucket"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid = "IcebergCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:database/${var.database}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:table/${var.database}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_access" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_access.json
}

# ------------------------------------------------------------------ Glue job
resource "aws_glue_job" "load_trips" {
  name              = "nyc-taxi-load-trips"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  execution_class   = var.glue_execution_class
  timeout           = 45 # minutes - a stuck job can't run up a bill
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--datalake-formats" = "iceberg"
    "--extra-py-files"   = "s3://${local.bucket}/${aws_s3_object.glue_lib.key}"
    "--TempDir"          = "s3://${local.bucket}/glue-temp/"
    "--bucket"           = local.bucket
    "--database"         = var.database
    "--raw_prefix"       = "raw/yellow/"
    "--reset"            = "false"
    "--naive_check"      = "true"
  }
}

# ------------------------------------------------------------------ Athena
resource "aws_athena_workgroup" "taxi" {
  name          = "nyc-taxi"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    # Safety net: Athena cancels any single query that would scan more than this.
    bytes_scanned_cutoff_per_query = var.athena_scan_limit_bytes

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${local.bucket}/athena-results/"
    }
  }
}
