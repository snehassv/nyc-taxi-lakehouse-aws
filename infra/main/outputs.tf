output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "database" {
  value = aws_glue_catalog_database.taxi.name
}

output "glue_job" {
  value = aws_glue_job.load_trips.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.taxi.name
}

output "region" {
  value = var.region
}
