output "kinesis_stream_name" {
  value = aws_kinesis_stream.indicadores.name
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.indicadores.arn
}

output "s3_bucket" {
  value = aws_s3_bucket.datalake.bucket
}

output "lambda_function_name" {
  value = aws_lambda_function.consumer.function_name
}

output "glue_database" {
  value = aws_glue_catalog_database.indicadores.name
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.indicadores.endpoint
}

output "emr_application_id" {
  value = aws_emrserverless_application.indicadores.id
}

output "emr_execution_role_arn" {
  value = aws_iam_role.emr_serverless_role.arn
}
