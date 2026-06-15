variable "aws_region" {
  default = "us-east-2"
}

variable "bucket_name" {
  default = "indicadores-streaming-datalake-2026"
}

variable "kinesis_stream_name" {
  default = "indicadores-stream"
}

variable "redshift_namespace" {
  default = "indicadores-ns"
}

variable "redshift_workgroup" {
  default = "indicadores-wg"
}

variable "redshift_admin_user" {
  default = "admin_indicadores"
}

variable "redshift_admin_password" {
  default = "IndicadoresStream2026!"
}
