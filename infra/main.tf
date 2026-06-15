terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ============================================
# S3 - Data Lake (bronze, silver, gold)
# ============================================
resource "aws_s3_bucket" "datalake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================
# KINESIS DATA STREAM
# Recebe os indicadores economicos em tempo real
# ============================================
resource "aws_kinesis_stream" "indicadores" {
  name             = var.kinesis_stream_name
  shard_count      = 1
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]

  tags = {
    Project = "indicadores-streaming-pipeline"
  }
}

# ============================================
# IAM ROLE - Lambda Consumer
# Permite a Lambda ler do Kinesis e escrever no S3
# ============================================
resource "aws_iam_role" "lambda_consumer_role" {
  name = "indicadores-lambda-consumer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_consumer_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_kinesis_s3" {
  name = "indicadores-lambda-kinesis-s3-policy"
  role = aws_iam_role.lambda_consumer_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:ListStreams"
        ]
        Resource = aws_kinesis_stream.indicadores.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      }
    ]
  })
}

# ============================================
# LAMBDA - Consumer
# Triggerada automaticamente pelo Kinesis
# Le os registros e salva no S3 bronze
# ============================================
data "archive_file" "lambda_consumer_zip" {
  type        = "zip"
  source_file = "${path.module}/../consumer/lambda_function.py"
  output_path = "${path.module}/lambda_consumer.zip"
}

resource "aws_lambda_function" "consumer" {
  function_name    = "indicadores-kinesis-consumer"
  filename         = data.archive_file.lambda_consumer_zip.output_path
  source_code_hash = data.archive_file.lambda_consumer_zip.output_base64sha256
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda_consumer_role.arn
  timeout          = 60

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.datalake.bucket
    }
  }
}

# Liga o Kinesis na Lambda - toda vez que chega dado, dispara a Lambda
resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn  = aws_kinesis_stream.indicadores.arn
  function_name     = aws_lambda_function.consumer.arn
  starting_position = "LATEST"
  batch_size        = 100
}

# ============================================
# GLUE CATALOG - Database e Crawler
# Cataloga o schema dos dados no S3 (silver)
# ============================================
resource "aws_glue_catalog_database" "indicadores" {
  name = "indicadores_streaming_db"
}

resource "aws_iam_role" "glue_crawler_role" {
  name = "indicadores-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "indicadores-glue-s3-policy"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*"
      ]
    }]
  })
}

resource "aws_glue_crawler" "silver_crawler" {
  name          = "indicadores-silver-crawler"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = aws_glue_catalog_database.indicadores.name

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.bucket}/silver/"
  }
}

# ============================================
# REDSHIFT SERVERLESS - Data Warehouse (gold)
# ============================================
resource "aws_redshiftserverless_namespace" "indicadores" {
  namespace_name      = var.redshift_namespace
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  db_name             = "indicadoresdb"
}

resource "aws_redshiftserverless_workgroup" "indicadores" {
  namespace_name     = aws_redshiftserverless_namespace.indicadores.namespace_name
  workgroup_name     = var.redshift_workgroup
  base_capacity      = 8
  publicly_accessible = true
}

# ============================================
# EMR SERVERLESS - Transformacao com PySpark
# Processa o bronze e gera o silver em Parquet
# ============================================
resource "aws_emrserverless_application" "indicadores" {
  name          = "indicadores-emr-app"
  release_label = "emr-7.1.0"
  type          = "SPARK"

  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "16 GB"
  }
}

resource "aws_iam_role" "emr_serverless_role" {
  name = "indicadores-emr-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "emr_s3_access" {
  name = "indicadores-emr-s3-policy"
  role = aws_iam_role.emr_serverless_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*"
      ]
    }]
  })
}
