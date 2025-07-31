provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw" {
  bucket = "rawdatajenkins"
}

resource "aws_s3_bucket" "processed" {
  bucket = "rawdatajenkins-bucket"
}

resource "aws_glue_job" "etl_job" {
  name     = "etl"
  role_arn = aws_iam_role.glue_role.arn
  command {
    name            = "glueetl"
    "
    python_version  = "3"
  }
  max_capacity = 2
}

resource "aws_iam_role" "glue_role" {
  name = "glue-service-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Principal = {
        Service = "glue.amazonaws.com"
      },
      Effect = "Allow",
      Sid    = ""
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}