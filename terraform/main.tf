terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "chicago_crime_lake" {
  bucket =  var.bucket_name  
  force_destroy = true

  tags = {
    Project = "chicago-crime-pipeline"
  }
}

resource "aws_s3_bucket_versioning" "chicago_crime_versioning" {
  bucket = aws_s3_bucket.chicago_crime_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}