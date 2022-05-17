terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }

  backend "s3" {
    encrypt = "true"
    region  = "us-east-1"
    key     = "bda-mlops-feature-store"
  }
}

provider "aws" {
  region = "us-east-1"
}