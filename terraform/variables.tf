variable "aws_region" {
  description = "Aws region for the project's services"
  type        = string
  default     = "us-east-1"
}

# Unique identifier for the S3 Bucket
variable "bucket_name" {
  description = "Name of the s3 Bucket"
  type        = string
  default     = "skystream-data-bucket"
}

variable "dataset_name" {
  description = "Glue Catalog Dataset name(logical dataset for Redshift)"
  type        = string
  default     = "skystream_database"
}