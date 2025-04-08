variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "bucket_name" {
  description = "Unique name for the GCS bucket"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "europe-central2"
}
