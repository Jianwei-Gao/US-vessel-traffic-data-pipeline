variable "credentials" {
  type        = string
  description = "Path to Service Account json file"
  default     = "/credentials/google-credentials.json"
}

variable "project" {
  type        = string
  description = "Google Cloud project id"
  sensitive   = true
}

variable "region" {
  type        = string
  description = "Google Cloud resource region"
  sensitive   = true
}

variable "gcs_bucket_name" {
  type        = string
  description = "GCP bucket name"
  sensitive   = true
}

variable "bigquery_dataset_name" {
  type        = string
  description = "BigQuery dataset name"
  sensitive   = true
}
