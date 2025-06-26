terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.41.0"
    }
  }
}

provider "google" {
  credentials = sensitive(file(var.credentials))
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "parquet-bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.bigquery_dataset_name
  location = var.region
}
