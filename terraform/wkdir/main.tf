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

resource "google_storage_bucket_object" "spark_init" {
  name   = "code/sedona_init.sh"
  source = "/terraform/sedona_init.sh"
  bucket = var.gcs_bucket_name

  depends_on = [google_storage_bucket.parquet-bucket]
}

resource "google_dataproc_cluster" "spark_cluster" {
  name    = var.dataproc_cluster_name
  project = var.project
  region  = var.region

  cluster_config {
    endpoint_config {
      enable_http_port_access = true
    }

    gce_cluster_config {
      internal_ip_only = false
    }

    master_config {
      num_instances = 1
      machine_type  = "n4-highmem-4"
      disk_config {
        boot_disk_type    = "hyperdisk-balanced"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 4
      machine_type  = "n4-highmem-4"
      disk_config {
        boot_disk_type    = "hyperdisk-balanced"
        boot_disk_size_gb = 100
      }
    }

    software_config {
      image_version = "2.2-debian12"
      override_properties = {
        "spark:spark.dataproc.enhanced.optimizer.enabled" = "true"
        "spark:spark.dataproc.enhanced.execution.enabled" = "true"
        "spark.sql.extensions" = "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
        "spark.serializer" =  "org.apache.spark.serializer.KryoSerializer"
        "spark.kryo.registrator" =  "org.apache.sedona.core.serde.SedonaKryoRegistrator"
        "spark.sedona.enableParserExtension" =  "false"
        "dataproc:pip.packages" = "apache-sedona==1.7.2"
      }
    }

    initialization_action {
      script      = "gs://vessel-traffic-parquet-data/code/sedona_init.sh"
    }
  }

  depends_on = [google_storage_bucket_object.spark_init]
}

resource "google_cloud_run_v2_job" "cloud_run_job_worker" {
  name                = var.cloudrun_job_name
  project             = var.project
  location            = var.region
  deletion_protection = false

  template {
    template {
      max_retries = 0
      containers {
        image = "jianweigao/noaa_ais_data_ingestion_cloudrun_job"
        resources {
          limits = {
            cpu    = "4"
            memory = "6Gi"
          }
        }
        volume_mounts {
          name       = "gcs_bucket"
          mount_path = "/mnt/gcs/"
        }
      }

      volumes {
        name = "gcs_bucket"
        gcs {
          bucket = var.gcs_bucket_name
        }
      }
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.bigquery_dataset_name
  description                = "Contains all analysis table through dbt"
  location                   = var.region
  project                    = var.project
  delete_contents_on_destroy = true
}
