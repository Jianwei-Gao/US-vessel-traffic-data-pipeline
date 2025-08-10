# US Vessel Traffic Data Pipeline

This project aims to develope a data pipeline to ingest, transform, and analyze historic AIS data for US Vessels.  

Datasource: https://hub.marinecadastre.gov/pages/vesseltraffic

## Tech Stack
* Docker
* Terraform
* Apache Airflow
* Apache Spark + Dataproc
* Cloud Run
* Google Cloud Storage
* BigQuery
* dbt core

## Setup
0. start up a google cloud account and create a project
1. follow setup in crediential folder's [read me](credentials/README.md)
2. fill out example.env as a new ".env" file
    * HOST_UID: Host id on linux system; may ignore if on Windows
    * GCP_PROJECT_ID: Project id of your gcp project
    * GCP_REGION: Region id for the region you want your cloud infrastructure to be hosted in. Can be the region closest to you or the region that charges the cheapeast for its resource (ex: us-central1)
    * GCS_BUCKET_NAME: Name of bucket you want the data to be stored in. Terraform will create this for you later on.
    * DATAPROC_CLUSTER: Name of dataproc cluster. Terraform will create this for you later on. 
    * CLUSTER_VCPU: Number of vcpu in dataproc cluster (defaults to 8). No need to change unless you are editing the cluster setting in [terraform](terraform/wkdir/main.tf) file 
    * CLOUDRUN_JOB_NAME: Name for your cloud run job. Terraform will create this for you later on.
    * BQ_DATASET_NAME: Name for your bigquery dataset. Terraform will create this for you later on. 
3. Copy & Paste (or create soft-link) the ".env" file into airflow, dbt-core, and terraform folder
4. follow setup in terraform's [read me](terraform/README.md) to start up your google cloud infrastructure
5. follow setup in airflow's [read me](airflow/README.md) to start up the data pipeline orchestration
6. 