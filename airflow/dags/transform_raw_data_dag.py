import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc \
  import DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator

with DAG(  
  dag_id = f"transform_raw_data",
  schedule= "0 0 2 1 *",
  catchup=False,
  start_date=datetime(2029, 1, 1),
  ):
    upload_spark_code = LocalFilesystemToGCSOperator(
      task_id = "upload_spark_code",
      src="/opt/airflow/dags/spark/transformData.py",
      dst="code/",
      bucket=os.environ.get("GCS_BUCKET_NAME")
    )

    start_cluster = DataprocStartClusterOperator(
      task_id="start_cluster",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      cluster_name=os.environ.get("DATAPROC_CLUSTER"),
    )
    
    dataproc_submit = DataprocSubmitJobOperator(
      task_id = f"pyspark_job",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      job={
        "reference": {"project_id": os.environ.get("GCP_PROJECT_ID")},
        "placement":{
            "cluster_name":os.environ.get("DATAPROC_CLUSTER")
        },
        "pyspark_job":{
          "main_python_file_uri":f"gs://{os.environ.get("GCS_BUCKET_NAME")}/code/transformData.py",
          "args": [
            "--bucket", os.environ.get("GCS_BUCKET_NAME"), 
            "--path", "raw_day/year=2024/",
            "--vcpu", os.environ.get("CLUSTER_VCPU", "8")
          ],
          "properties": {
            "spark.dataproc.enhanced.optimizer.enabled": "true",
            "spark.dataproc.enhanced.execution.enabled": "true"
          }
        }
      }
    )
    
    stop_cluster = DataprocStopClusterOperator(
      task_id="stop_cluster",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      cluster_name=os.environ.get("DATAPROC_CLUSTER"),
      trigger_rule="always"
    )
        
    upload_spark_code >> start_cluster >> dataproc_submit >> stop_cluster


