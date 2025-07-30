import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc \
  import DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator

with DAG(  
  dag_id = f"transform_raw_data",
  schedule= "0 0 1 1 *",
  catchup=False,
  start_date=datetime(2029, 1, 1),
  render_template_as_native_obj=True
  ):
    upload_spark_code = LocalFilesystemToGCSOperator(
      task_id = "upload_spark_code",
      src="/opt/airflow/dags/spark/transformData.py",
      dst="/code/",
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
        "placement":{
            "clusterName":os.environ.get("DATAPROC_CLUSTER")
          },
        "pysparkJob":{
          "mainPythonFileUri":"gs://vessel-traffic-parquet-data/code/transformData.py",
          "args": ["--bucket", os.environ.get("GCS_BUCKET_NAME"), 
                   "--path", "raw_day/year=2024/month=01/AIS_2024_01_01.parquet",
                   "--vcpu", os.environ.get("CLUSTER_VCPU", "4")]
        }
      }
    )
    
    stop_cluster = DataprocStopClusterOperator(
      task_id="stop_cluster",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      cluster_name=os.environ.get("DATAPROC_CLUSTER"),
    )
        
    upload_spark_code >> start_cluster >> dataproc_submit >> stop_cluster


