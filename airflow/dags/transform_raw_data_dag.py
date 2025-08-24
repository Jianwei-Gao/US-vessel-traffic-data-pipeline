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
      src=["/opt/airflow/dags/spark/dataAggregation.py", "/opt/airflow/dags/spark/cleanData.py", 
           "/opt/airflow/dags/spark/ports.csv"],
      dst="code/",
      bucket=os.environ.get("GCS_BUCKET_NAME")
    )

    start_cluster = DataprocStartClusterOperator(
      task_id="start_cluster",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      cluster_name=os.environ.get("DATAPROC_CLUSTER"),
    )
    
    dataproc_clean_data = DataprocSubmitJobOperator(
      task_id = f"spark_clean_raw_data",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      job={
        "reference": {"project_id": os.environ.get("GCP_PROJECT_ID")},
        "placement":{
            "cluster_name":os.environ.get("DATAPROC_CLUSTER")
        },
        "pyspark_job":{
          "main_python_file_uri":f"gs://{os.environ.get("GCS_BUCKET_NAME")}/code/cleanData.py",
          "args": [
            "--bucket", os.environ.get("GCS_BUCKET_NAME"), 
            "--path", "raw_day/year={{ logical_date.format('YYYY') }}/",
            "--vcpu", os.environ.get("CLUSTER_VCPU", "12"),
            "--input_size", "80"
          ],
          "properties": {
            "spark.dataproc.enhanced.optimizer.enabled": "true",
            "spark.dataproc.enhanced.execution.enabled": "true",
            "spark.network.timeout": "1200s",
            "spark.sql.adaptive.enabled":"true",
            "spark.sql.adaptive.coalescePartitions.enabled":"true",
            "spark.sql.adaptive.skewJoin.enabled":"true",
            "spark.executor.cores": "3",
            "spark.executor.instances": "4",
            "spark.executor.memory":"25332m",
            "spark.executor.memoryOverhead":"2814m"
          }
        }
      }
    )
    
    dataproc_aggregation = DataprocSubmitJobOperator(
      task_id = f"spark_data_aggregation",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      job={
        "reference": {"project_id": os.environ.get("GCP_PROJECT_ID")},
        "placement":{
            "cluster_name":os.environ.get("DATAPROC_CLUSTER")
        },
        "pyspark_job":{
          "main_python_file_uri":f"gs://{os.environ.get("GCS_BUCKET_NAME")}/code/dataAggregation.py",
          "args": [
            "--bucket", os.environ.get("GCS_BUCKET_NAME"), 
            "--path", "ais_data/year={{ logical_date.format('YYYY') }}/",
            "--vcpu", os.environ.get("CLUSTER_VCPU", "12"),
            "--input_size", "80"
          ],
          "properties": {
            "spark.dataproc.enhanced.optimizer.enabled": "true",
            "spark.dataproc.enhanced.execution.enabled": "true",
            "spark.network.timeout": "1200s",
            "spark.sql.adaptive.enabled":"true",
            "spark.sql.adaptive.coalescePartitions.enabled":"true",
            "spark.sql.adaptive.skewJoin.enabled":"true",
            "spark.executor.cores": "3",
            "spark.executor.instances": "4",
            "spark.executor.memory":"25332m",
            "spark.executor.memoryOverhead":"2814m"
          }
        }
      }
    )
    
    stop_cluster = DataprocStopClusterOperator(
      task_id="stop_cluster",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      cluster_name=os.environ.get("DATAPROC_CLUSTER"),
      #trigger_rule="all_done"
    )
        
    upload_spark_code >> start_cluster >> dataproc_clean_data >> dataproc_aggregation >> stop_cluster


