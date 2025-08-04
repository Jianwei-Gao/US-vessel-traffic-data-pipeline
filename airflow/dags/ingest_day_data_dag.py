import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.exceptions import AirflowSkipException

def gcs_check(**context):
  hook = GCSHook()
  year = context["logical_date"].format('YYYY')
  month = context["logical_date"].format('MM')
  day = context["logical_date"].format('DD')
  print(f"checking is file exists for {year}-{month}-{day}")
  file_prefix = f"AIS_{year}_{context["logical_date"].format('MM')}_{day}"

  if hook.exists(os.environ.get("GCS_BUCKET_NAME"), 
                    f"raw_day/year={year}/month={context["logical_date"].format('M')}/{file_prefix}.parquet"):
    raise AirflowSkipException()
  
with DAG( 
  dag_id = f"ingest_day_data_gcs",
  schedule= "0 0 * * *",
  catchup=False,
  is_paused_upon_creation=True,
  start_date=datetime(2029, 1, 1),
  render_template_as_native_obj=True
  ):                        
    check_file_exists = PythonOperator(
      task_id = f"check_file_exists",
      python_callable=gcs_check
    )
    
    ingest_day_data = CloudRunExecuteJobOperator(
      task_id = "ingest_day_data",
      project_id=os.environ.get("GCP_PROJECT_ID"),
      region=os.environ.get("GCP_REGION"),
      job_name =os.environ.get("CLOUDRUN_JOB_NAME"),
      overrides = {
        "container_overrides": [
          {
            "args":[ 
              "--url", 
              "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"
                "{{ logical_date.format('YYYY') }}/"
                "AIS_{{ logical_date.format('YYYY') }}_{{ logical_date.format('MM') }}_{{ logical_date.format('DD') }}.zip",
              "--path",
              "/mnt/gcs/raw_day/"
                "year={{ logical_date.format('YYYY') }}/month={{ logical_date.format('M') }}/"
                "AIS_{{ logical_date.format('YYYY') }}_{{ logical_date.format('MM') }}_{{ logical_date.format('DD') }}.parquet"
            ]
          }
        ]
      }
    )
    
    check_file_exists >> ingest_day_data