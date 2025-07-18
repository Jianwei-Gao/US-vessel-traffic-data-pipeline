import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers import local_to_gcs
from airflow.providers.google.cloud.operators.dataproc \
  import DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator

def generate_day_range(year:int, month:int) -> list:
  from calendar import monthrange
  return list(range(1, monthrange(year, month)[1]+1))

def ingest_day_wrapper(day:int, **context) -> None:
  year = context["logical_date"].format('YYYY')
  month = context["logical_date"].format('MM')
  hook = GCSHook()
  print("Checking if parquet already exists")
  if not hook.exists(os.environ.get("GCS_BUCKET_NAME"), f"raw_day/year={year}/month={month}/AIS_{year}_{month}_{day:02d}.parquet"):
    print("parquet doesn't exists, ingesting data")
    from common.ingest_day import ingest_day
    url = f'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/AIS_{year}_{month}_{day:02d}.zip'
    path = f'gs://{ os.environ.get("GCS_BUCKET_NAME") }/raw_day/year={year}/month={month}/AIS_{year}_{month}_{day:02d}.parquet'
    storage_options = {'service_account_path':'/opt/airflow/credentials/google-credential.json'}
    ingest_day(url=url, path=path, storage_options=storage_options)
  else:
    print("parquet exists, skipping ingestion")
    
with DAG(  
  dag_id = f"ingest_day_data_gcs",
  schedule= "0 0 2 * *",
  catchup=False,
  is_paused_upon_creation=True,
  start_date=datetime(2029, 1, 1),
  render_template_as_native_obj=True
  ):
    generate_task_range = PythonOperator(
      task_id = "generate_task_range",
      python_callable=generate_day_range,
      op_kwargs={'year': '{{ logical_date.year }}',
                 'month': '{{ logical_date.month }}'
                 }
    )
            
    ingest_day_data = PythonOperator.partial(
      task_id = f"ingest_day_data",
      python_callable=ingest_day_wrapper,
    ).expand(
      op_kwargs = generate_task_range.output.map(lambda day : {"day":day})
    )
        
    generate_task_range >> ingest_day_data