import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from ingest_day import ingest_day

def checkIfParquetExistsGCS(**kwargs) -> bool:
  hook = GCSHook()
  return not hook.exists(os.environ.get("GCS_BUCKET_NAME"), f"raw_day/{kwargs['parquet_name']}")
  
with DAG(  
  dag_id = f"ingest_day_data_gcs",
  schedule= "0 0 * * *",
  catchup=False,
  is_paused_upon_creation=True,
  start_date=datetime(2029, 1, 1)
  ):
    checkFileExistGCS = ShortCircuitOperator(
      task_id = f"check_file_exists_gcs",
      python_callable=checkIfParquetExistsGCS,
      op_kwargs={'parquet_name':'AIS_{{ logical_date.format(\'YYYY_MM_DD\') }}.parquet'},
      trigger_rule = "none_failed",
      ignore_downstream_trigger_rules=False
    )
      
    ingest_day_data = PythonOperator(
      task_id = f"ingest_day_data",
      python_callable=ingest_day,
      op_kwargs={ 'url':'https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{{ logical_date.format(\'YYYY\') }}/AIS_{{ logical_date.format(\'YYYY_MM_DD\') }}.zip',
                  'path':f'gs://{ os.environ.get("GCS_BUCKET_NAME") }/raw_day/AIS_{{{{ logical_date.format(\'YYYY_MM_DD\') }}}}.parquet',
                  'storage_options': {'service_account_path':'/opt/airflow/credentials/google-credential.json'}
                }
    )
            
    checkFileExistGCS >> ingest_day_data