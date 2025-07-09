import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCS_BUCKET_NAME")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET","trips_data_all")
# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# DATASET_PREFIX = os.environ.get("DATASET_NAME")

def checkIfParquetExistsGCS(**kwargs) -> bool:
  hook = GCSHook()
  return not hook.exists(os.environ.get("GCS_BUCKET_NAME"), f"raw_day/{kwargs['parquet_name']}")

def get_pl_schema():
  from polars import Schema, String, Datetime, Int32, Int16, Float64, Float32
  return Schema(
    {
      "MMSI":Int32(),
      "BaseDateTime":Datetime(),
      "LAT":Float64(),
      "LON":Float64(),
      "SOG":Float32(),
      "COG":Float32(),
      "Heading":Float32(),
      "VesselName":String(),
      "IMO":String(),
      "CallSign":String(),
      "VesselType":Int16(),
      "Status":Int16(),
      "Length":Float32(),
      "Width":Float32(),
      "Draft":Float32(),
      "Cargo":String(),
      "TransceiverClass":String()
    }
  )

def ingest_day(url:str, path:str, storage_options:dict = None):
  import requests, zipfile, io
  from polars import read_csv 
  res = requests.get(url=url, stream=True)
  if(res.status_code == 200):
    zip_buffer = io.BytesIO(res.content)
    with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
      csv_filename = zip_ref.namelist()[0]
      with zip_ref.open(csv_filename) as csv_file:
        read_csv(csv_file, has_header=True, infer_schema=False,schema=get_pl_schema()) \
          .write_parquet(file=path, storage_options=storage_options)
  else:
    raise ValueError(res.text)
  
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