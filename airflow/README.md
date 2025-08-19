# Local Airflow setup in docker

To set up, run the following: 
1. read and complete setup in project root page's readme
2. docker compose up airflow-init
3. docker compose up

Airflow UI are accessable through localhost:8080
## DAGs
There are currently two main dags for data ingestion:

1. ingest_day_data_gcs:
    - downloading each day's data, convert to parquet, and upload to google cloud storage using cloud run jobs as worker
    - run backfill from YYYY-MM-DD to YYYY-MM-DD to ingest each day's data (if available) between the two ranges, set "max active runs" to >=2 for backfill to mutiple cloud run jobs in parrallel
2. transform_raw_data: 
    - data cleaning and aggregrating on a year's day data 
    - run backfill from YYYY-01-01 to YYYY-01-03 to transform ingested data under year YYYY

You should run the ingest_day_data_gcs dag first to ingest the raw data into gcs, then run the transform_raw_data to batch process the ingested data