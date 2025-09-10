{{ config(
  partition_by={
    "field": "PingTime",
    "data_type": "datetime",
    "granularity": "month"
  }
) }}

WITH source AS (
  SELECT * from {{ source('vessel_data_gcp','vessel_agg') }}
)

SELECT 
  MMSI,
  BaseDateTime as PingTime,
  NearPort,
  MeterSincePrevPing,
  MinSincePrevPing as MinuteSincePrevPing
FROM source