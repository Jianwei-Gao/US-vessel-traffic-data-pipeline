{{ config(
  partition_by={
    "field": "PingTime",
    "data_type": "datetime",
    "granularity": "month"
  }
) }}

WITH source AS (
  SELECT * from {{ source('vessel_data_gcp','vessel_ais') }}
)

SELECT 
  MMSI,
  BaseDateTime as PingTime,
  LAT,
  LON,
  SOG,
  COG,
  Heading,
  Status,
  Draft,
  Cargo,
  TransceiverClass as Class
FROM source