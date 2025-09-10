WITH source AS (
  SELECT * from {{ source('vessel_data_gcp','vessel_profile') }}
)

SELECT 
  MMSI,
  VesselName as Name,
  IMO,
  CallSign,
  VesselType as Type,
  Length,
  Width
FROM source