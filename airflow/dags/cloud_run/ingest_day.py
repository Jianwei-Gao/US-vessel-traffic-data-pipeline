def get_pl_schema():
  from polars import Schema, String, Datetime, Int16, Float64, Float32
  return Schema(
    {
      "MMSI":String(),
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
        read_csv(csv_file, has_header=True, infer_schema=False,schema=get_pl_schema(),encoding="ascii",quote_char=None) \
        .write_parquet(file=path, storage_options=storage_options)     
  else:
    raise ValueError(res.text)
