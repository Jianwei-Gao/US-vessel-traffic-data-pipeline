import argparse
from ingest_day import ingest_day

# Start script
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="ingesting zipped csv file from url into path as parquet")
  parser.add_argument("--url", required =True, type=str, help="url of data")
  parser.add_argument("--path", required=True, type=str, help="path to write to")
  args = parser.parse_args()
  
  #print(args)
  ingest_day(url=args.url, path=args.path)