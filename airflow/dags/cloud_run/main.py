import argparse
from ingest_day import ingest_day

# Start script
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Transforming gcs raw day data with Spark")
  parser.add_argument("--url", required =True, type=str, help="url of data")
  parser.add_argument("--path", required=True, type=str, help="path to write to")
  args = parser.parse_args()
  
  ingest_day(url=args.url, path=args.path)