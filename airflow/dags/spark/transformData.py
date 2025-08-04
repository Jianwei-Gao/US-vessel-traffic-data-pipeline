from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window 
import pyspark.sql.functions as F
import argparse

def get_spark_schema():
  return StructType([
    StructField("MMSI", StringType(), False),
    StructField("BaseDateTime", TimestampType(), False),
    StructField("LAT", DoubleType(), False),
    StructField("LON", DoubleType(), False),
    StructField("SOG", FloatType(), True),
    StructField("COG", FloatType(), True),
    StructField("Heading", FloatType(), True),
    StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("CallSign", StringType(), True),
    StructField("VesselType", ShortType(), True),
    StructField("Status", ShortType(), True),
    StructField("Length", FloatType(), True),
    StructField("Width", FloatType(), True),
    StructField("Draft", FloatType(), True),
    StructField("Cargo", StringType(), True),
    StructField("TransceiverClass", StringType(), False)
  ])
  
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Transforming gcs raw day data with Spark")
  parser.add_argument("--bucket", required =True, type=str, help="id of google cloud bucket")
  parser.add_argument("--path", required=True, type=str, help="directory within bucket to data file(s) to be processed")
  parser.add_argument("--vcpu", required=False, type=int, help="for configuration tuning: enter number of number of vcpu cluster has")
  args = parser.parse_args()
  
  #tuning config according to suggestion from: https://cloud.google.com/dataproc/docs/support/spark-job-tuning
  config = SparkConf(loadDefaults=True)
  config.set("spark.dataproc.enhanced.optimizer.enabled", True)
  config.set("spark.dataproc.enhanced.execution.enabled", True)
  if(args.vcpu):
      config.set("spark.sql.shuffle.partitions", 3*args.vcpu)
      config.set("spark.default.parallelism", 3*args.vcpu)
  
  #start the spark session
  spark = SparkSession.builder.config(conf=config).appName("spark").getOrCreate()
  
  #read the raw data
  gcs_path = f"gs://{args.bucket}/"
  spark_df = spark.read.schema(get_spark_schema()).format("parquet").load(gcs_path + args.path)
  
  #disgard ais data with invalid mmsi or positional data
  spark_df = spark_df.filter((F.length(F.col("MMSI")) == 9) & (F.abs(F.col("LAT")) <= 90) & (F.abs(F.col("LON")) <= 180))

  #separate posititional and idenfification data
  vessel_profile_df = spark_df.select("MMSI", "VesselName", "IMO", "CallSign", "VesselType", "Length", "Width").distinct()
  ais_df = spark_df.select("MMSI","BaseDateTime","LAT","LON","SOG","COG","Heading","Status","Draft","Cargo","TransceiverClass")

  #test to make sure MMSI profiles are distinct
  if(vessel_profile_df.select("MMSI").distinct().count() != vessel_profile_df.select("MMSI").count()):
    vessel_profile_df.groupBy("MMSI").count().filter(F.expr("count > 1")).sort(F.desc("count")).show()
    raise ValueError("none-distinct MMSI found")

  #documentation regarding "invalid/not accessable/default" values on:
  #https://www.navcen.uscg.gov/ais-class-a-reports

  #replace values for "invalid/not accessable/default" to Null for non-categorial field 
  vessel_profile_df = vessel_profile_df.replace("IMO0000000", None, "IMO")
  vessel_profile_df = vessel_profile_df.replace(0, None, ["Length", "Width"])
  ais_df = ais_df.replace(511.0, None, "Heading")
  ais_df = ais_df.replace(0, None, "Draft")
  
  #handling negative COG and SOG value according to no.24 and 25 of https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf
  #non-categorial field;replace "invalid/not accessable/default" encoding to Null 
  def data_cleaning_col_select(col:str):
    if col == "COG":
      return F.when(F.col("COG") < 0, F.col("COG") + 409.6) \
              .when(F.col("COG") == 360, None) \
              .otherwise(F.col("COG")) \
              .cast(FloatType()).alias("COG")
    elif col == "SOG":
      return F.when(F.col("SOG") < 0, F.col("SOG") + 102.4) \
              .when(F.col("COG") == 102.3, None) \
              .otherwise(F.col("SOG")) \
              .cast(FloatType()).alias("SOG")
    else:
      return F.col(col)
  ais_df = ais_df.select(*[data_cleaning_col_select(col) for col in ais_df.columns])

  #replace null to encoded "invalid/not accessable/default" values for categorial field
  vessel_profile_df = vessel_profile_df.fillna(0, "VesselType")
  ais_df = ais_df.fillna(15, "Status")
  ais_df = ais_df.fillna(0, "Cargo")

  delta_lat = F.radians(F.expr("lead_LAT - LAT"))
  delta_lon = F.radians(F.expr("lead_LON - LON"))
  a = F.pow(F.sin(delta_lat / 2), 2) + F.cos(F.radians(F.col("LAT"))) * F.cos(F.radians(F.col("lead_LAT"))) * F.pow(F.sin(delta_lon)/2,2)
  c = 2 * F.atan2(F.sqrt(a), F.sqrt(1-a))
  d = 6371 * c

  windowSpec = Window.partitionBy(F.col("MMSI")).orderBy(F.asc(F.col("BaseDateTime")))
  ais_df = ais_df.select("*",
                        F.lead(F.col("LAT")).over(windowSpec).alias("lead_LAT"),
                        F.lead(F.col("LON")).over(windowSpec).alias("lead_LON"),
                        F.lead(F.col("BaseDateTime")).over(windowSpec).alias("lead_time"),
                        d.alias("distance_km_since_prev_ping"),
                        ((F.to_unix_timestamp(F.col("lead_time")) - F.to_unix_timestamp(F.col("BaseDateTime")))/60).alias("time_since_prev_ping")
                        )

  #Make column for partition
  ais_df = ais_df.select(*[col for col in ais_df.columns if col not in ["lead_LAT","lead_LON", "lead_time"]],  
                         F.year(F.col("BaseDateTime")).alias("year"), F.month(F.col("BaseDateTime")).alias("month"))
  
  #write transformed data 
  vessel_profile_df.write.mode("overwrite").parquet(gcs_path + "vessel_profile/")
  ais_df.write.mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month") \
        .parquet(gcs_path + "ais_data/")
  