from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
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
  
  config = SparkConf(loadDefaults=True)
  config.set("spark.dataproc.enhanced.optimizer.enabled", True)
  config.set("spark.dataproc.enhanced.execution.enabled", True)
  if(args.vcpu & args.vcpu.isdigit()):
    config.set("spark.sql.shuffle.partitions", 4*args.vcpu)
    config.set("spark.default.parallelism", 4*args.vcpu)
  spark = SparkSession.builder.config(conf=config).appName("spark").getOrCreate()
  gcs_path = f"gs://{args.bucket}/"
  spark_df = spark.read.schema(get_spark_schema()).format("parquet").load(gcs_path + args.path)
  vessel_profile_df = spark_df.select("MMSI", "VesselName", "IMO", "CallSign", "VesselType", "Length", "Width").distinct()
  ais_df = spark_df.select("MMSI","BaseDateTime","LAT","LON","SOG","COG","Heading","Status","Draft","Cargo","TransceiverClass")

  #test to make sure MMSI profiles are distinct
  if(vessel_profile_df.select("MMSI").distinct().count() != vessel_profile_df.select("MMSI").count()):
    vessel_profile_df.groupBy("MMSI").count().filter(F.expr("count > 1")).sort(F.desc("count")).show()
    raise ValueError("none-distinct MMSI found")

  #documentation regarding "invalid/not accessable/default" values on:
  #https://www.navcen.uscg.gov/ais-class-a-reports

  #disgard ais data with invalid mmsi or positional data
  ais_df = ais_df.filter((F.length(F.col("MMSI")) == 9) & (F.abs(F.col("LAT")) <= 90) & (F.abs(F.col("LON")) <= 180))

  #replace values for "invalid/not accessable/default" to Null for non-categorial field 
  vessel_profile_df = vessel_profile_df.replace("IMO0000000", None, "IMO")
  vessel_profile_df = vessel_profile_df.replace(0, None, ["Length", "Width"])
  ais_df = ais_df.replace(511.0, None, "Heading")
  ais_df = ais_df.replace(0, None, "Draft")
  
  #handling negative COG and SOG value according to no.24 and 25 of https://coast.noaa.gov/data/marinecadastre/ais/faq.pdf
  #non-categorial field;replace "invalid/not accessable/default" encoding to Null 
  ais_df = ais_df.withColumn("COG", 
                             F.when(F.col("COG") < 0, F.col("COG") + 409.6)
                              .when(F.col("COG") == 360, None)
                              .otherwise(F.col("COG"))
                              .cast(FloatType()))
  ais_df = ais_df.withColumn("SOG",
                             F.when(F.col("SOG") < 0, F.col("SOG") + 102.4)
                              .when(F.col("COG") == 102.3, None)
                              .otherwise(F.col("SOG"))
                              .cast(FloatType()))

  #replace null to encoded "invalid/not accessable/default" values for categorial field
  vessel_profile_df = vessel_profile_df.fillna(0, "VesselType")
  ais_df = ais_df.fillna(15, "Status")
  ais_df = ais_df.fillna(0, "Cargo")

  #Make column for partition
  ais_df = ais_df.withColumn("year",  F.year(F.col("BaseDateTime"))) \
                 .withColumn("month", F.month(F.col("BaseDateTime")))
  
  #write transformed data 
  vessel_profile_df.write.mode("overwrite").parquet(gcs_path + "vessel_profile/")
  ais_df.write.mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month") \
        .parquet(gcs_path + "ais_data/")
  