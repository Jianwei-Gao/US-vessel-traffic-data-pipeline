from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

def get_spark_schema():
  from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, DoubleType, StringType
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
  conf = SparkConf().set('spark.ui.port', '4045')\
    .set("google.cloud.auth.service.account.enable", "true")\
    .set("google.cloud.auth.service.account.json.keyfile", "/opt/spark/credentials/google-credential.json")
  spark = SparkSession.builder.appName("test").config(conf = conf).master("local[*]").getOrCreate()

  gcs_path = "gs://vessel-traffic-parquet-data/"
  spark_df = spark.read.schema(get_spark_schema()).format("parquet").load(gcs_path + "raw_day")
  vessel_profile_df = spark_df.select("MMSI", "VesselName", "IMO", "CallSign", "VesselType", "Length", "Width").distinct()
  ais_df = spark_df.select("MMSI","BaseDateTime","LAT","LON","SOG","COG","Heading","Status","Draft","Cargo","TransceiverClass")

  #documentation regarding "invalid/not accessable/default" values on:
  #https://www.navcen.uscg.gov/ais-class-a-reports

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
                              .otherwise(F.col("COG")))
  ais_df = ais_df.withColumn("SOG",
                             F.when(F.col("SOG") < 0, F.col("SOG") + 102.4)
                              .when(F.col("COG") == 102.3, None)
                              .otherwise(F.col("SOG")))

  #replace null to encoded "invalid/not accessable/default" values for categorial field
  vessel_profile_df = vessel_profile_df.fillna(0, "VesselType")
  ais_df = ais_df.fillna(15, "Status")
  ais_df = ais_df.fillna(0, "Cargo")

  #disgard ais data with invalid mmsi or positional data
  ais_df = ais_df.filter((F.length(F.col("MMSI")) == 9) & (F.abs(F.col("LAT")) <= 90) & (F.abs(F.col("LAT")) <= 180))

  #write transformed data 
  vessel_profile_df.write.parquet("./vessel_profile")
  ais_df.write.parquet("./ais_data")
  