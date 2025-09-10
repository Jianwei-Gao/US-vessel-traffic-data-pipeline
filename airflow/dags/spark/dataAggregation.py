from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window 
import pyspark.sql.functions as F
from sedona.spark import *
import argparse

def get_spark_schema():
  return StructType([
    StructField("MMSI", StringType(), False),
    StructField("BaseDateTime", TimestampType(), False),
    StructField("LAT", DoubleType(), False),
    StructField("LON", DoubleType(), False),
  ])

def get_port_schema():
  return StructType([
    StructField("UNLOCODE", StringType(), False),
    StructField("NAME", StringType(), False),
    StructField("STATE", StringType(), False),
    StructField("LAT", DoubleType(), False),
    StructField("LON", DoubleType(), False),
  ])

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Data aggregation with Spark")
  parser.add_argument("--bucket", required =True, type=str, help="id of google cloud bucket")
  parser.add_argument("--path", required =True, type=str, help="path of the ais data in google cloud bucket")
  parser.add_argument("--vcpu", required=False, type=int, help="for configuration tuning: enter number of number of vcpu cluster has")
  parser.add_argument("--input_size", required=False, type=int, help="for configuration tuning: enter total size of data files in gb")
  args = parser.parse_args()
  
  #tuning config according to suggestion from: https://cloud.google.com/dataproc/docs/support/spark-job-tuning
  #installing apache sedona dependencies
  config = SparkConf(loadDefaults=True)
  config.set("spark.dataproc.enhanced.optimizer.enabled", True)\
        .set("spark.dataproc.enhanced.execution.enabled", True)
        
  if(args.vcpu):
    if(args.input_size):
      config.set("spark.sql.shuffle.partitions", (int)(((args.input_size*1000.0)/200.0)//args.vcpu)*args.vcpu)
    else: 
      config.set("spark.sql.shuffle.partitions", 3*args.vcpu)
    config.set("spark.default.parallelism", 3*args.vcpu)
        
  #start the spark session
  spark = SparkSession.builder.config(conf=config).appName("spark").getOrCreate()
  SedonaContext.create(spark)
  
  #read the raw data
  gcs_path = f"gs://{args.bucket}/"
  ais_df = spark.read.schema(get_spark_schema()).format("parquet").load(gcs_path + args.path)
  ais_df = ais_df.select("MMSI", "BaseDateTime", ST_Point(F.col("LON"), F.col("LAT")).alias("coord")).alias("ais")
  port_df = spark.read.schema(get_port_schema()).csv(gcs_path + "code/ports.csv", header=True)
  port_df = port_df.select("UNLOCODE", ST_Point(F.col("LON"), F.col("LAT")).alias("coord")).alias("port")

  #data aggregation on if a vessel is near a port at a time 
  windowSpec = Window.partitionBy(F.col("MMSI"), F.col("BaseDateTime")).orderBy(F.asc(F.col("d")))
  agg_df = ais_df.join(F.broadcast(port_df), ST_Contains(ST_Buffer(F.col("port.coord"), 3500, True), F.col("ais.coord")))
  agg_df = agg_df.select("MMSI", "BaseDateTime", "UNLOCODE", ST_DistanceSpheroid(F.col("ais.coord"), F.col("port.coord")).alias("d"))\
                .select("MMSI", "BaseDateTime", "UNLOCODE", F.row_number().over(windowSpec).alias("r")).filter(F.col("r") == 1)\
                .select("MMSI", "BaseDateTime", "UNLOCODE").alias("agg")
  ais_df = ais_df.join(agg_df, 
                       (F.col("ais.MMSI") == F.col("agg.MMSI")) & (F.col("ais.BaseDateTime") == F.col("agg.BaseDateTime")),
                       "leftouter")\
                 .select("ais.MMSI", "ais.BaseDateTime", "ais.coord", "agg.UNLOCODE")
                 
  #data aggregation on distance traveled and time since last ping
  windowSpec = Window.partitionBy(F.col("MMSI")).orderBy(F.asc(F.col("BaseDateTime")))
  ais_df = ais_df.select("*",
                        F.lag(F.col("coord")).over(windowSpec).alias("lag_coord"),
                        F.lag(F.col("BaseDateTime")).over(windowSpec).alias("lag_time"))\
                  .select("MMSI", "BaseDateTime", F.col("UNLOCODE").alias("NearPort"), 
                          ST_DistanceSpheroid(F.col("lag_coord"), F.col("coord")).alias("MeterSincePrevPing"),
                          ((F.to_unix_timestamp(F.col("lag_time")) - F.to_unix_timestamp(F.col("BaseDateTime")))/60).alias("MinSincePrevPing")
                          )
  #Make column for partition
  ais_df = ais_df.select("*", F.date_format(F.col("BaseDateTime"), "YYYY-MM").alias("YearMonth"))
  
  #write aggregated data 
  ais_df.write.mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month") \
        .parquet(gcs_path + "time_analysis_data/")