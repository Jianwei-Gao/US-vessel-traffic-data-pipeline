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
    StructField("LON", DoubleType(), False)
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
  config = SparkConf(loadDefaults=True)
  config.set("spark.dataproc.enhanced.optimizer.enabled", True)
  config.set("spark.dataproc.enhanced.execution.enabled", True)
  
  if(args.vcpu):
    if(args.input_size):
      config.set("spark.sql.shuffle.partitions", (int)(((args.input_size*1000.0)/128.0)//args.vcpu)*args.vcpu)
    else: 
      config.set("spark.sql.shuffle.partitions", 3*args.vcpu)
    config.set("spark.default.parallelism", 3*args.vcpu)
        
  #start the spark session
  spark = SparkSession.builder.config(conf=config).appName("spark").getOrCreate()
  
  #read the raw data
  gcs_path = f"gs://{args.bucket}/"
  ais_df = spark.read.schema(get_spark_schema()).format("parquet").load(gcs_path + args.path)
  port_df = spark.read.schema(get_port_schema()).csv(gcs_path + "code/ports.csv", header=True)

  #calculating distance between pings through haversine formula 
  delta_lat = F.radians(F.expr("LAT - lag_LAT"))
  delta_lon = F.radians(F.expr("LON - lag_LON"))
  a = F.pow(F.sin(delta_lat / 2), 2) + F.cos(F.radians(F.col("LAT"))) * F.cos(F.radians(F.col("lag_LAT"))) * F.pow(F.sin(delta_lon)/2,2)
  c = 2 * F.atan2(F.sqrt(a), F.sqrt(1-a))
  d = 6371 * c
  
  #calculating distance between vessel and ports through haversine formula
  delta_lat_port = F.radians(F.expr("ais.LAT - port.LAT"))
  delta_lon_port = F.radians(F.expr("ais.LON - port.LON"))
  a_port = F.pow(F.sin(delta_lat_port / 2), 2) + F.cos(F.radians(F.col("ais.LAT"))) * F.cos(F.radians(F.col("port.LAT"))) * F.pow(F.sin(delta_lon_port)/2,2)
  c_port = 2 * F.atan2(F.sqrt(a_port), F.sqrt(1-a_port))
  d_port = (6371 * c_port).alias("km_to_port")

  #data aggregation on distance traveled and time since last ping
  windowSpec = Window.partitionBy(F.col("MMSI")).orderBy(F.asc(F.col("BaseDateTime")))
  ping_df = ais_df.select("MMSI", "BaseDateTime", "LAT", "LON",
                        F.lag(F.col("LAT")).over(windowSpec).alias("lag_LAT"),
                        F.lag(F.col("LON")).over(windowSpec).alias("lag_LON"),
                        F.lag(F.col("BaseDateTime")).over(windowSpec).alias("lag_time"),
                        d.alias("km_trav_since_prev_ping"),
                        ((F.to_unix_timestamp(F.col("BaseDateTime")) - F.to_unix_timestamp(F.col("lag_time")))/60).alias("sec_since_prev_ping")
                        ).alias("ais")

  #data aggregation on if a vessel is near a port at a time 
  windowSpec_port = Window.partitionBy(F.col("ais.MMSI"), F.col("ais.BaseDateTime")).orderBy(F.asc(F.col("km_to_port")))
  cross_df = ping_df.select("MMSI", "BaseDateTime", "LAT", "LON").crossJoin(F.broadcast(port_df.alias("port")))
  cross_df = cross_df.select("ais.MMSI", "ais.BaseDateTime", d_port, "port.UNLOCODE").filter(F.col("km_to_port") <= 35)
  cross_df = cross_df.select("*", F.row_number().over(windowSpec_port).alias("order")).where(F.col("order") == 1)
  cross_df = cross_df.select("ais.MMSI", "ais.BaseDateTime", "port.UNLOCODE")
  ping_df = ping_df.join(cross_df.alias("cross"), (F.expr("ais.MMSI = cross.MMSI AND ais.BaseDateTime = cross.BaseDateTime")), "left_outer")
  ping_df = ping_df.select("ais.MMSI", "ais.BaseDateTime", "ais.km_trav_since_prev_ping", "ais.sec_since_prev_ping", F.col("UNLOCODE").alias("InPort"))
  ping_df = ping_df.fillna("N/A", "InPort")
  
  #Make column for partition
  ping_df = ping_df.select("*", F.year(F.col("BaseDateTime")).alias("year"), F.month(F.col("BaseDateTime")).alias("month"))
  
  #write aggregated data 
  ping_df.repartition("year", "month").write.mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month") \
        .parquet(gcs_path + "time_analysis_data/")