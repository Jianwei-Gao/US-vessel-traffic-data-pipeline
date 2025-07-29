from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

def get_spark_schema():
  from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, DoubleType, StringType
  return StructType([
    StructField("MMSI", StringType(), False),
    StructField("BaseDateTime", TimestampType(), False),
    StructField("LAT", DoubleType(), False),
    StructField("LON", DoubleType(), False),
    StructField("SOG", FloatType(), False),
    StructField("COG", FloatType(), False),
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
  
conf = SparkConf().set('spark.ui.port', '4045')\
  .set("google.cloud.auth.service.account.enable", "true")\
  .set("google.cloud.auth.service.account.json.keyfile", "/opt/spark/credentials/google-credential.json")
spark = SparkSession.builder.appName("test").config(conf = conf).master("local[*]").getOrCreate()

gcs_path = "gs://vessel-traffic-parquet-data/"
spark_df = spark.read.schema(get_spark_schema()).format("parquet").load(gcs_path + "raw_day/year=2024/month=01/AIS_2024_01_01.parquet")

spark_df.show()