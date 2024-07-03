from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.avro.functions import from_avro
import uuid
import time




spark = SparkSession \
    .builder \
    .appName("Finnhub Streamer") \
    .getOrCreate()
        
while True:
	#Spark Read CSV File
	dataDir = "/home/ubuntu/tmp/data"
	df = spark.read.json(dataDir).withWatermark("ingest_timestamp", "10 minutes").dropDuplicates(["uuid", "ingest_timestamp"])
	#Write DataFrame to address directory
	df.printSchema()
	df.repartition(1).write.mode("overwrite").csv("/home/ubuntu/finnhub_stock_estimator/chunk/chunk.csv")
	
	time.sleep(600)



