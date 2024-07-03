from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.avro.functions import from_avro
import uuid


# Define the MongoDB settings
mongodb_database = "test_db"
trades_collection = "test_col"
aggregates_collection = "aggregates"


#hdfs_path = f"hdfs://172.31.61.121:9000/data"

def foreach_batch_function(df, epoch_id):
    df.show()



# Define the Kafka settings
kafka_server = "localhost"
kafka_port = "9092"
topic_market = "market"
min_partitions = "1"



# Define the Avro schema as a JSON string
avro_schema = """
{
  "type" : "record",
  "name" : "message",
  "namespace" : "FinnhubProducer",
  "fields" : [ {
    "name" : "data",
    "type" : {
      "type" : "array",
      "items" : {
        "type" : "record",
        "name" : "data",
        "fields" : [ {
          "name" : "c",
          "type":[
            {
               "type":"array",
               "items":["null","string"],
               "default":[]
            },
            "null"
          ],
          "doc" : "Trade conditions"
        }, 
        {
          "name" : "p",
          "type" : "double",
          "doc" : "Price at which the stock was traded"
        }, 
        {
          "name" : "s",
          "type" : "string",
          "doc" : "Symbol of a stock"
        }, 
        {
          "name" : "t",
          "type" : "long",
          "doc" : "Timestamp at which the stock was traded"
        }, 
        {
          "name" : "v",
          "type" : "double",
          "doc" : "Volume at which the stock was traded"
        } ]
      },
      "doc" : "Trades messages"
    },
    "doc"  : "Contains data inside a message"
  }, 
  {
    "name" : "type",
    "type" : "string",
    "doc"  : "Type of message"
  } ],
  "doc" : "A schema for upcoming Finnhub messages"
}
"""


spark = SparkSession \
    .builder \
    .appName("Finnhub Streamer") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0") \
    .getOrCreate()
        
# Define the Spark Structured Streaming read stream from Kafka
input_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("host", "localhost") \
    .option("port", 9092) \
    .option("subscribe", "market") \
    .option("failOnDataLoss", False) \
    .load()
    
    
print("tryparse")
parsed_df = input_df \
        .withColumn("parsed_value", from_avro(col("value"), avro_schema))

# Define the UDF for generating UUIDs
@F.udf(StringType())
def make_uuid():
    return str(uuid.uuid1())

   
# Explode the nested array within the parsed Avro data
exploded_df = parsed_df \
    .selectExpr("explode(parsed_value.data) as data", "parsed_value.type")

# Flatten the nested structure
flattened_df = exploded_df \
    .select(
        make_uuid().alias("uuid"),
        col("data.p").alias("price"),
        col("data.s").alias("symbol"),
        col("data.t").alias("trade_timestamp"),
        col("data.v").alias("volume")
    )

# Add proper timestamps
final_df = flattened_df \
    .withColumn("trade_timestamp", expr("timestamp(trade_timestamp / 1000)")) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withWatermark("ingest_timestamp", "10 minutes") \
    .dropDuplicates(["uuid", "ingest_timestamp"])

final_df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("checkpointLocation", "/home/ubuntu/tmp/checkpoint") \
    .option("path", "/home/ubuntu/tmp/data") \
    .option("maxRecordsPerFile", 100) \
    .trigger(processingTime="3 seconds") \
    .start().awaitTermination()

#final_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()


