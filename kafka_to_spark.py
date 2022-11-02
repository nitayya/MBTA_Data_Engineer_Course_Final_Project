import pymongo
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType
from pyspark.sql import SparkSession
import configuration as c

### Creating a Conection Between Spark And Kafka ####
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

####  Creating Spark Session ####
spark = SparkSession\
        .builder\
        .appName("mbta_bot")\
        .getOrCreate()

#### ReadStream From Kafka Amadeus Topic ####
df_kafka = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", c.bootstrapServers)\
    .option("subscribe", c.topic1)\
    .load()

#### Creating a Schema for Spark Structured Streaming ####
schema = StructType() \
    .add("predicted_arrival_time", StringType())\
    .add("chat_id", StringType())\
    .add("user_id", StringType())\
    .add("origin_stop_id", StringType())\
    .add("destination_stop_id", StringType())\
    .add("route_id", StringType()) \
    .add("route_fare_class", StringType()) \
    .add("route_name", StringType()) \
    .add("trip_id", StringType()) \
    .add("origin_stop_name", StringType()) \
    .add("dest_stop_name", StringType()) \
    .add("duration", StringType())\
    .add("user_user_name", StringType())\
    .add("user_first_name", StringType())\
    .add("user_last_name", StringType())\


### Change Json To Dataframe With Schema ####
df_kafka = df_kafka.select(col("value").cast("string"))\
     .select(from_json(col("value"), schema).alias("value"))\
     .select("value.*")

#### Adding Calculated Columns To Spark Data Frame ####
df_kafka = df_kafka.withColumn("current_ts", current_timestamp().cast('string'))
df_kafka= df_kafka.withColumn("is_active", lit(1))


#### Defining A Function To Send Dataframe To MongoDB ####
def write_df_mongo(target_df):

    mongodb_client = pymongo.MongoClient(c.mongo_client_url)
    mydb = mongodb_client[c.mongodb_client]
    mycol = mydb[c.mongodb_db]
    post = {
        "predicted_arrival_time": target_df.predicted_arrival_time,
        "chat_id": target_df.chat_id,
        "user_id": target_df.user_id,
        "origin_stop_id": target_df.origin_stop_id,
        "destination_stop_id": target_df.destination_stop_id,
        "origin_stop_name":target_df.origin_stop_name,
        "dest_stop_name": target_df.dest_stop_name,
        'route_id': target_df.route_id,
        'route_fare_class': target_df.route_fare_class,
        'route_name': target_df.route_name,
        "trip_id": target_df.trip_id,
        "duration": target_df.duration,
        "user_user_name": target_df.user_user_name,
        "user_first_name": target_df.user_first_name,
        "user_last_name": target_df.user_last_name,
        "current_ts": target_df.current_ts,
        "is_active": target_df.is_active
    }

    mycol.insert_one(post)
    print('item inserted')
    print(post)

#### Spark Action ###
df_kafka \
    .writeStream \
    .foreach(write_df_mongo)\
    .outputMode("append") \
    .start() \
    .awaitTermination()
df_kafka.show(truncate=False)