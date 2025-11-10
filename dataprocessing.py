from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

topic = "cctv_vehicle_counts"
port = "127.0.0.1:9092"
table = "sensor_vehicle_counts"
keyspace = "traffic_counts"


schema = StructType() \
    .add("timeuuid_id", StringType()) \
    .add("lgu_code", StringType()) \
    .add("sensor_id", StringType()) \
    .add("date_saved", StringType()) \
    .add("time_saved", StringType()) \
    .add("total", IntegerType()) \
    .add("car", IntegerType()) \
    .add("bus", IntegerType()) \
    .add("truck", IntegerType()) \
    .add("jeepney", IntegerType()) \
    .add("bike", IntegerType()) \
    .add("tryke", IntegerType()) \
    .add("others", IntegerType())


def connect_to_kafka(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", port) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.select(
            from_json(col("value").cast("string"), schema
                      ).alias("data")).select("data.*")
    return parsed_df


def connect_to_cassandra(parsed_df):

    def write_to_cassandra(batch_df, batch_id):
        batch_df.write\
                .format("org.apache.spark.sql.cassandra") \
                .options(keyspace=keyspace, table=table) \
                .option("checkpointLocation", "/tmp/spark_checkpoints") \
                .mode("append") \
                .save()

    query = parsed_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .start()

    return query


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CCTV Data Streaming") \
        .getOrCreate()

    df = connect_to_kafka(spark)
    query = connect_to_cassandra(df)

    query.awaitTermination()
