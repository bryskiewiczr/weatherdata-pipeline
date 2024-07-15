from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType, ArrayType, TimestampType


weather_schema = StructType([
    StructField("location", StructType([
        StructField("name", StringType(), False),
        StructField("country", StringType(), False),
        StructField("lat", FloatType(), False),
        StructField("lon", FloatType(), False),
        StructField("timezone", StringType(), False),
        StructField("localtime_epoch", IntegerType(), False),
        StructField("localtime", TimestampType(), False)
    ])),
    StructField("current", StructType([
        StructField("last_updated_epoch", IntegerType(), False),
        StructField("last_updated", TimestampType(), False),
        StructField("temperature_c", FloatType(), False),
        StructField("feelslike_c", FloatType(), False),
        StructField("windchill_c", FloatType(), False),
        StructField("heatindex_c", FloatType(), False),
        StructField("dewpoint_c", FloatType(), False),
        StructField("humidity", IntegerType(), False),
        StructField("pressure_mb", FloatType(), False),
        StructField("precip_mm", FloatType(), False),
        StructField("wind_kph", FloatType(), False),
        StructField("wind_deg", IntegerType(), False),
        StructField("wind_dir", StringType(), False),
        StructField("gust_kph", FloatType(), False),
        StructField("is_day", IntegerType(), False),
        StructField("cloud", IntegerType(), False),
        StructField("vis_km", FloatType(), False),
        StructField("uv", FloatType(), False),
        StructField("condition", StructType([
            StructField("text", StringType(), False),
            StructField("icon", StringType(), False),
            StructField("code", IntegerType(), False)
        ]))
    ]))
])


spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_data") \
    .load()


weather_df = df.select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
    .select(col("data.location"), col("data.current").alias("current_weather"), col("data.location.localtime").alias("localtime"))  # current is a keyword


# this is used to make sure our hive table regularly picks up the data from hdfs
def refresh_table_metadata(batch_df, batch_id):
    batch_df.write.mode("append").partitionBy("localtime").format("parquet").save("hdfs://hdfs-namenode:8020/user/hive/warehouse/weather_data_hive_table")
    spark.sql("MSCK REPAIR TABLE default.weather_data_hive_table")


query_hdfs = weather_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hdfs-namenode:8020/weather_data") \
    .option("checkpointLocation", "hdfs://hdfs-namenode:8020/checkpoints/weather_data_hdfs") \
    .start()


query_parquet_for_hive = weather_df.writeStream \
    .foreachBatch(refresh_table_metadata) \
    .option("checkpointLocation", "hdfs://hdfs-namenode:8020/checkpoints/weather_data_hive") \
    .start()


spark.streams.awaitAnyTermination()
