from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, floor, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, LongType, IntegerType, ArrayType, TimestampType
)

import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    .select(
        col("data.location.name").alias("name"),
        col("data.location.country").alias("country"),
        col("data.location.lat").alias("lat"),
        col("data.location.lon").alias("lon"),
        col("data.location.timezone").alias("timezone"),
        col("data.location.localtime_epoch").alias("localtime_epoch"),
        col("data.location.localtime").alias("localtime"),
        col("data.current.last_updated_epoch").alias("last_updated_epoch"),
        col("data.current.last_updated").alias("last_updated"),
        col("data.current.temperature_c").alias("temperature_c"),
        col("data.current.feelslike_c").alias("feelslike_c"),
        col("data.current.windchill_c").alias("windchill_c"),
        col("data.current.heatindex_c").alias("heatindex_c"),
        col("data.current.dewpoint_c").alias("dewpoint_c"),
        col("data.current.humidity").alias("humidity"),
        col("data.current.pressure_mb").alias("pressure_mb"),
        col("data.current.precip_mm").alias("precip_mm"),
        col("data.current.wind_kph").alias("wind_kph"),
        col("data.current.wind_deg").alias("wind_deg"),
        col("data.current.wind_dir").alias("wind_dir"),
        col("data.current.gust_kph").alias("gust_kph"),
        col("data.current.is_day").alias("is_day"),
        col("data.current.cloud").alias("cloud"),
        col("data.current.vis_km").alias("vis_km"),
        col("data.current.uv").alias("uv"),
        col("data.current.condition.text").alias("condition_text"),
        col("data.current.condition.icon").alias("condition_icon"),
        col("data.current.condition.code").alias("condition_code"),
        date_format(
            expr("timestamp(floor(unix_timestamp(data.location.localtime) / 600) * 600)"),
            "HH-mm-ss-dd-MM-yyyy"
        ).alias("ts")
    )


def write_to_hive(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .partitionBy("ts") \
        .format("parquet") \
        .save("hdfs://hdfs-namenode:8020/user/hive/warehouse/weather_data")
    logger.info(f"Written {batch_id} to HDFS")

    partition_values = batch_df.select("ts").distinct().collect()
    for row in partition_values:
        partition = row["ts"]
        add_partition = f"ALTER TABLE default.weather_data ADD IF NOT EXISTS PARTITION (ts='{partition}')"
        spark.sql(add_partition)
        logger.info(f"Added partition {partition} to Hive")


query = weather_df.writeStream \
    .foreachBatch(write_to_hive) \
    .option("checkpointLocation", "hdfs://hdfs-namenode:8020/checkpoints/weather_data_hive") \
    .start()


query.awaitTermination()
