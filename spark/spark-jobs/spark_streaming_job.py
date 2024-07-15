from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType, ArrayType

# Define the schema for the weather data
weather_schema = StructType([
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_offset", IntegerType(), True),
    StructField("current", StructType([
        StructField("dt", LongType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True),
        StructField("temp", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("dew_point", FloatType(), True),
        StructField("uvi", FloatType(), True),
        StructField("clouds", IntegerType(), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("wind_deg", IntegerType(), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ]), True), True)
    ]), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_data") \
    .load()

# Parse the JSON data
weather_df = df.select(from_json(col("value").cast("string"), weather_schema).alias("data")).select("data.*")

# Extract the required fields from the parsed data
# weather_df = weather_df.select(
#     col("lat"),
#     col("lon"),
#     col("timezone"),
#     col("timezone_offset"),
#     col("current.dt").alias("current_dt"),
#     col("current.sunrise").alias("current_sunrise"),
#     col("current.sunset").alias("current_sunset"),
#     col("current.temp").alias("current_temp"),
#     col("current.feels_like").alias("current_feels_like"),
#     col("current.pressure").alias("current_pressure"),
#     col("current.humidity").alias("current_humidity"),
#     col("current.dew_point").alias("current_dew_point"),
#     col("current.uvi").alias("current_uvi"),
#     col("current.clouds").alias("current_clouds"),
#     col("current.visibility").alias("current_visibility"),
#     col("current.wind_speed").alias("current_wind_speed"),
#     col("current.wind_deg").alias("current_wind_deg"),
#     col("current.weather").alias("current_weather")
# )

# weather_df = weather_df.select(
#     col("current.temp").alias("temp"),
#     col("current.humidity").alias("humidity"),
#     col("current.dt").alias("name")
# )

# Write data to HDFS
query_hdfs = weather_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hdfs-namenode:8020/weather_data") \
    .option("checkpointLocation", "hdfs://hdfs-namenode:8020/checkpoints/weather_data_hdfs") \
    .start()

# Create temporary view for writing to Hive
weather_df.createOrReplaceTempView("weather_data_temp")

# Write data to Hive
# query_hive = spark.sql("""
#     INSERT INTO TABLE default.weather_data_hive_table
#     SELECT * FROM weather_data_temp
# """)

# Await termination of the queries
query_hdfs.awaitTermination()
# query_hive.awaitTermination()
