from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Define the schema for the weather data
weather_schema = StructType([
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True)
    ]), True),
    StructField("name", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_data") \
    .load()

# Parse the JSON data
weather_df = df.select(from_json(col("value").cast("string"), weather_schema).alias("data")).select("data.*")

# Write data to HDFS
query_hdfs = weather_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://hdfs-namenode:8020/weather_data") \
    .option("checkpointLocation", "hdfs://hdfs-namenode:8020/checkpoints/weather_data_hdfs") \
    .start()

# Write data to Hive
weather_df.createOrReplaceTempView("weather_data_temp")
query_hive = spark.sql("""
    INSERT INTO TABLE weather_data_hive_table
    SELECT * FROM weather_data_temp
""")

# Await termination of the queries
query_hdfs.awaitTermination()
query_hive.awaitTermination()