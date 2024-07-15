#!/bin/bash

# Wait for Hive Metastore to be available
while ! nc -z hive-metastore 9083; do
  echo "Waiting for Hive Metastore to be available..."
  sleep 2
done

echo "Hive Metastore is available. Proceeding to create table."

# Run Hive query to create table
hive -e "CREATE TABLE IF NOT EXISTS weather_data_hive_table (
    location STRUCT<
      name: STRING,
      country: STRING,
      lat: FLOAT,
      lon: FLOAT,
      timezone: STRING,
      localtime_epoch: INT,
      localtime: TIMESTAMP
    >,
    current_weather STRUCT<
        last_updated_epoch: INT,
        last_updated: TIMESTAMP,
        temperature_c: FLOAT,
        feelslike_c: FLOAT,
        windchill_c: FLOAT,
        heatindex_c: FLOAT,
        dewpoint_c: FLOAT,
        humidity: INT,
        pressure_mb: FLOAT,
        precip_mm: FLOAT,
        wind_kph: FLOAT,
        wind_deg: INT,
        wind_dir: STRING,
        gust_kph: FLOAT,
        is_day: INT,
        cloud: INT,
        vis_km: FLOAT,
        uv: FLOAT,
        condition: STRUCT<
          text: STRING,
          icon: STRING,
          code: INT
        >
    >
)
PARTITIONED BY (localtime TIMESTAMP)
STORED AS PARQUET
LOCATION 'hdfs://hdfs-namenode:8020/user/hive/warehouse/weather_data_hive_table';"