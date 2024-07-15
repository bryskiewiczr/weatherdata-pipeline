#!/bin/bash

# Wait for Hive Metastore to be available
while ! nc -z hive-metastore 9083; do
  echo "Waiting for Hive Metastore to be available..."
  sleep 2
done

echo "Hive Metastore is available. Proceeding to create table."

# Run Hive query to create table
hive -e "CREATE TABLE IF NOT EXISTS weather_data_hive_table (temp FLOAT, humidity FLOAT, name STRING);"
