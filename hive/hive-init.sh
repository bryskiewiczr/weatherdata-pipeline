#!/bin/bash

sleep 30

hive -e "CREATE TABLE IF NOT EXISTS weather_data_hive_table (
    temp FLOAT,
    humidity FLOAT,
    name STRING
);"