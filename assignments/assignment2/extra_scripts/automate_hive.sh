#!/bin/bash

# Input arguments
DATE="$1"
NUM_DAYS="$2"    
BASE_PATH="/user/hadoop/raw/logs"  # Base path for logs


# Validate date format (basic check)
if ! [[ $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Date must be in YYYY-MM-DD format"
    exit 1
fi

# Extract year, month, and day
YEAR=${DATE:0:4}
MONTH=${DATE:5:2}
DAY=${DATE:8:2}

# Check if month is valid (01-12)
if (( 10#$MONTH < 1 || 10#$MONTH > 12 )); then
    echo "Error: Month must be between 01 and 12"
    exit 1
fi

# Check if day is valid for the given month
case $MONTH in
    01|03|05|07|08|10|12) MAX_DAY=31 ;;
    04|06|09|11) MAX_DAY=30 ;;
    02)
        # Check for leap year
        if (( (YEAR % 4 == 0 && YEAR % 100 != 0) || (YEAR % 400 == 0) )); then
            MAX_DAY=29
        else
            MAX_DAY=28
        fi
        ;;
esac

if (( 10#$DAY < 1 || 10#$DAY > MAX_DAY )); then
    echo "Error: Invalid day for the given month"
    exit 1
fi

# Ingest data into HDFS
echo "Ingesting data into HDFS..."
bash ingest_all_logs.sh "$DATE" "$NUM_DAYS"

echo "Running Hive queries..."
PARTITION_QUERIES=""  # Ensure it's empty before starting
START_EPOCH=$(date -d "$DATE" +%s)

for (( i=0; i<NUM_DAYS; i++ )); do
    PARTITION_DATE=$(date -d "@$(( START_EPOCH + (i * 86400) ))" "+%Y-%m-%d")
    PARTITION_PATH=$(date -d "@$(( START_EPOCH + (i * 86400) ))" "+$BASE_PATH/%Y/%m/%d/")
    
    # Correctly append the queries with explicit newlines
    PARTITION_QUERIES+="ALTER TABLE logs ADD PARTITION (log_date='$PARTITION_DATE') LOCATION '$PARTITION_PATH';"$'\n'
done

hive -e "
CREATE EXTERNAL TABLE metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY ','  
STORED AS TEXTFILE  
LOCATION '/user/hadoop/raw/metadata'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE logs (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp TIMESTAMP,  
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (log_date STRING)
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/user/hadoop/raw/logs'
TBLPROPERTIES ('skip.header.line.count'='1');

${PARTITION_QUERIES}  -- Insert partition queries correctly

CREATE TABLE fact_logs (
    user_id INT, 
    content_id INT,
    event_timestamp TIMESTAMP,
    action_id INT,
    device_id INT,
    region_id INT,
    session_id STRING
)
PARTITIONED BY (log_date STRING)
STORED AS PARQUET;

CREATE TABLE dim_content (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
STORED AS PARQUET;

CREATE TABLE dim_device (
    device_id INT,
    device STRING
)
STORED AS PARQUET;

CREATE TABLE dim_region (
    region_id INT,
    region STRING 
)
STORED AS PARQUET;

CREATE TABLE dim_action (
    action_id INT,
    action STRING 
)
STORED AS PARQUET;

INSERT INTO dim_content
SELECT content_id, title, category, length, artist FROM metadata;

INSERT INTO dim_device
SELECT 
    t.device_id,
    t.device
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY device) AS device_id, 
        device
    FROM (SELECT DISTINCT device FROM logs) AS unique_devices
) t;

INSERT INTO dim_region
SELECT 
    t.region_id,
    t.region
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY region) AS region_id, 
        region
    FROM (SELECT DISTINCT region FROM logs) AS unique_regions
) t;

INSERT INTO dim_action
SELECT 
    t.action_id,
    t.action
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY action) AS action_id, 
        action
    FROM (SELECT DISTINCT action FROM logs) AS unique_actions
) t;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO fact_logs 
PARTITION (log_date)
SELECT 
    l.user_id,
    l.content_id,
    l.event_timestamp,
    a.action_id,
    d.device_id,
    r.region_id,
    l.session_id,
    l.log_date
FROM logs l
JOIN dim_action a ON l.action = a.action
JOIN dim_device d ON l.device = d.device
JOIN dim_region r ON l.region = r.region;
"

echo "Hive processing completed!"
