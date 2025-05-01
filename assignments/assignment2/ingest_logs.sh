#!/bin/bash

echo "Ingesting Logs"

# Check if date parameter is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Input date parameter
DATE=$1

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

# Base directories
LOGS_BASE_DIR="/user/hadoop/raw/logs"
META_BASE_DIR="/user/hadoop/raw/metadata"

# Target directories
LOGS_TARGET_DIR="${LOGS_BASE_DIR}/${YEAR}/${MONTH}/${DAY}"
META_TARGET_DIR="${META_BASE_DIR}"

# Source directory for logs (assuming logs are stored in YYYY-MM-DD folders locally)
SOURCE_LOG_DIR="${DATE}"

# Check if source log file exists and copy it
if [ -f "${SOURCE_LOG_DIR}/user_activity_logs.csv" ]; then

    # Create target log directory if it does not exist
    /home/hadoop/hadoop/bin/hdfs dfs -mkdir -p "$LOGS_TARGET_DIR"

    # Copy the file
    /home/hadoop/hadoop/bin/hdfs dfs -put -f "${SOURCE_LOG_DIR}/user_activity_logs.csv" "$LOGS_TARGET_DIR/"
    if [ $? -eq 0 ]; then
        echo "Successfully copied user_activity_logs.csv to $LOGS_TARGET_DIR"
    else
        echo "Error: Failed to copy user_activity_logs.csv"
        exit 1
    fi
else
    echo "Error: user_activity_logs.csv not found in ${SOURCE_LOG_DIR}"
    exit 1
fi

# Check if metadata file exists and copy it
if [ -f "content_metadata.csv" ]; then
    # Create metadata directory if it does not exist
    /home/hadoop/hadoop/bin/hdfs dfs -mkdir -p "$META_TARGET_DIR"
    
    # Copy the file
    /home/hadoop/hadoop/bin/hdfs dfs -put -f "content_metadata.csv" "$META_TARGET_DIR/"
    if [ $? -eq 0 ]; then
        echo "Successfully copied content_metadata.csv to $META_TARGET_DIR"
    else
        echo "Error: Failed to copy content_metadata.csv"
        exit 1
    fi
else
    echo "Error: content_metadata.csv not found in current directory"
    exit 1
fi

echo "Ingestion complete for date: $DATE"
exit 0
