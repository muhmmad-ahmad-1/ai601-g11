#!/bin/bash

# Input arguments
START_DATE="$1"
NUM_DAYS="$2"    
SCRIPT_TO_RUN="./ingest_logs.sh" 

# Validate inputs
if [[ -z "$START_DATE" || -z "$NUM_DAYS" ]]; then
    echo "Usage: $0 <start-date YYYY-MM-DD> <number-of-days>"
    exit 1
fi

# Loop through the number of days
for ((i=0; i<NUM_DAYS; i++)); do
    # Get the current date
    CURRENT_DATE=$(date -d "$START_DATE + $i days" +%Y-%m-%d)

    # Run for current date
    echo "Running $SCRIPT_TO_RUN with date: $CURRENT_DATE"
    bash "$SCRIPT_TO_RUN" "$CURRENT_DATE"
done

echo "Logs processed for all dates!"
