#!/bin/bash

hive -e " 
SELECT 
    c.category,
    COUNT(*) AS play_count
FROM fact_logs f
JOIN dim_action a ON f.action_id = a.action_id
JOIN dim_content c ON f.content_id = c.content_id
WHERE a.action = 'play' 
AND f.log_date BETWEEN '2024-03-01' AND '2024-03-28'
GROUP BY c.category
ORDER BY play_count DESC
LIMIT 10;

WITH session_durations AS (
    SELECT 
        session_id,
        DATE_SUB(TO_DATE(event_timestamp), Pmod(DAYOFWEEK(TO_DATE(event_timestamp)) - 2, 7)) AS week_start, -- Get Monday
        MAX(UNIX_TIMESTAMP(event_timestamp)) - MIN(UNIX_TIMESTAMP(event_timestamp)) AS session_length_seconds
    FROM fact_logs
    WHERE log_date BETWEEN '2024-03-01' AND '2024-03-28'
    GROUP BY session_id, DATE_SUB(TO_DATE(event_timestamp), Pmod(DAYOFWEEK(TO_DATE(event_timestamp)) - 2, 7))
)
SELECT 
    week_start,
    AVG(session_length_seconds) AS avg_session_length_seconds
FROM session_durations
GROUP BY week_start
ORDER BY week_start;

SELECT 
    DATE_FORMAT(f.event_timestamp, 'yyyy-MM') AS month,
    r.region,
    COUNT(DISTINCT f.user_id) AS active_users
FROM fact_logs f
JOIN dim_region r ON f.region_id = r.region_id
WHERE f.log_date >= '2024-03-01' AND f.log_date < '2024-03-28'
GROUP BY DATE_FORMAT(f.event_timestamp, 'yyyy-MM'), r.region
ORDER BY month, active_users DESC;

SELECT 
    TO_DATE(f.event_timestamp) AS play_date,
    r.region,
    HOUR(f.event_timestamp) AS peak_hour,
    COUNT(*) AS play_count
FROM fact_logs f
JOIN dim_action a ON f.action_id = a.action_id
JOIN dim_region r ON f.region_id = r.region_id
WHERE a.action = 'play'  
AND f.log_date BETWEEN '2024-03-01' AND '2024-03-28' 
GROUP BY TO_DATE(f.event_timestamp), r.region, HOUR(f.event_timestamp)
ORDER BY play_count DESC
LIMIT 10;
"

echo "Hive DDL querying completed!"