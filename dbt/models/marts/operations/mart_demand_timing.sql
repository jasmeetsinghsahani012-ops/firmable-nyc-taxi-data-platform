{{ config(materialized='table') }}

SELECT
    pickup_hour,
    pickup_day_name,
    is_weekend,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM {{ ref('int_trip_enriched') }}
WHERE total_amount > 0
GROUP BY 1, 2, 3