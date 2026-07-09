{{ config(materialized='table') }}

SELECT
    pu_location_id,
    pickup_borough,
    pickup_zone,
    payment_type,
    CASE
        WHEN trip_distance < 1 THEN '0-1 miles'
        WHEN trip_distance < 3 THEN '1-3 miles'
        WHEN trip_distance < 5 THEN '3-5 miles'
        WHEN trip_distance < 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END AS distance_bucket,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage,
    ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM {{ ref('int_trip_enriched') }}
WHERE total_amount > 0
  AND tip_percentage IS NOT NULL
GROUP BY 1, 2, 3, 4, 5