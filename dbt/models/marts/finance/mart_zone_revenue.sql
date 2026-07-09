{{ config(materialized='table') }}

SELECT
    pickup_month,
    pu_location_id,
    pickup_borough,
    pickup_zone,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_revenue,
    RANK() OVER (
        PARTITION BY pickup_month
        ORDER BY SUM(total_amount) DESC
    ) AS monthly_revenue_rank
FROM {{ ref('int_trip_enriched') }}
WHERE total_amount > 0
  AND pickup_month IS NOT NULL
GROUP BY
    pickup_month,
    pu_location_id,
    pickup_borough,
    pickup_zone