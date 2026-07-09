/*
Business question:
What is the relationship between trip distance, payment type and tip percentage?
Are there zones where passengers tip significantly more or less?

Performance considerations:
- Uses MART_TIP_BEHAVIOUR instead of calculating metrics from 38M trips.
- Aggregations are performed before sorting to reduce compute.
- ORDER BY is executed on a small aggregated dataset.
- If queried frequently by zone, clustering on pickup_zone or payment_type can improve pruning.
*/

WITH zone_tip_summary AS (
    SELECT
        pickup_borough,
        pickup_zone,
        payment_type,
        distance_bucket,
        SUM(trip_count) AS trip_count,
        ROUND(AVG(avg_trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(avg_tip_percentage), 2) AS avg_tip_percentage,
        ROUND(AVG(avg_tip_amount), 2) AS avg_tip_amount,
        ROUND(AVG(avg_total_amount), 2) AS avg_total_amount
    FROM FIRMABLE_TAXI_DB.RAW_MARTS.MART_TIP_BEHAVIOUR
    GROUP BY 1, 2, 3, 4
),

ranked_zones AS (
    SELECT
        *,
        RANK() OVER (ORDER BY avg_tip_percentage DESC) AS highest_tip_rank,
        RANK() OVER (ORDER BY avg_tip_percentage ASC) AS lowest_tip_rank
    FROM zone_tip_summary
    WHERE trip_count >= 100
)

SELECT
    pickup_borough,
    pickup_zone,
    payment_type,
    distance_bucket,
    trip_count,
    avg_trip_distance,
    avg_tip_percentage,
    avg_tip_amount,
    avg_total_amount,
    highest_tip_rank,
    lowest_tip_rank
FROM ranked_zones
WHERE highest_tip_rank <= 20
   OR lowest_tip_rank <= 20
ORDER BY avg_tip_percentage DESC;