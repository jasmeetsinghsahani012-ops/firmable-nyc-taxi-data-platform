/*
Business question:
Which pickup zones generate the most revenue across 2023?
Does the ranking shift meaningfully by month?

Performance considerations:
- Query reads from the pre-aggregated MART_ZONE_REVENUE table instead of the 38M-row raw dataset.
- Aggregation is precomputed, reducing compute cost.
- ORDER BY is applied only on a small aggregated result set.
- For production, clustering by pickup_month or pu_location_id can improve pruning.
*/

WITH annual_revenue AS (
    SELECT
        pu_location_id,
        pickup_borough,
        pickup_zone,
        SUM(total_revenue) AS annual_revenue,
        SUM(trip_count) AS annual_trip_count
    FROM FIRMABLE_TAXI_DB.RAW_MARTS.MART_ZONE_REVENUE
    GROUP BY 1, 2, 3
),

ranked_annual AS (
    SELECT
        *,
        RANK() OVER (ORDER BY annual_revenue DESC) AS annual_revenue_rank
    FROM annual_revenue
)

SELECT
    monthly.pickup_month,
    monthly.pu_location_id,
    monthly.pickup_borough,
    monthly.pickup_zone,
    monthly.trip_count,
    monthly.total_revenue,
    monthly.monthly_revenue_rank,
    annual.annual_revenue,
    annual.annual_trip_count,
    annual.annual_revenue_rank
FROM FIRMABLE_TAXI_DB.RAW_MARTS.MART_ZONE_REVENUE monthly
LEFT JOIN ranked_annual annual
    ON monthly.pu_location_id = annual.pu_location_id
WHERE annual.annual_revenue_rank <= 20
ORDER BY
    annual.annual_revenue_rank,
    monthly.pickup_month;