/*
Business question:
How does trip volume and average fare vary across hours of the day?
When are the peak and trough periods?

Performance considerations:
- Uses the pre-aggregated MART_DEMAND_TIMING table.
- Avoids repeatedly scanning the raw trip table.
- ORDER BY pickup_hour sorts only 24 hourly buckets, making the cost negligible.
- Snowflake result cache can serve repeated dashboard queries efficiently.
*/

WITH hourly_summary AS (
    SELECT
        pickup_hour,
        SUM(trip_count) AS total_trips,
        ROUND(AVG(avg_fare_amount), 2) AS avg_fare_amount,
        ROUND(AVG(avg_total_amount), 2) AS avg_total_amount
    FROM FIRMABLE_TAXI_DB.RAW_MARTS.MART_DEMAND_TIMING
    GROUP BY 1
),

ranked_hours AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_trips DESC) AS demand_peak_rank,
        RANK() OVER (ORDER BY total_trips ASC) AS demand_trough_rank
    FROM hourly_summary
)

SELECT
    pickup_hour,
    total_trips,
    avg_fare_amount,
    avg_total_amount,
    demand_peak_rank,
    demand_trough_rank,
    CASE
        WHEN demand_peak_rank <= 3 THEN 'Peak period'
        WHEN demand_trough_rank <= 3 THEN 'Trough period'
        ELSE 'Normal period'
    END AS demand_period_classification
FROM ranked_hours
ORDER BY pickup_hour;