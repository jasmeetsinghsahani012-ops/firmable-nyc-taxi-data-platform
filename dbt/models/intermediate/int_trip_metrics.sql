{{ config(materialized='table') }}

SELECT
    *,
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,

    DATE_FROM_PARTS(
        YEAR(tpep_pickup_datetime),
        MONTH(tpep_pickup_datetime),
        1
    ) AS pickup_month,

    EXTRACT(hour FROM tpep_pickup_datetime) AS pickup_hour,
    DAYNAME(tpep_pickup_datetime) AS pickup_day_name,

    DATEDIFF(
        minute,
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    ) AS trip_duration_minutes,

    CASE
        WHEN total_amount > 0
            THEN ROUND((tip_amount / total_amount) * 100, 2)
        ELSE NULL
    END AS tip_percentage,

    CASE
        WHEN DATEDIFF(minute, tpep_pickup_datetime, tpep_dropoff_datetime) > 0
            THEN ROUND(trip_distance / (DATEDIFF(minute, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0), 2)
        ELSE NULL
    END AS average_speed_mph,

    CASE
        WHEN DAYOFWEEK(tpep_pickup_datetime) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM {{ ref('stg_yellow_taxi_trips') }}