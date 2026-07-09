{{ config(materialized='table') }}

SELECT
    trips.*,

    pickup_zones.borough AS pickup_borough,
    pickup_zones.zone AS pickup_zone,
    pickup_zones.service_zone AS pickup_service_zone,

    dropoff_zones.borough AS dropoff_borough,
    dropoff_zones.zone AS dropoff_zone,
    dropoff_zones.service_zone AS dropoff_service_zone

FROM {{ ref('int_trip_metrics') }} trips

LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} pickup_zones
    ON trips.pu_location_id = pickup_zones.location_id

LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} dropoff_zones
    ON trips.do_location_id = dropoff_zones.location_id