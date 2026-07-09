{{ config(materialized='view') }}

SELECT
    location_id,
    borough,
    zone,
    service_zone,
    loaded_at
FROM {{ source('raw', 'TAXI_ZONE_LOOKUP') }}