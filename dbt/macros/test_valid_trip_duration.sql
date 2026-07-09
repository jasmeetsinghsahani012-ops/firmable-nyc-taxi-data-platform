{% test valid_trip_duration(model, pickup_column, dropoff_column) %}

SELECT *
FROM {{ model }}
WHERE {{ dropoff_column }} < {{ pickup_column }}

{% endtest %}