{{
    config(
        materialized='table'
    )
}}
--For each record in `dim_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
--Compute the continous `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id
with trip_duration_data as(
select dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    year,
    month,
    SAFE_CAST(PUlocationID AS INT64) as PUlocationID,
    SAFE_CAST(DOlocationID AS INT64) as DOlocationID,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    SR_Flag,
    Affiliated_base_number,
    timestamp_diff(dropOff_datetime, pickup_datetime, second) AS trip_duration
    from {{ ref('dim_fhv_trips')}}
)
SELECT *,
    percentile_cont(trip_duration, 0.90) OVER(PARTITION BY year, month, PUlocationID, DOlocationID) AS trip_duration_p90
FROM trip_duration_data