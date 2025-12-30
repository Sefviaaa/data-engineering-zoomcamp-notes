{{ config(materialized='table')}}
--Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit card')`)
--Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month
-- Now, what are the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020?
with cleaned_data as(
    select 
        service_type,
        fare_amount,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    from {{ ref("fact_trips")}}
    where fare_amount > 0 AND
        trip_distance > 0 AND
        payment_type_description IN ('Cash', 'Credit card')
),
continuous_percentile as(
    select 
        service_type,
        year,
        month, 
        percentile_cont(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS fare_p97,
        percentile_cont(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS fare_p95,
        percentile_cont(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS fare_p90
    from cleaned_data
)
SELECT * from continuous_percentile
