{{ config(materialized='table')}}

-- Quarterly Revenues for each year based on total_amount
with quarterly_revenue as (
    select 
            service_type,
            EXTRACT(YEAR FROM pickup_datetime) as year,
            EXTRACT(QUARTER FROM pickup_datetime) as quarter,
            SUM(total_amount) as revenue
    from {{ ref('fact_trips')}}
    where EXTRACT(YEAR FROM pickup_datetime) IN (2019,2020)
    group by service_type, year, quarter
    order by revenue DESC
),
quarterly_growth as(
    select
        year, 
        quarter, 
        service_type,
        revenue,
        LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year) as prev_year_revenue,
        (revenue - LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year)) / 
        NULLIF(LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year), 0) AS yoy_growth
    from quarterly_revenue
)
SELECT * from quarterly_growth