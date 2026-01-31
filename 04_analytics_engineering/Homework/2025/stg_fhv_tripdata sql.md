# stg_fhv_tripdata.sql

```sql

{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata_external_2019') }}
  where dispatching_base_num is not null 
)
select

    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
    

from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

```

```sql
dbt build --select stg_fhv_tripdata --vars '{is_test_run: false}'
```

`rn` is short for **row number**, and in *this model* it is used **only for deduplication**.

Let’s break it down very plainly.

---

## What this line does

```sql
row_number() over (
  partition by vendorid, lpep_pickup_datetime
) as rn

```

This tells BigQuery/dbt:

> “For rows that share the same vendorid and the same lpep_pickup_datetime, number them 1, 2, 3, …”
> 

So inside each `(vendorid, lpep_pickup_datetime)` group:

- one row gets `rn = 1`
- duplicates get `rn = 2`, `rn = 3`, etc.

---

## Why you need it in **green_tripdata**

NYC Green Taxi data **does contain duplicates**:

- same vendor
- same pickup timestamp
- repeated rows from ingestion / source issues

If you don’t remove duplicates:

- counts will be inflated
- surrogate keys will collide
- fact tables become unreliable

---

## How the deduplication actually happens

This line is the *actual* deduplication:

```sql
where rn = 1

```

Meaning:

> “Keep only the first row in each duplicate group, discard the rest.”
> 

So the full logic is:

1. Assign a row number per `(vendorid, pickup_time)`
2. Keep only the first row
3. Drop all duplicates

---

## Why `(vendorid, lpep_pickup_datetime)`?

Because for Green Taxi data:

- there is **no true trip_id in the raw source**
- this combination is the **closest natural key**
- it’s what the Zoomcamp project expects

Later you generate a **surrogate key**:

```sql
{{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }}

```

That only works correctly if duplicates are removed first — otherwise:

- multiple rows would get the same `tripid`
- `unique` tests would fail

---

## Why FHV staging does NOT use `rn`

Important contrast:

- **Green / Yellow** → structured taxi meter data → deduplication required
- **FHV** → dispatch records → *no reliable natural key*

That’s why:

- you correctly **did not** deduplicate FHV
- and the instruction explicitly said *“DO NOT add deduplication”* for FHV

So `rn` is **intentionally absent** in FHV staging.

---

## One-sentence mental model (remember this)

> rn exists only to safely remove duplicate raw trips before creating a stable trip ID.
> 

---

## Final summary

- `rn` = row number per potential duplicate group
- Used to **deduplicate** Green Taxi data
- `where rn = 1` keeps only one copy
- Necessary before generating `tripid`
- Correct and expected in this model

Nothing is wrong here — this is **exactly how staging should handle duplicates** 👍