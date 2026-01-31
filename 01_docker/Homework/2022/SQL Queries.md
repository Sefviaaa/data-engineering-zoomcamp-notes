# SQL Queries

```sql
SELECT *
FROM information_schema.columns;
```

![image.png](SQL%20Queries/image.png)

## Checking Data Types

```sql
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('yellow_tripdata_2021_01', 'taxi_zone_lookup');
```

![image.png](SQL%20Queries/image%201.png)

## Confirm that tables exist

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';
```

## Data Count

![image.png](SQL%20Queries/image%202.png)

![image.png](SQL%20Queries/image%203.png)