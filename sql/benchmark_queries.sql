-- Benchmark queries. Each one runs on BOTH tables ({table} is replaced with
-- trips_unpartitioned and trips_partitioned). The answers must match; only the
-- data scanned should differ.
--
-- Format: a line "-- name: <id> | <short description>" starts each query.

-- name: q1_one_month | Trips and average fare in one month (Jan 2019)
SELECT count(*) AS trips, round(avg(fare_amount), 4) AS avg_fare
FROM {table}
WHERE tpep_pickup_datetime >= TIMESTAMP '2019-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2019-02-01 00:00:00';

-- name: q2_three_years | Revenue per month, Jan 2023 to Jan 2025
SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       count(*) AS trips,
       round(sum(total_amount), 2) AS revenue
FROM {table}
WHERE tpep_pickup_datetime >= TIMESTAMP '2023-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2025-02-01 00:00:00'
GROUP BY 1
ORDER BY 1;

-- name: q3_new_column_2025 | How many Jan 2025 trips paid the new congestion-zone fee
SELECT count(*) AS trips,
       count_if(cbd_congestion_fee > 0) AS paid_cbd_fee
FROM {table}
WHERE tpep_pickup_datetime >= TIMESTAMP '2025-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2025-02-01 00:00:00';

-- name: q4_no_time_filter | Top pickup zones over all years (no time filter: partitioning can't help)
SELECT pulocationid, count(*) AS trips
FROM {table}
WHERE pulocationid IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC, 1
LIMIT 10;
