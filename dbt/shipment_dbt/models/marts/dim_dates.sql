{{
    config(
        materialized='table',
        tags=['marts', 'dim']
    )
}}

/*
  dim_dates.sql
  A date spine covering the full range of order dates in the dataset (2015-2018).

  Why NOT a recursive CTE:
    MySQL's @@cte_max_recursion_depth defaults to 1000. Our date range spans
    1,461 days — exceeding that default. Cross-joining digit tables is the
    idiomatic MySQL pattern for generating sequences without recursion limits.

  How the number generator works:
    Four sets of digits (0-9) cross-joined produce 10,000 combinations (0-9999).
    We filter down to the 1,461 rows we actually need.
*/

WITH
digits AS (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
    UNION ALL SELECT 8 UNION ALL SELECT 9
),

nums AS (
    SELECT (d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000) AS seq
    FROM digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
    CROSS JOIN digits d4
),

date_spine AS (
    SELECT DATE_ADD('2015-01-01', INTERVAL seq DAY) AS date_day
    FROM nums
    WHERE DATE_ADD('2015-01-01', INTERVAL seq DAY) <= '2018-12-31'
)

SELECT
    date_day                                                    AS date_id,
    date_day,
    YEAR(date_day)                                              AS `year`,
    QUARTER(date_day)                                           AS `quarter`,
    MONTH(date_day)                                             AS `month`,
    MONTHNAME(date_day)                                         AS month_name,
    WEEK(date_day, 1)                                           AS week_of_year,
    DAY(date_day)                                               AS day_of_month,
    DAYOFWEEK(date_day)                                         AS day_of_week,
    DAYNAME(date_day)                                           AS day_name,
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN 1 ELSE 0 END  AS is_weekend,
    CONCAT(YEAR(date_day), '-Q', QUARTER(date_day))             AS `year_quarter`,
    DATE_FORMAT(date_day, '%Y-%m')                              AS `year_month`
FROM date_spine
ORDER BY date_day
