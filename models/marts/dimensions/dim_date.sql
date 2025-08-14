{{ config(
    materialized='table'
) }}

WITH dates AS (
    SELECT
        DATEADD(day, seq4(), '2025-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 2557)) -- ~7 anos (2025 a 2031)
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::NUMBER(8,0) AS date_id,
    date_day AS full_date,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    DAYOFWEEK(date_day) AS day_of_week, -- 1=Sunday, 7=Saturday
    DAYNAME(date_day) AS day_name,
    QUARTER(date_day) AS quarter,
    'Q' || QUARTER(date_day) AS quarter_name,
    MONTHNAME(date_day) AS month_name,
    DAYOFYEAR(date_day) AS day_of_year,
    WEEKOFYEAR(date_day) AS week_of_year,
    CASE WHEN date_day = DATE_TRUNC('month', date_day) THEN 1 ELSE 0 END AS is_month_start,
    CASE WHEN date_day = DATEADD(day, -1, DATEADD(month, 1, DATE_TRUNC('month', date_day))) THEN 1 ELSE 0 END AS is_month_end
FROM dates
WHERE date_day <= '2031-12-31'
ORDER BY date_day










--old version
{# {{ config(
    materialized='table'
) }}

WITH dates AS (
    SELECT
        TO_DATE(DATEADD(day, seq4(), '2025-01-01'::DATE))::DATE AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 2190))  -- ~6 years of dates
    WHERE date_day <= '2031-12-31'::DATE
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::NUMBER AS date_id,
    date_day::DATE AS full_date,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    DAYOFWEEK(date_day) AS day_of_week,
    QUARTER(date_day) AS quarter,
    MONTHNAME(date_day) AS month_name,
    DAYNAME(date_day) AS day_name
FROM dates
ORDER BY date_day #}
