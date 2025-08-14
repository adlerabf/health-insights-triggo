{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT DISTINCT
        city_code,
        city_name,
        state_code,
        region_name
    FROM {{ ref('stg_cities_data') }}
),

with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY city_code) AS location_key,
        city_code,
        UPPER(TRIM(city_name)) AS city_name,
        UPPER(TRIM(state_code)) AS state_code,
        UPPER(TRIM(region_name)) AS region_name
    FROM base
)

SELECT *
FROM with_keys
ORDER BY location_key


{# {{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        city_code,
        city_name,
        state_code,
        region_name
    FROM {{ ref('stg_cities_data') }}
)

SELECT DISTINCT
    city_code AS locality_key,
    city_name,
    state_code,
    region_name
FROM base #}