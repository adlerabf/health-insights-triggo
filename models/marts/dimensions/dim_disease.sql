{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT DISTINCT
        icd_code,
        icd_description
    FROM {{ ref('stg_icd_data') }}
),

with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY icd_code) AS disease_key,
        icd_code,
        icd_description
    FROM base
)

SELECT *
FROM with_keys
ORDER BY disease_key

{# {{ config(
    materialized='table'
) }}

SELECT
    icd_code,
    icd_description
FROM {{ ref('stg_icd_data') }} #}