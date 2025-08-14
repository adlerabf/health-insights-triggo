{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT DISTINCT
        procedure_code,
        procedure_name
    FROM {{ ref('stg_procedures_name') }}
),

with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY procedure_code) AS procedure_key,
        procedure_code,
        UPPER(TRIM(procedure_name)) AS procedure_name
    FROM base
)

SELECT *
FROM with_keys
ORDER BY procedure_key



{# {{ config(
    materialized='table'
) }}

SELECT
    procedure_code,
    procedure_name
FROM {{ ref('stg_procedures_name') }} #}